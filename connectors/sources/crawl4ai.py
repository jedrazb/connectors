#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Crawl4AI Web Crawler Data Source
"""

import fnmatch
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from functools import cached_property
from typing import Dict, List, Set
from urllib.parse import urlparse

import aiohttp
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    URLPatternFilter,
    DomainFilter,
)
from crawl4ai.async_crawler_strategy import AsyncHTTPCrawlerStrategy

from connectors.source import BaseDataSource
from connectors.utils import hash_id


class Crawl4AIWebDataSource(BaseDataSource):
    """Web crawler data source using crawl4ai for intelligent crawling and content extraction"""

    name = "Crawl4AI Web Crawler"
    service_type = "crawl4ai"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @cached_property
    def allowed_domains_set(self) -> Set[str]:
        """Get allowed domains as a set, auto-extracting from start URLs if empty"""
        domains = set()

        allowed_domains = self.configuration.get("allowed_domains", [])
        if allowed_domains:
            domains.update(
                domain.strip().lower() for domain in allowed_domains if domain.strip()
            )

        if not domains:
            start_urls = self.configuration.get("start_urls", [])
            for url in start_urls:
                if url and url.strip():
                    try:
                        parsed = urlparse(url.strip())
                        if parsed.netloc:
                            domains.add(parsed.netloc.lower())
                    except Exception:
                        continue

        return domains

    @cached_property
    def binary_extensions_set(self) -> Set[str]:
        """Get binary file extensions as a set"""
        extensions = self.configuration.get("binary_file_extensions", [])
        if not extensions:
            return set()

        if isinstance(extensions, str):
            extensions = [ext.strip() for ext in extensions.split(",")]

        return {ext.strip().lower().lstrip(".") for ext in extensions if ext.strip()}

    @classmethod
    def get_default_configuration(cls):
        return {
            "start_urls": {
                "display": "textarea",
                "label": "Seed URLs",
                "order": 1,
                "tooltip": "Comma-separated list of URLs to start crawling from.",
                "type": "list",
                "value": "https://example.com",
            },
            "allowed_domains": {
                "display": "textarea",
                "label": "Allowed domains",
                "order": 2,
                "tooltip": "Comma-separated list of domains to restrict crawling to. Leave empty to allow all domains from seed URLs. Example: example.com, docs.example.com",
                "type": "list",
                "value": "",
                "required": False,
            },
            "sitemap_urls": {
                "display": "textarea",
                "label": "Sitemap URLs (optional)",
                "order": 3,
                "tooltip": "Comma-separated list of XML sitemap URLs to discover additional pages to crawl.",
                "type": "list",
                "value": "",
                "required": False,
            },
            "url_include_patterns": {
                "display": "textarea",
                "label": "URL patterns to include",
                "order": 4,
                "tooltip": "Comma-separated glob patterns for URLs to include. Use '*' to match any characters. Example: https://example.com/docs/*, *.pdf",
                "type": "list",
                "value": "*",
            },
            "url_exclude_patterns": {
                "display": "textarea",
                "label": "URL patterns to exclude",
                "order": 5,
                "tooltip": "Comma-separated glob patterns for URLs to exclude. Example: */admin/*, */login/*, *.zip",
                "type": "list",
                "value": "*/admin/*, */login/*",
                "required": False,
            },
            "max_crawl_depth": {
                "display": "numeric",
                "label": "Maximum crawl depth",
                "order": 6,
                "tooltip": "Maximum number of link levels to follow from seed URLs. 0 = seed URLs only, 1 = seed URLs + direct links, etc.",
                "type": "int",
                "value": 2,
            },
            "extract_full_content": {
                "display": "toggle",
                "label": "Extract full page content",
                "order": 7,
                "tooltip": "Extract the full page content as markdown. When disabled, only basic metadata is extracted.",
                "type": "bool",
                "value": True,
            },
            "extract_binary_content": {
                "display": "toggle",
                "label": "Extract binary content (PDFs, DOCX, etc.)",
                "order": 8,
                "tooltip": "Extract text content from binary files like PDFs, DOCX, PPTX using crawl4ai's parsing capabilities.",
                "type": "bool",
                "value": False,
            },
            "binary_file_extensions": {
                "display": "textarea",
                "label": "Binary file extensions to extract",
                "order": 9,
                "tooltip": "Comma-separated list of binary file extensions to extract content from. Example: pdf, docx, pptx, xlsx",
                "type": "list",
                "value": "pdf, docx, pptx, xlsx, doc, xls, ppt",
                "required": False,
                "depends_on": [{"field": "extract_binary_content", "value": True}],
            },
        }

    async def ping(self):
        """Test connectivity by checking if start URLs are accessible"""
        try:
            extract_binary = self.configuration.get("extract_binary_content", False)
            if extract_binary and not self.binary_extensions_set:
                self._logger.warning(
                    "Binary content extraction is enabled but no binary file extensions are configured"
                )

            start_urls = self.configuration.get("start_urls", [])
            if not start_urls:
                return False

            first_url = start_urls[0].strip() if start_urls[0] else None
            if not first_url:
                return False

            # Test first URL with HTTP request
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    first_url, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    return response.status < 400

        except Exception as e:
            self._logger.error(f"Ping failed: {e}")
            return False

    async def changed(self):
        """Always return True as web content can change frequently"""
        return True

    def _is_binary_file(self, url: str) -> bool:
        """Check if URL points to a binary file that we can extract content from"""
        if not self.configuration.get("extract_binary_content", False):
            return False

        extensions = self.binary_extensions_set
        if not extensions:
            return False

        try:
            path = urlparse(url).path.lower()
            if "." in path:
                extension = path.split(".")[-1]
                return extension in extensions
        except Exception:
            return False

        return False

    def _create_filter_chain(self) -> FilterChain:
        """Create filter chain for URL filtering using crawl4ai built-ins"""
        filters = []

        allowed_domains = list(self.allowed_domains_set)
        if allowed_domains:
            domain_filter = DomainFilter(allowed_domains=allowed_domains)
            filters.append(domain_filter)
            self._logger.info(f"Added domain filter for: {allowed_domains}")

        include_patterns = self.configuration.get("url_include_patterns", [])
        exclude_patterns = self.configuration.get("url_exclude_patterns", [])

        if include_patterns:
            url_filter = URLPatternFilter(patterns=include_patterns)
            filters.append(url_filter)
            self._logger.info(f"Added include patterns: {include_patterns}")

        if exclude_patterns:
            url_filter = URLPatternFilter(patterns=exclude_patterns, reverse=True)
            filters.append(url_filter)
            self._logger.info(f"Added exclude patterns: {exclude_patterns}")

        filter_chain = FilterChain(filters) if filters else None
        self._logger.info(f"Created filter chain with {len(filters)} filters")
        return filter_chain

    async def _parse_sitemap(self, sitemap_url: str) -> List[str]:
        """Parse XML sitemap and extract URLs, applying include pattern filtering"""
        urls = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(sitemap_url) as response:
                    if response.status == 200:
                        content = await response.text()
                        root = ET.fromstring(content)

                        namespaces = {
                            "sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"
                        }

                        for url_elem in root.findall(
                            ".//sitemap:url/sitemap:loc", namespaces
                        ):
                            if url_elem.text:
                                urls.append(url_elem.text.strip())

                        if not urls:
                            for url_elem in root.findall(".//loc"):
                                if url_elem.text:
                                    urls.append(url_elem.text.strip())

        except Exception as e:
            self._logger.error(f"Error parsing sitemap {sitemap_url}: {e}")

        # Filter sitemap URLs based on include patterns
        include_patterns = self.configuration.get("url_include_patterns", [])
        exclude_patterns = self.configuration.get("url_exclude_patterns", [])

        if include_patterns or exclude_patterns:
            filtered_urls = []
            for url in urls:
                include_match = True
                exclude_match = False

                # Check include patterns
                if include_patterns:
                    include_match = any(
                        fnmatch.fnmatch(url, pattern) for pattern in include_patterns
                    )

                # Check exclude patterns
                if exclude_patterns:
                    exclude_match = any(
                        fnmatch.fnmatch(url, pattern) for pattern in exclude_patterns
                    )

                # Include URL if it matches include patterns and doesn't match exclude patterns
                if include_match and not exclude_match:
                    filtered_urls.append(url)
                    self._logger.debug(
                        f"Sitemap URL '{url}' included (include: {include_match}, exclude: {exclude_match})"
                    )
                else:
                    self._logger.debug(
                        f"Sitemap URL '{url}' excluded (include: {include_match}, exclude: {exclude_match})"
                    )

            patterns_info = []
            if include_patterns:
                patterns_info.append(f"include: {include_patterns}")
            if exclude_patterns:
                patterns_info.append(f"exclude: {exclude_patterns}")

            self._logger.info(
                f"Sitemap filtering: {len(filtered_urls)}/{len(urls)} URLs match patterns ({', '.join(patterns_info)})"
            )
            return filtered_urls

        return urls

    def _create_crawler_config(self) -> CrawlerRunConfig:
        """Create CrawlerRunConfig with proper filtering using BFSDeepCrawlStrategy"""
        # Collect start URLs
        start_urls = self.configuration.get("start_urls", [])
        start_urls = [url.strip() for url in start_urls if url and url.strip()]

        # Store sitemap URLs for later processing in get_docs method
        self._sitemap_urls = self.configuration.get("sitemap_urls", [])
        self._sitemap_urls = [
            url.strip() for url in self._sitemap_urls if url and url.strip()
        ]

        max_depth = self.configuration.get("max_crawl_depth", 2)
        filter_chain = self._create_filter_chain()

        # Create deep crawl strategy with filtering
        deep_crawl_strategy = BFSDeepCrawlStrategy(
            max_depth=max_depth,
            include_external=False,
            filter_chain=filter_chain,
            max_pages=10000,
        )

        # Store start URLs for manual iteration
        self._start_urls = start_urls

        config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            deep_crawl_strategy=deep_crawl_strategy,
            stream=True,
            verbose=True,
            delay_before_return_html=0.1,
            check_robots_txt=True,
        )

        self._logger.info(
            f"Created CrawlerRunConfig with {len(start_urls)} start URLs and {len(self._sitemap_urls)} sitemaps"
        )
        return config

    def _extract_content(self, result) -> str:
        """Extract content from crawl4ai result using built-in markdown"""
        extract_full = self.configuration.get("extract_full_content", True)

        if not extract_full:
            return ""

        if hasattr(result, "markdown") and result.markdown:
            return result.markdown.fit_markdown or result.markdown.raw_markdown or ""

        return ""

    def _extract_title(self, result, url: str, is_binary: bool) -> str:
        """Extract title from result"""
        if (
            hasattr(result, "metadata")
            and result.metadata
            and isinstance(result.metadata, dict)
        ):
            title = result.metadata.get("title", "")
            if title:
                return title

        if is_binary:
            try:
                filename = urlparse(url).path.split("/")[-1]
                return filename or f"Binary Document ({url.split('.')[-1].upper()})"
            except:
                return "Binary Document"

        return ""

    def _process_crawl_result(self, result, crawled_count):
        """Process a single crawl result and return a document dict"""
        if not result.success:
            self._logger.warning(
                f"Failed to crawl {result.url}: {getattr(result, 'error_message', 'Unknown error')}"
            )
            return None

        is_binary = self._is_binary_file(result.url)
        content = self._extract_content(result)
        title = self._extract_title(result, result.url, is_binary)

        if is_binary:
            self._logger.info(
                f"Extracted {len(content)} characters from binary file: {result.url}"
            )

        doc = {
            "_id": hash_id(result.url),
            "url": result.url,
            "title": title,
            "content": content,
            "depth": getattr(result, "depth", 0),
            "_timestamp": datetime.now(timezone.utc).isoformat(),
            "size": len(content),
            "status_code": result.status_code,
        }

        self._logger.debug(
            f"Successfully crawled {result.url}: title='{title}', content_size={len(content)}, is_binary={is_binary}"
        )

        return doc

    async def get_docs(self, filtering=None):
        """Main method to crawl and yield documents using crawl4ai's built-in filtering"""
        self._logger.info("Starting Crawl4AI web crawling with built-in filtering...")

        allowed_domains = self.allowed_domains_set
        self._logger.info(
            f"Allowed domains: {allowed_domains if allowed_domains else 'All domains from seed URLs'}"
        )

        extract_binary = self.configuration.get("extract_binary_content", False)
        self._logger.info(
            f"Binary content extraction: {'Enabled' if extract_binary else 'Disabled'}"
        )
        if extract_binary:
            self._logger.info(f"Binary file extensions: {self.binary_extensions_set}")

        max_depth = self.configuration.get("max_crawl_depth", 2)
        self._logger.info(f"Max crawl depth: {max_depth}")

        browser_config = BrowserConfig(
            headless=False,
            verbose=False,
        )

        crawler_strategy = AsyncHTTPCrawlerStrategy()
        crawler_config = self._create_crawler_config()

        # Collect all URLs to crawl
        all_start_urls = list(self._start_urls)  # Copy the start URLs

        # Parse sitemaps and add filtered URLs
        if hasattr(self, "_sitemap_urls") and self._sitemap_urls:
            for sitemap_url in self._sitemap_urls:
                try:
                    sitemap_urls = await self._parse_sitemap(sitemap_url)
                    all_start_urls.extend(sitemap_urls)
                    self._logger.info(
                        f"Added {len(sitemap_urls)} filtered URLs from sitemap: {sitemap_url}"
                    )
                except Exception as e:
                    self._logger.error(f"Error processing sitemap {sitemap_url}: {e}")

        # Validate that we have start URLs
        if not all_start_urls:
            self._logger.error("No valid start URLs found after processing sitemaps")
            return

        crawled_count = 0

        async with AsyncWebCrawler(
            config=browser_config, crawler_strategy=crawler_strategy
        ) as crawler:
            try:
                # Crawl each start URL individually with deep crawling strategy
                for url in all_start_urls:
                    try:
                        crawled_count += 1
                        self._logger.debug(f"Crawling URL {crawled_count}: {url}")

                        # BFSDeepCrawlStrategy will handle filtering of discovered URLs automatically
                        result = await crawler.arun(url=url, config=crawler_config)

                        doc = self._process_crawl_result(result, crawled_count)
                        if doc:
                            yield doc, None

                    except Exception as e:
                        self._logger.error(f"Error crawling {url}: {e}")
                        continue

            except Exception as e:
                self._logger.error(f"Error during crawling: {e}")
                raise

        self._logger.info(f"Crawling completed. Processed {crawled_count} URLs")
