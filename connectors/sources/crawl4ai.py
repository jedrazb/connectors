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
from typing import List
from urllib.parse import urlparse

import aiohttp
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, HTTPCrawlerConfig
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
        self.start_urls = self.configuration.get("start_urls", [])
        self.allowed_domains = self.configuration.get("allowed_domains", [])
        self.sitemap_urls = self.configuration.get("sitemap_urls", [])
        self.url_include_patterns = self.configuration.get("url_include_patterns", [])
        self.url_exclude_patterns = self.configuration.get("url_exclude_patterns", [])
        self.max_crawl_depth = self.configuration["max_crawl_depth"]
        self.extract_full_content = self.configuration["extract_full_content"]
        self.extract_binary_content = self.configuration["extract_binary_content"]
        self.binary_file_extensions = self.configuration.get(
            "binary_file_extensions", []
        )

        # If allowed_domains is empty, extract domains from start_urls
        if not self.allowed_domains and self.start_urls:
            self.allowed_domains = self._extract_domains_from_urls(self.start_urls)
            self._logger.info(
                f"Extracted allowed domains from start URLs: {self.allowed_domains}"
            )

    def _extract_domains_from_urls(self, urls: List[str]) -> List[str]:
        """Extract unique domains from a list of URLs"""
        domains = set()
        for url in urls:
            try:
                parsed = urlparse(url)
                if parsed.netloc:
                    domains.add(parsed.netloc)
            except Exception as e:
                self._logger.warning(f"Failed to parse URL {url}: {e}")
        return list(domains)

    def _normalize_url(self, url: str) -> str:
        """Normalize URL by removing fragment identifiers to prevent duplicate crawling"""
        parsed = urlparse(url)
        return parsed._replace(fragment="").geturl()

    def _should_include_url(self, url: str) -> bool:
        """Check if URL should be included based on include/exclude patterns"""
        # Check include patterns
        if self.url_include_patterns:
            included = any(
                fnmatch.fnmatch(url, pattern) for pattern in self.url_include_patterns
            )
            if not included:
                return False

        # Check exclude patterns
        if self.url_exclude_patterns:
            excluded = any(
                fnmatch.fnmatch(url, pattern) for pattern in self.url_exclude_patterns
            )
            if excluded:
                return False

        return True

    def _is_binary_file(self, url: str) -> bool:
        """Check if URL points to a binary file based on extension"""
        if not self.extract_binary_content:
            return False

        if not self.binary_file_extensions:
            return False

        parsed_url = urlparse(url)
        path = parsed_url.path.lower()

        return any(
            path.endswith(f".{ext.lower()}") for ext in self.binary_file_extensions
        )

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
                "tooltip": "Comma-separated list of domains to restrict crawling to. Leave empty to allow all domains from seed URLs.",
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
                "tooltip": "Comma-separated glob patterns for URLs to include. Use '*' to match any characters.",
                "type": "list",
                "value": "*",
            },
            "url_exclude_patterns": {
                "display": "textarea",
                "label": "URL patterns to exclude",
                "order": 5,
                "tooltip": "Comma-separated glob patterns for URLs to exclude.",
                "type": "list",
                "value": "*/admin/*, */login/*",
                "required": False,
            },
            "max_crawl_depth": {
                "display": "numeric",
                "label": "Maximum crawl depth",
                "order": 6,
                "tooltip": "Maximum number of link levels to follow from seed URLs.",
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
                "tooltip": "Comma-separated list of binary file extensions to extract content from.",
                "type": "list",
                "value": "pdf, docx, pptx, xlsx, doc, xls, ppt",
                "required": False,
                "depends_on": [{"field": "extract_binary_content", "value": True}],
            },
        }

    async def ping(self):
        """Test connectivity by checking if start URLs are accessible"""
        try:
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

    async def _parse_sitemap_xml(self, sitemap_url: str) -> List[str]:
        """Parse XML sitemap and extract URLs using proper XML parsing"""
        urls = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    sitemap_url, timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        content = await response.text()
                        self._logger.debug(f"Sitemap content length: {len(content)}")

                        # Try to parse as XML
                        try:
                            root = ET.fromstring(content)

                            # Handle both sitemap index and URL sitemaps
                            namespaces = {
                                "sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"
                            }

                            # Look for URL entries
                            for url_elem in root.findall(
                                ".//sitemap:url/sitemap:loc", namespaces
                            ):
                                if url_elem.text:
                                    url = url_elem.text.strip()
                                    if self._should_include_url(url):
                                        urls.append(url)

                            # If no URLs found with namespace, try without
                            if not urls:
                                for url_elem in root.findall(".//url/loc"):
                                    if url_elem.text:
                                        url = url_elem.text.strip()
                                        if self._should_include_url(url):
                                            urls.append(url)

                            # Also try direct loc elements
                            if not urls:
                                for url_elem in root.findall(".//loc"):
                                    if url_elem.text:
                                        url = url_elem.text.strip()
                                        if self._should_include_url(url):
                                            urls.append(url)

                        except ET.ParseError as e:
                            self._logger.warning(
                                f"Failed to parse XML for {sitemap_url}: {e}"
                            )
                            # Fallback to text-based extraction
                            lines = content.split("\n")
                            for line in lines:
                                line = line.strip()
                                if line.startswith("http://") or line.startswith(
                                    "https://"
                                ):
                                    url = self._normalize_url(line)
                                    if self._should_include_url(url):
                                        urls.append(url)
                    else:
                        self._logger.warning(
                            f"Sitemap {sitemap_url} returned status {response.status}"
                        )

        except Exception as e:
            self._logger.error(f"Error parsing sitemap {sitemap_url}: {e}")

        self._logger.info(f"Discovered {len(urls)} URLs from sitemap {sitemap_url}")
        return urls

    async def _discover_sitemap_urls(self) -> List[str]:
        """Discover URLs from sitemaps using direct HTTP requests"""
        discovered_urls = []

        for sitemap_url in self.sitemap_urls:
            try:
                self._logger.info(f"Processing sitemap: {sitemap_url}")
                urls = await self._parse_sitemap_xml(sitemap_url)
                discovered_urls.extend(urls)

            except Exception as e:
                self._logger.warning(f"Failed to process sitemap {sitemap_url}: {e}")

        return discovered_urls

    async def get_docs(self, filtering=None):
        """Main method to crawl and yield documents using crawl4ai's built-in filtering"""
        self._logger.info(f"Starting crawl with {len(self.start_urls)} start URLs")

        # Create filter chain for domain restrictions and URL patterns
        filters = []

        if self.allowed_domains:
            filters.append(DomainFilter(allowed_domains=self.allowed_domains))

        if self.url_include_patterns:
            include_filter = URLPatternFilter(patterns=self.url_include_patterns)
            filters.append(include_filter)

        if self.url_exclude_patterns:
            exclude_filter = URLPatternFilter(
                patterns=self.url_exclude_patterns, reverse=True
            )
            filters.append(exclude_filter)

        filter_chain = FilterChain(filters) if filters else None

        # Configure crawl strategy
        crawl_strategy = BFSDeepCrawlStrategy(
            max_depth=self.max_crawl_depth,
            include_external=False,  # Keep within allowed domains
            filter_chain=filter_chain,
        )

        # Use HTTP-only crawler for speed
        http_config = HTTPCrawlerConfig(
            method="GET",
            headers={"User-Agent": "Crawl4AI-Connector/1.0"},
            follow_redirects=True,
            verify_ssl=True,
        )

        crawler_config = CrawlerRunConfig(
            deep_crawl_strategy=crawl_strategy,
            stream=True,  # Use streaming for real-time results
            word_count_threshold=10 if self.extract_full_content else 100,
            exclude_external_links=True,
            page_timeout=60000,  # 60 seconds timeout
        )

        try:
            async with AsyncWebCrawler(
                crawler_strategy=AsyncHTTPCrawlerStrategy(browser_config=http_config)
            ) as crawler:

                # Discover additional URLs from sitemaps
                all_start_urls = list(self.start_urls)
                if self.sitemap_urls:
                    sitemap_urls = await self._discover_sitemap_urls()
                    all_start_urls.extend(sitemap_urls)
                    self._logger.info(f"Added {len(sitemap_urls)} URLs from sitemaps")

                # Process each start URL
                processed_urls = set()

                for start_url in all_start_urls:
                    normalized_url = self._normalize_url(start_url)
                    if normalized_url in processed_urls:
                        continue

                    processed_urls.add(normalized_url)

                    try:
                        self._logger.info(f"Crawling: {normalized_url}")

                        # Handle binary files differently
                        if self._is_binary_file(normalized_url):
                            # For binary files, just create a basic document
                            doc = {
                                "_id": hash_id(normalized_url),
                                "url": normalized_url,
                                "title": normalized_url.split("/")[-1],
                                "content": f"Binary file: {normalized_url}",
                                "depth": 0,
                                "_timestamp": datetime.now(timezone.utc).isoformat(),
                            }
                            yield doc, None
                            continue

                        # Use streaming crawl for regular content
                        async for result in await crawler.arun(
                            url=normalized_url, config=crawler_config
                        ):
                            if result.success:
                                # Check if this URL should be included based on our exclude patterns
                                if not self._should_include_url(result.url):
                                    self._logger.debug(
                                        f"Skipping excluded URL: {result.url}"
                                    )
                                    continue

                                # Create document from crawl result
                                content = (
                                    result.markdown
                                    if self.extract_full_content
                                    else result.cleaned_html
                                )
                                title = result.metadata.get("title", "")
                                if not title:
                                    # Extract title from URL if not available
                                    title = result.url.split("/")[-1] or result.url

                                depth = result.metadata.get("depth", 0)

                                doc = {
                                    "_id": hash_id(result.url),
                                    "url": result.url,
                                    "title": title,
                                    "content": content,
                                    "depth": depth,
                                    "_timestamp": datetime.now(
                                        timezone.utc
                                    ).isoformat(),
                                }

                                # Add metadata if available
                                if hasattr(result, "links") and result.links:
                                    doc["links_count"] = len(result.links)

                                if hasattr(result, "media") and result.media:
                                    doc["media_count"] = len(
                                        result.media.get("images", [])
                                    )

                                yield doc, None

                            else:
                                self._logger.warning(
                                    f"Failed to crawl {result.url}: {result.error_message}"
                                )

                    except Exception as e:
                        self._logger.error(f"Error crawling {normalized_url}: {e}")
                        continue

        except Exception as e:
            self._logger.error(f"Crawler initialization failed: {e}")
            raise
