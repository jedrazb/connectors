import asyncio
from typing import AsyncIterator, Iterator

from langchain_core.document_loaders import BaseLoader
from langchain_core.documents import Document

from connectors.sources.sharepoint_online import SharepointOnlineDataSource
from connectors.sources.mongo import MongoDataSource
from connectors.sources.google_drive import GoogleDriveDataSource

from connectors.source import DataSourceConfiguration
from connectors.es.settings import TIMESTAMP_FIELD


# wrapper class to handle limit of docs to fetc, sync rules, etc
class ConnectorDataSourceLoader(BaseLoader):

    def __init__(self, cls, **kwargs) -> None:

        connector_config = cls.get_default_configuration()

        # Apply user config
        for key, value in kwargs.items():
            if key in connector_config.keys():
                connector_config[key]["value"] = value

        data_source_config = DataSourceConfiguration(connector_config)

        self.data_provider = cls(data_source_config)

    def lazy_load(self) -> Iterator[dict]:
        async_gen = self.alazy_load()
        loop = asyncio.get_event_loop()

        try:
            while True:
                item = loop.run_until_complete(self._next_item(async_gen))
                if item is None:
                    break
                yield item
        except StopAsyncIteration:
            return

    async def _next_item(self, async_gen):
        try:
            return await async_gen.__anext__()
        except StopAsyncIteration:
            return None

    async def alazy_load(
        self,
    ) -> AsyncIterator[Document]:
        async for doc, lazy_download in self.data_provider.get_docs(filtering=None):
            doc["id"] = doc.pop("_id")
            # TODO: not all sources have timestamp field and support downloads
            # data = await lazy_download(doit=True, timestamp=doc[TIMESTAMP_FIELD])
            # doc.update(data)
            yield doc


class SharepointOnlineLoader(ConnectorDataSourceLoader):
    def __init__(self, **kwargs):
        super().__init__(cls=SharepointOnlineDataSource, **kwargs)


class MongoDBLoader(ConnectorDataSourceLoader):
    def __init__(self, **kwargs):
        super().__init__(cls=MongoDataSource, **kwargs)


class GoogleDriveLoader(ConnectorDataSourceLoader):
    def __init__(self, **kwargs):
        super().__init__(cls=GoogleDriveDataSource, **kwargs)
