# cnu_crawler/core/fetcher.py
from typing import Any, Optional

from aiohttp_retry import RetryClient, ExponentialRetry

from src.cnu_crawler.config import DEFAULT_HEADERS

class Fetcher:
    """Singleton-style 비동기 HTTP fetcher."""
    _instance: Optional["Fetcher"] = None

    def __init__(self):
        retry_opts = ExponentialRetry(attempts=5, start_timeout=1)
        self._client = RetryClient(raise_for_status=True, retry_options=retry_opts,
                                   headers=DEFAULT_HEADERS)

    @classmethod
    def instance(cls) -> "Fetcher":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def get_text(self, url: str, **kwargs) -> str:
        async with self._client.get(url, **kwargs) as resp:
            return await resp.text()

    async def get_json(self, url: str, **kwargs) -> Any:
        async with self._client.get(url, **kwargs) as resp:
            return await resp.json(content_type=None)

    async def close(self):
        await self._client.close()

# 헬퍼 coroutine
async def fetch_text(url: str, **kw) -> str:
    return await Fetcher.instance().get_text(url, **kw)

async def fetch_json(url: str, **kw) -> Any:
    return await Fetcher.instance().get_json(url, **kw)
