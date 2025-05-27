# src/cnu_crawler/core/fetcher.py
from typing import Any, Optional
from loguru import logger
import aiohttp  # ClientTimeout을 위해 import
from aiohttp_retry import RetryClient, ExponentialRetry  # type: ignore
from cnu_crawler.config import DEFAULT_HEADERS


class Fetcher:
    _instance: Optional["Fetcher"] = None
    _retry_client_instance: Optional[RetryClient] = None

    def __init__(self):
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient를 초기화합니다...")

        # 타임아웃 설정 (초 단위)
        # connect: 연결 시도 타임아웃
        # total: 전체 요청 타임아웃 (연결, 요청 전송, 응답 수신 포함)
        # sock_connect: 소켓 연결 타임아웃
        # sock_read: 소켓 읽기 타임아웃
        timeout_settings = aiohttp.ClientTimeout(
            total=60,  # 전체 요청 시간 60초
            connect=10,  # 연결 시도 시간 10초
            sock_connect=10,  # 소켓 연결 시간 10초
            sock_read=30  # 소켓 읽기 시간 30초
        )

        # ExponentialRetry 설정: 재시도 횟수 및 시작 타임아웃
        # attempts: 최대 재시도 횟수 (기존 5회)
        # start_timeout: 첫 재시도 대기 시간 (기존 1초)
        # max_timeout: 최대 대기 시간 (예: 30초)
        # factor: 대기 시간 증가 배수 (예: 2)
        retry_opts = ExponentialRetry(
            attempts=3,  # 재시도 횟수를 3회로 줄여봄 (또는 유지)
            start_timeout=1,
            max_timeout=30,
            factor=2
        )

        self._retry_client_instance = RetryClient(
            client_timeout=timeout_settings,  # 명시적 타임아웃 설정 추가
            raise_for_status=True,
            retry_options=retry_opts,
            headers=DEFAULT_HEADERS
        )
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient가 초기화되었습니다.")

    # ... (이하 나머지 코드는 이전 답변과 동일하게 유지) ...

    @classmethod
    def instance(cls) -> "Fetcher":
        if cls._instance is None:
            logger.debug("새로운 Fetcher 싱글톤 인스턴스를 생성합니다.")
            cls._instance = cls()
        return cls._instance

    async def get_text(self, url: str, **kwargs) -> str:
        if not self._retry_client_instance:
            logger.error("get_text 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")
        async with self._retry_client_instance.get(url, **kwargs) as resp:  # type: ignore
            return await resp.text()

    async def get_json(self, url: str, **kwargs) -> Any:
        if not self._retry_client_instance:
            logger.error("get_json 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")
        async with self._retry_client_instance.get(url, **kwargs) as resp:  # type: ignore
            return await resp.json(content_type=None)

    async def close_http_client(self):
        if self._retry_client_instance is not None:
            logger.info("Fetcher의 RetryClient를 닫습니다...")
            await self._retry_client_instance.close()
            self._retry_client_instance = None
            logger.info("Fetcher의 RetryClient가 닫혔고 None으로 설정되었습니다.")
        else:
            logger.info("Fetcher의 RetryClient가 이미 None이거나 초기화되지 않았습니다. 아무 작업도 수행하지 않습니다.")


async def fetch_text(url: str, **kw) -> str:
    return await Fetcher.instance().get_text(url, **kw)


async def fetch_json(url: str, **kw) -> Any:
    return await Fetcher.instance().get_json(url, **kw)


async def close_global_fetcher_client():
    logger.debug("전역 fetcher 클라이언트를 닫으려고 시도합니다.")
    if Fetcher._instance is not None:
        await Fetcher.instance().close_http_client()
    else:
        logger.debug("Fetcher 인스턴스를 찾을 수 없어 닫을 클라이언트가 없습니다.")