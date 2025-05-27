# src/cnu_crawler/core/fetcher.py
from typing import Any, Optional
from loguru import logger
# aiohttp_retry 타입을 명시적으로 사용하기 위해 type: ignore를 사용하거나,
# aiohttp_retry가 type hint를 제대로 지원하는지 확인 필요합니다.
# 현재 코드에서는 type: ignore를 사용하여 타입 체커 오류를 무시합니다.
from aiohttp_retry import RetryClient, ExponentialRetry # type: ignore
from cnu_crawler.config import DEFAULT_HEADERS

class Fetcher:
    _instance: Optional["Fetcher"] = None
    _retry_client_instance: Optional[RetryClient] = None # 명확성을 위해 이름 변경

    def __init__(self):
        """
        RetryClient를 초기화합니다. 이 메서드는 Fetcher 싱글톤 인스턴스가
        처음 생성될 때 호출됩니다.
        """
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient를 초기화합니다...")
        retry_opts = ExponentialRetry(attempts=5, start_timeout=1)
        # 클라이언트는 Fetcher 인스턴스의 속성입니다.
        self._retry_client_instance = RetryClient(raise_for_status=True, retry_options=retry_opts,
                                                  headers=DEFAULT_HEADERS)
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient가 초기화되었습니다.")

    @classmethod
    def instance(cls) -> "Fetcher":
        """Fetcher의 싱글톤 인스턴스를 가져옵니다."""
        if cls._instance is None:
            logger.debug("새로운 Fetcher 싱글톤 인스턴스를 생성합니다.")
            cls._instance = cls()
        return cls._instance

    async def get_text(self, url: str, **kwargs) -> str:
        """URL에서 텍스트 내용을 가져옵니다."""
        if not self._retry_client_instance:
            logger.error("get_text 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            # 이는 instance()가 먼저 호출되지 않았거나 클라이언트가 이미 닫힌 경우 프로그래밍 오류를 나타냅니다.
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")
        # type: ignore는 RetryClient의 get 메서드에 대한 타입 힌트 문제를 무시합니다.
        async with self._retry_client_instance.get(url, **kwargs) as resp: # type: ignore
            return await resp.text()

    async def get_json(self, url: str, **kwargs) -> Any:
        """URL에서 JSON 내용을 가져옵니다."""
        if not self._retry_client_instance:
            logger.error("get_json 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")
        # type: ignore는 RetryClient의 get 메서드에 대한 타입 힌트 문제를 무시합니다.
        async with self._retry_client_instance.get(url, **kwargs) as resp: # type: ignore
            # content_type=None은 기본 JSON content-type 검사를 우회합니다.
            return await resp.json(content_type=None)

    async def close_http_client(self): # 명확성을 위해 이름 변경
        """내부 aiohttp 클라이언트 세션을 닫습니다."""
        if self._retry_client_instance is not None:
            logger.info("Fetcher의 RetryClient를 닫습니다...")
            await self._retry_client_instance.close()
            self._retry_client_instance = None # 재사용을 방지하기 위해 닫은 후 None으로 설정
            logger.info("Fetcher의 RetryClient가 닫혔고 None으로 설정되었습니다.")
        else:
            logger.info("Fetcher의 RetryClient가 이미 None이거나 초기화되지 않았습니다. 아무 작업도 수행하지 않습니다.")

# 헬퍼 코루틴 (변경되지 않지만 수정된 Fetcher에 의존)
async def fetch_text(url: str, **kw) -> str:
    return await Fetcher.instance().get_text(url, **kw)

async def fetch_json(url: str, **kw) -> Any:
    return await Fetcher.instance().get_json(url, **kw)

# 애플리케이션 종료 시 호출될 새로운 전역 비동기 함수
async def close_global_fetcher_client():
    """Fetcher 인스턴스가 존재하면 클라이언트 세션을 닫습니다."""
    logger.debug("전역 fetcher 클라이언트를 닫으려고 시도합니다.")
    if Fetcher._instance is not None:
        await Fetcher.instance().close_http_client()
    else:
        logger.debug("Fetcher 인스턴스를 찾을 수 없어 닫을 클라이언트가 없습니다.")