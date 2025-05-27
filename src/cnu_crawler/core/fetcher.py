# src/cnu_crawler/core/fetcher.py
from typing import Any, Optional
from loguru import logger
import aiohttp  # ClientTimeout 객체는 여기서 직접 사용하지 않음
from aiohttp_retry import RetryClient, ExponentialRetry  # type: ignore
from cnu_crawler.config import DEFAULT_HEADERS


class Fetcher:
    _instance: Optional["Fetcher"] = None
    _retry_client_instance: Optional[RetryClient] = None

    def __init__(self):
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient를 초기화합니다...")

        # ExponentialRetry 설정: 재시도 횟수 및 시작 타임아웃
        # attempts: 최대 재시도 횟수
        # start_timeout: 첫 재시도 대기 시간 (초)
        # max_timeout: 최대 대기 시간 (초) - 재시도 간 대기 시간의 상한
        # factor: 대기 시간 증가 배수
        retry_opts = ExponentialRetry(
            attempts=3,
            start_timeout=1,  # 첫 시도 후 1초 대기 후 재시도 시작
            max_timeout=30,  # 재시도 간 대기 시간이 최대 30초를 넘지 않도록 함
            factor=2
            # 각 개별 요청의 타임아웃은 aiohttp.ClientSession의 기본값을 따르거나,
            # RetryClient가 내부적으로 ClientSession에 전달하는 방식을 사용해야 합니다.
            # RetryClient가 client_timeout 인자를 직접 받지 않으므로,
            # aiohttp의 기본 타임아웃(5분)이 적용되거나,
            # 또는 RetryClient가 생성하는 ClientSession에 timeout을 설정하는 다른 메커니즘이 필요합니다.
            # 만약 개별 요청 타임아웃 설정이 필수라면,
            # RetryClient 대신 ClientSession을 직접 사용하고 재시도 로직을 구현해야 할 수 있습니다.
        )

        # RetryClient 생성 시 'client_timeout' 인자를 제거합니다.
        # 대신, RetryClient가 내부적으로 사용하는 ClientSession에 타임아웃을 설정하려면
        # session_params 또는 client_session_kwargs와 같은 인자를 찾아보거나,
        # aiohttp_retry 라이브러리가 지원하는 다른 방식을 사용해야 합니다.
        # 여기서는 해당 인자를 제거하고, retry_opts의 시간 설정에 의존합니다.
        self._retry_client_instance = RetryClient(
            # client_timeout=timeout_settings, # 이 줄을 제거하거나 주석 처리합니다.
            raise_for_status=True,
            retry_options=retry_opts,
            headers=DEFAULT_HEADERS
            # 만약 aiohttp.ClientSession의 timeout을 설정하고 싶다면,
            # RetryClient가 이를 지원하는지 확인해야 합니다.
            # 예: client_session_kwargs={"timeout": aiohttp.ClientTimeout(total=60)}
            # 위 예시는 가상이며, 실제 지원 여부는 라이브러리 문서 확인 필요.
            # 현재 aiohttp_retry (2.x 버전 기준)는 ClientSession의 timeout을 직접 설정하는
            # 명시적인 파라미터를 RetryClient 생성자에 제공하지 않는 것으로 보입니다.
            # 요청별 타임아웃은 GET/POST 메서드 호출 시 'timeout' 인자로 전달할 수 있습니다.
        )
        logger.debug("싱글톤 인스턴스를 위해 Fetcher의 RetryClient가 초기화되었습니다.")

    async def get_text(self, url: str, **kwargs) -> str:
        if not self._retry_client_instance:
            logger.error("get_text 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")

        # 개별 요청에 타임아웃 설정 (예: 30초)
        request_timeout = kwargs.pop("timeout", aiohttp.ClientTimeout(total=30))

        async with self._retry_client_instance.get(url, timeout=request_timeout, **kwargs) as resp:  # type: ignore
            return await resp.text()

    async def get_json(self, url: str, **kwargs) -> Any:
        if not self._retry_client_instance:
            logger.error("get_json 호출 전에 Fetcher의 RetryClient가 초기화되지 않았습니다.")
            raise RuntimeError("Fetcher가 제대로 초기화되지 않았거나 클라이언트가 이미 닫혔습니다.")

        # 개별 요청에 타임아웃 설정 (예: 30초)
        request_timeout = kwargs.pop("timeout", aiohttp.ClientTimeout(total=30))

        async with self._retry_client_instance.get(url, timeout=request_timeout, **kwargs) as resp:  # type: ignore
            return await resp.json(content_type=None)

    # ... 이하 나머지 코드는 이전과 동일 ...
    @classmethod
    def instance(cls) -> "Fetcher":
        if cls._instance is None:
            logger.debug("새로운 Fetcher 싱글톤 인스턴스를 생성합니다.")
            cls._instance = cls()
        return cls._instance

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