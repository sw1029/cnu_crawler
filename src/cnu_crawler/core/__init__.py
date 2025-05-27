"""
core 서브패키지

크롤링 공통 로직(fetcher, browser, parser 등)을 모아둔 공간.
여기서 __all__ 에 올린 객체만 외부에 노출됩니다.
"""

# fetcher — 비동기 HTTP
from .fetcher import Fetcher, fetch_json, fetch_text

# browser — Selenium/CDP
from .browser import get_driver

# parser — BeautifulSoup 헬퍼
from .parser import html_first, html_select

__all__ = [
    "Fetcher",
    "fetch_json",
    "fetch_text",
    "get_driver",
    "html_first",
    "html_select",
]
