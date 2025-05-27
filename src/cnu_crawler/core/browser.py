# cnu_crawler/core/browser.py
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cnu_crawler.config import SELENIUM_DRIVER, DEFAULT_HEADERS

@contextmanager
def get_driver(headless: bool = True):
    opts = Options()
    if headless:
        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f'user-agent={DEFAULT_HEADERS["User-Agent"]}')

    # ✅ 최신 방식: performance 로그를 위해 options에 직접 capability 추가
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(options=opts)

    try:
        yield driver
    finally:
        driver.quit()