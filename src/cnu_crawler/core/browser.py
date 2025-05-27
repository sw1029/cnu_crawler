# cnu_crawler/core/browser.py
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

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

    # ✅ performance 로그를 위한 로그 설정 추가
    caps = DesiredCapabilities.CHROME.copy()
    caps["goog:loggingPrefs"] = {"performance": "ALL"}

    driver = webdriver.Chrome(options=opts, desired_capabilities=caps)

    try:
        yield driver
    finally:
        driver.quit()