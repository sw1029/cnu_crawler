# cnu_crawler/core/browser.py
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from ..config import SELENIUM_DRIVER, DEFAULT_HEADERS

@contextmanager
def get_driver(headless: bool = True):
    opts = Options()
    if headless:
        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
    # Friendly UA
    opts.add_argument(f'user-agent={DEFAULT_HEADERS["User-Agent"]}')
    driver = webdriver.Chrome(executable_path=SELENIUM_DRIVER, options=opts)
    try:
        yield driver
    finally:
        driver.quit()
