# cnu_crawler/config.py
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# 크롤링 대상 최상위 URL
ROOT_URL = os.getenv("CNU_ROOT_URL", "https://plus.cnu.ac.kr")

# 요청 헤더(학내 서버 Friendly)
DEFAULT_HEADERS = {
    "User-Agent": "CNUNoticeBot/1.0 (+https://github.com/yourname/cnu_crawler)",
    "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
}

# 크롤링 주기(분)
SCHEDULE_MINUTES = int(os.getenv("SCHEDULE_MINUTES", 30))

# Selenium WebDriver 경로(자동 설치 시 pass)
SELENIUM_DRIVER = os.getenv("SELENIUM_DRIVER", "chromedriver")
