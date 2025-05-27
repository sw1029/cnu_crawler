# cnu_crawler/utils.py
import re
from datetime import datetime, timezone, timedelta

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

def now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))

def iso_now() -> str:
    return now_kst().strftime(ISO_FORMAT)

def clean_text(txt: str) -> str:
    return re.sub(r"\s+", " ", txt).strip()
