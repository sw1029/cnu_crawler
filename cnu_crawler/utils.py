import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Generator, Tuple, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import DEFAULT_HEADERS, ENCODING_FALLBACKS


def resilient_get(url: str, **kwargs) -> requests.Response:
    """
    GET 요청을 시도하되, 인코딩 문제가 있으면 fallback encoding을 적용한다.
    """
    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=kwargs.get("timeout", 10))
    # 인코딩 추정 실패 시 수동 지정
    if resp.encoding is None or "charset" not in resp.headers.get("content-type", ""):
        for enc in ENCODING_FALLBACKS:
            try:
                resp.encoding = enc
                resp.text  # 접근만 해도 디코딩 시도
                break
            except UnicodeDecodeError:
                continue
    resp.raise_for_status()
    return resp


def load_links(path: Path) -> Generator[Tuple[str, str, str], None, None]:
    """
    links.txt 파일에서 (college, dept_or_grad, url) 튜플을 차례로 반환.
    두 번째 항목이 '-' 이면 college 값으로 대체, 반대도 동일.
    """
    with path.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        for college, dept, url in reader:
            college = college.strip()
            dept    = dept.strip()
            if college == "-":
                college = dept
            if dept == "-":
                dept = college
            yield college, dept, url.strip()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def today_str() -> str:
    return datetime.now().strftime("%Y%m%d")


def save_dataframe(df: pd.DataFrame, college: str, dept: str) -> Path:
    """
    CSV 저장 경로: data/csv/{college}_{dept}_{YYYYMMDD}.csv
    """
    from .config import CSV_DIR

    safe = lambda s: re.sub(r"[^\w가-힣]", "_", s)  # 파일명 안전화
    fname = f"{safe(college)}_{safe(dept)}_{today_str()}.csv"
    path = CSV_DIR / fname
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
