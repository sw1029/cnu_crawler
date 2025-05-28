# src/scraper/base.py
from abc import ABC, abstractmethod
import time
import pandas as pd
from typing import List, Dict

class ScraperBase(ABC):
    """모든 Scraper가 상속할 공통 인터페이스"""

    def __init__(self, college: str, dept: str, base_url: str):
        self.college = college
        self.dept = dept
        self.base_url = base_url

    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """공지사항 한 페이지를 DataFrame으로 반환"""
        ...

    # ── 후처리 공용 함수 ───────────────────────────────
    def _standardize(self, rows: List[Dict]) -> pd.DataFrame:
        for row in rows:
            row.setdefault("college", self.college)
            row.setdefault("dept", self.dept)
            row.setdefault("crawled_at", int(time.time()))
        return pd.DataFrame(rows)
