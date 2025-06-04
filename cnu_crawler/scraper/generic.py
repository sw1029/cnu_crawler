"""
학과·대학 사이트별 HTML 구조가 달라도
공지 제목·URL·게시일만 추출할 수 있으면 된다.
이 GenericScraper 는
 • 테이블(tr td), 리스트(ul li), 카드(div.card …) 등 여러 패턴을 시도하고
 • selector 가 먹히는 순간 결과를 DataFrame 으로 반환한다.
"""

import re, time
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from ..utils import resilient_get, normalize_whitespace
from .base import ScraperBase


# ── 후보 CSS selector 목록 ─────────────────────────────────────
#   (앞에 있는 것부터 시도)
CANDIDATE_ROWS: Sequence[Tuple[str, str]] = [
    # (row 선택자, title-anchor 선택자); ''이면 row 자체가 anchor
    ("table tbody tr", "td a"),
    ("div.board_list tbody tr", "td a"),
    ("ul li", "a"),
    ("div.list li", "a"),
    ("div.card", "a"),
]


# ── 게시일로 추정 가능한 문자열 정규식 ─────────────────────────
_DATE_RE = re.compile(
    r"(20\d{2}[./-]\d{1,2}[./-]\d{1,2})|(\d{4}\.\d{2}\.\d{2})|(\d{4}-\d{2}-\d{2})"
)


class GenericScraper(ScraperBase):
    def scrape(self) -> pd.DataFrame:  # type: ignore[override]
        try:
            resp = resilient_get(self.base_url, timeout=10)
        except requests.HTTPError as e:
            if e.response.status_code == 404 and "mode=list" not in self.base_url:
                # 혹시 mode=list 빠졌다면 한 번 더 시도
                fallback = self.base_url + ("?mode=list" if "?" not in self.base_url else "&mode=list")
                resp = resilient_get(fallback, timeout=10)
            else:
                raise
        resp = resilient_get(self.base_url, timeout=10)
        base = resp.url.rsplit("/", 1)[0]  # 상대 URL 보정용
        soup = BeautifulSoup(resp.text, "html.parser")
        for row_sel, a_sel in CANDIDATE_ROWS:
            rows, parsed = [], []
            for row in soup.select(row_sel):
                a_tag = row.select_one(a_sel) if a_sel else row
                if not a_tag or not a_tag.get("href"):
                    continue
                title = normalize_whitespace(a_tag.get_text())
                if not title:
                    continue
                href = a_tag["href"].strip()
                if href.startswith("/"):
                    href = f"{resp.url.split('/', 3)[:3][0]}{href}"
                elif not href.startswith("http"):
                    href = f"{base}/{href.lstrip('./')}"
                posted_at = _extract_date(row)
                parsed.append(
                    dict(
                        id=_make_id(href),
                        title=title,
                        url=href,
                        posted_at=posted_at,
                    )
                )
            if parsed:  # 이 selector 가 최소 1개는 먹혔다면 성공
                rows = parsed
                break

        if not rows:  # 모든 selector 실패
            raise RuntimeError(f"[GenericScraper] No rows parsed for {self.base_url}")

        return self._standardize(rows)


# ────────────────────────────── helpers ──────────────────────────────
def _extract_date(node: Tag) -> str:
    """tr 또는 li 내부에서 yyyy.mm.dd·yyyy-mm-dd 패턴을 찾는다."""
    text = normalize_whitespace(node.get_text(" "))
    m = _DATE_RE.search(text)
    return m.group(0) if m else ""

def _make_id(url: str) -> str:
    return re.sub(r"\W+", "_", url) + "_" + str(int(time.time()))
