# cnu_crawler/core/parser.py
from bs4 import BeautifulSoup
from typing import List

def html_select(html: str, selector: str, attr: str | None = None) -> List[str]:
    """CSS 선택자로 텍스트/속성 리스트 추출."""
    soup = BeautifulSoup(html, "lxml")
    elems = soup.select(selector)
    if attr:
        return [e.get(attr, "").strip() for e in elems if e.get(attr)]
    return [e.get_text(strip=True) for e in elems]

def html_first(html: str, selector: str, attr: str | None = None) -> str | None:
    lst = html_select(html, selector, attr)
    return lst[0] if lst else None
