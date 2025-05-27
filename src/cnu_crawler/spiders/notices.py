# cnu_crawler/spiders/notices.py
from datetime import datetime
from typing import Dict, List
from loguru import logger

from src.cnu_crawler.core.fetcher import fetch_json, fetch_text
from src.cnu_crawler.core.parser import html_select
from src.cnu_crawler.storage import Department, Notice, get_session

BOARD_CODES = {"undergrad": "board?code=undergrad_notice",
               "grad": "board?code=grad_notice"}

async def crawl_board(dept: Department, board_key: str):
    """단일 게시판(학부/대학원) 증분 수집."""
    base = dept.url.rstrip("/")
    page = 1
    inserted = 0
    with get_session() as sess:
        last = (sess.query(Notice)
                    .filter_by(dept_id=dept.id, board=board_key)
                    .order_by(Notice.post_id.desc())
                    .first())
        last_id = last.post_id if last else "0"
    while True:
        list_url = f"{base}/{BOARD_CODES[board_key]}&page={page}"
        try:
            data = await fetch_json(list_url)
            posts = data["posts"] if isinstance(data, dict) else data
        except Exception:
            # HTML fallback
            html = await fetch_text(list_url)
            ids = html_select(html, "td.no", None)  # 게시글 번호
            titles = html_select(html, "td.title a")
            links = html_select(html, "td.title a", "href")
            dates = html_select(html, "td.date")
            posts = [{"id": i, "title": t, "url": l, "date": d}
                     for i, t, l, d in zip(ids, titles, links, dates)]
        fresh: List[Dict] = []
        for p in posts:
            if str(p["id"]) <= str(last_id):  # 증분 종료
                break
            fresh.append({
                "dept_id": dept.id,
                "board": board_key,
                "post_id": str(p["id"]),
                "title": p["title"],
                "url": p["url"] if p["url"].startswith("http") else base + p["url"],
                "posted_at": datetime.fromisoformat(p["date"]) if "T" in p["date"]
                              else datetime.strptime(p["date"], "%Y-%m-%d")
            })
        if not fresh:
            break
        with get_session() as sess:
            sess.bulk_insert_mappings(Notice, fresh)
            sess.commit()
        inserted += len(fresh)
        page += 1
    if inserted:
        logger.info(f"📄 {dept.name} ({board_key}) 새 글 {inserted}건")

async def crawl_department_notices(dept: Department):
    for board_key in BOARD_CODES:
        await crawl_board(dept, board_key)
