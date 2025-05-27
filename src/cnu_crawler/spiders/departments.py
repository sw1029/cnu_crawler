# cnu_crawler/spiders/departments.py
from loguru import logger

from src.cnu_crawler.core.fetcher import fetch_json, fetch_text
from src.cnu_crawler.core.parser import html_select
from src.cnu_crawler.storage import College, Department, get_session

async def crawl_departments(college: College):
    logger.info(f"🏫 {college.name} 학부/학과 크롤링")
    # ① JSON API가 있는 경우
    api_url = f"{college.url}/department/list.json"
    try:
        data = await fetch_json(api_url)
        dept_list = [{"code": d["deptCd"], "name": d["deptNm"], "url": d["url"]} for d in data]
    except Exception:
        # ② 정적 HTML fallback
        html = await fetch_text(college.url)
        hrefs = html_select(html, "a[href*='department']",
                            attr="href")  # 추정
        names = html_select(html, "a[href*='department']")
        dept_list = [{"code": href.split("/")[-2], "name": nm, "url": href}
                     for nm, href in zip(names, hrefs)]
    with get_session() as sess:
        for d in dept_list:
            obj = (sess.query(Department)
                        .filter_by(college_id=college.id, code=d["code"]).one_or_none())
            if obj:
                obj.name, obj.url = d["name"], d["url"]
            else:
                obj = Department(college_id=college.id, **d)
                sess.add(obj)
        sess.commit()
