# cnu_crawler/spiders/colleges.py
import asyncio
import json
import re
from typing import List, Dict
from loguru import logger

from ..core.browser import get_driver
from ..core.fetcher import fetch_json
from ..core.parser import html_select
from ..storage.models import College, get_session

MENU_PATTERN = re.compile(r'collegeList\((\d+)\)')

async def discover_colleges(root_url: str) -> List[Dict]:
    """메인/대학 메뉴 네트워크 요청을 가로채 실제 API 패턴을 추출."""
    logger.info("🔍 대학 목록 탐색 중 …")
    with get_driver() as driver:
        driver.get(root_url)
        # 네트워크 요청 목록 (Chrome DevTools Protocol 이용)
        logs = driver.get_log("performance")
    # 네트워크 로그에서 collegeList API 추출
    api_urls = {json.loads(l["message"])["message"]["params"]["request"]["url"]
                for l in logs
                if "collegeList" in l["message"]}
    # 예상: 단일 URL
    colleges: List[Dict] = []
    for api in api_urls:
        data = await fetch_json(api)
        # [{collegeCd, collegeNm, url}, …] 형태 예상
        for c in data:
            colleges.append({"code": c["collegeCd"], "name": c["collegeNm"], "url": c["url"]})
    # DB upsert
    with get_session() as sess:
        for c in colleges:
            obj = sess.query(College).filter_by(code=c["code"]).one_or_none()
            if obj:
                obj.name, obj.url = c["name"], c["url"]
            else:
                obj = College(**c)
                sess.add(obj)
        sess.commit()
    return colleges
