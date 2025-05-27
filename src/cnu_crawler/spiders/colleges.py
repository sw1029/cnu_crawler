# cnu_crawler/spiders/colleges.py
import json
import re
from typing import List, Dict
from loguru import logger

# Selenium WebDriverWait 및 By 추가 (필요시 DOM 직접 상호작용용)
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By

from cnu_crawler.core.browser import get_driver
from cnu_crawler.core.fetcher import fetch_json
from cnu_crawler.storage import College, get_session

# FIXME: 이 패턴은 'collegeList(숫자)' 형태의 JS 호출을 찾기 위함.
# 실제 웹사이트의 대학 목록 API 호출 방식이 변경되었다면 이 패턴도 수정 필요.
MENU_PATTERN = re.compile(r'collegeList\((\d+)\)')  #

# FIXME: 실제 API 엔드포인트에 사용될 수 있는 키워드. 변경 시 수정.
COLLEGE_API_KEYWORD = "collegeList"


async def discover_colleges(root_url: str) -> List[Dict]:
    """메인/대학 메뉴 네트워크 요청을 가로채 실제 API 패턴을 추출."""
    logger.info("🔍 대학 목록 탐색 중 …")
    colleges: List[Dict] = []

    try:
        with get_driver() as driver:
            driver.get(root_url)  #

            # 만약 특정 버튼 클릭 등 사용자 상호작용 후 API가 호출된다면,
            # 아래와 같은 WebDriverWait 로직이 필요할 수 있습니다.
            # 예: WebDriverWait(driver, 10).until(
            #         EC.presence_of_element_located((By.ID, "some_menu_button"))
            #     ).click()
            # 그리고 API 호출이 완료될 시간을 벌기 위해 time.sleep() 또는 다른 대기 조건 사용 가능

            logs = driver.get_log("performance")  #

        # 네트워크 로그에서 collegeList API 추출
        api_urls = set()
        for l in logs:
            try:
                log_message = json.loads(l["message"])["message"]
                if "params" in log_message and "request" in log_message["params"]:
                    url = log_message["params"]["request"]["url"]
                    if COLLEGE_API_KEYWORD in url:  #
                        api_urls.add(url)
            except (json.JSONDecodeError, KeyError) as e:
                logger.trace(f"네트워크 로그 파싱 중 오류 (무시 가능): {l}")
                continue

        if not api_urls:
            logger.warning(f"'{COLLEGE_API_KEYWORD}' 키워드를 포함하는 API URL을 찾지 못했습니다. 웹사이트 구조 변경 가능성 있음.")
            return []

        logger.info(f"발견된 대학 API URL 후보: {api_urls}")

        for api_idx, api_url in enumerate(api_urls):
            try:
                logger.debug(f"API URL ({api_idx + 1}/{len(api_urls)}) 처리 중: {api_url}")
                data = await fetch_json(api_url)  #

                # FIXME: 실제 API 응답 구조에 따라 아래 키들을 수정해야 합니다.
                # 예: data가 리스트가 아니라면, data.get('resultList', []) 등으로 접근
                if not isinstance(data, list):
                    logger.warning(f"API 응답이 리스트 형태가 아닙니다: {api_url}, 데이터: {str(data)[:200]}")
                    # 다양한 API 응답 구조에 대한 처리 추가 가능
                    # if isinstance(data, dict) and "key_for_college_list" in data:
                    # data = data["key_for_college_list"]
                    # else:
                    # continue # 다음 API URL 시도
                    continue

                for item_idx, c_item in enumerate(data):
                    # FIXME: 'collegeCd', 'collegeNm', 'url' 키가 실제 API 응답과 다를 경우 수정 필요.
                    code = c_item.get("collegeCd")  #
                    name = c_item.get("collegeNm")  #
                    url = c_item.get("url")  #

                    if not all([code, name, url]):
                        logger.warning(f"항목 {item_idx}에 필수 정보(code, name, url)가 누락되었습니다: {c_item}")
                        continue

                    colleges.append({"code": str(code), "name": str(name), "url": str(url)})

                # 여러 API URL 중 첫 번째 유효한 응답만 사용할 경우 break
                if colleges:  # 유효한 대학 정보를 하나라도 찾았다면
                    logger.info(f"API URL {api_url} 에서 대학 목록 성공적으로 추출.")
                    break


            except Exception as e:
                logger.error(f"대학 목록 API ({api_url}) 처리 중 오류: {e}")
                continue

        if not colleges:
            logger.error("어떤 API에서도 대학 목록을 추출하지 못했습니다.")
            return []

        # DB upsert
        with get_session() as sess:
            for c in colleges:
                obj = sess.query(College).filter_by(code=c["code"]).one_or_none()  #
                if obj:
                    obj.name, obj.url = c["name"], c["url"]  #
                else:
                    obj = College(**c)  #
                    sess.add(obj)  #
            sess.commit()  #
        logger.success(f"총 {len(colleges)}개 대학 정보 업데이트 완료.")
        return colleges

    except Exception as e:
        logger.opt(exception=True).error("discover_colleges 실행 중 예외 발생")
        return []