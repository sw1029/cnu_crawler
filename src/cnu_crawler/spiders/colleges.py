# cnu_crawler/spiders/colleges.py
import re
from typing import List, Dict
from urllib.parse import urljoin  # 상대 URL을 절대 URL로 변환하기 위함

from loguru import logger
from selenium.webdriver.common.by import By  # XPath 등으로 요소를 찾기 위함
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnu_crawler.core.browser import get_driver
from cnu_crawler.storage import College, get_session
from cnu_crawler.utils import clean_text  # 텍스트 정제용


async def discover_colleges(root_url: str) -> List[Dict]:
    """
    메인 페이지에서 직접 HTML을 파싱하여 대학 목록을 추출합니다.
    사용자가 제공한 XPath 정보를 기반으로 작동합니다.
    """
    logger.info(f"🔍 대학 목록 탐색 중 (HTML 직접 파싱 방식): {root_url}")
    colleges_data: List[Dict] = []

    # 사용자 제공 XPath: 대학 목록을 포함하는 컨테이너
    COLLEGES_CONTAINER_XPATH = "/html/body/div[3]/div/div[3]"
    # 컨테이너 내부에서 각 대학을 나타내는 링크(<a> 태그)를 찾는 XPath
    # 예: /ul/li/a (li의 인덱스는 다양할 수 있으므로 //ul//li/a 와 같이 좀 더 일반적인 패턴 사용)
    # 또는 /ul/li/a (모든 li 밑의 a를 찾음)
    # 사용자가 "/ul/li[6]/a"와 같은 구체적인 예를 주셨으므로, 이 패턴을 일반화합니다.
    INDIVIDUAL_COLLEGE_LINK_XPATH = ".//ul//li/a"  # 컨테이너 XPath 기준 상대 경로

    try:
        with get_driver() as driver:
            driver.get(root_url)

            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, COLLEGES_CONTAINER_XPATH))
                )
                logger.info(f"대학 목록 컨테이너 XPath '{COLLEGES_CONTAINER_XPATH}' 발견됨.")
            except Exception as e_wait:
                logger.error(f"대학 목록 컨테이너 XPath '{COLLEGES_CONTAINER_XPATH}'를 찾는 중 타임아웃 또는 오류: {e_wait}")
                logger.debug(f"현재 페이지 소스 (일부): {driver.page_source[:1000]}")
                return []

            container_element = driver.find_element(By.XPATH, COLLEGES_CONTAINER_XPATH)
            college_link_elements = container_element.find_elements(By.XPATH, INDIVIDUAL_COLLEGE_LINK_XPATH)

            if not college_link_elements:
                logger.warning(
                    f"컨테이너('{COLLEGES_CONTAINER_XPATH}') 내에서 대학 링크 ('{INDIVIDUAL_COLLEGE_LINK_XPATH}')를 찾지 못했습니다. XPath 또는 웹사이트 구조 확인 필요.")
                return []

            logger.info(f"{len(college_link_elements)}개의 대학 링크 발견.")

            for idx, link_element in enumerate(college_link_elements):
                try:
                    college_name = clean_text(link_element.text)
                    college_url = link_element.get_attribute("href")

                    if not college_name:
                        logger.warning(
                            f"링크 요소에서 대학 이름을 찾을 수 없습니다 (인덱스: {idx}, 요소 HTML: {link_element.get_attribute('outerHTML')[:100]}). 건너뜁니다.")
                        continue
                    if not college_url:  # URL이 없는 경우는 거의 없겠지만 방어 코드
                        logger.warning(f"링크 요소에서 URL을 찾을 수 없습니다 (이름: {college_name}). 건너뜁니다.")
                        continue

                    college_url = urljoin(root_url, college_url)

                    # 대학 'code' 생성: URL의 마지막 유효한 경로 세그먼트 또는 이름 기반 슬러그
                    url_path_segments = [part for part in college_url.split('/') if
                                         part and part not in ('http:', 'https:', '')]
                    if url_path_segments:
                        # 예: http://example.com/college/abc -> abc
                        # 예: http://example.com/college/abc.html -> abc.html
                        # 예: http://example.com/college/abc?id=123 -> abc
                        college_code_candidate = url_path_segments[-1].split('?')[0].split('#')[0]
                    else:  # URL에서 코드 추출이 어려울 경우 이름 기반
                        college_code_candidate = college_name

                    # 코드 정제: 소문자, 공백->'-', 특수문자 제거, 길이 제한
                    college_code = re.sub(r'\s+', '-', college_code_candidate.lower())
                    college_code = re.sub(r'[^a-z0-9-_.]', '', college_code)[:50]  # 점(.)은 유지 (예: .html)
                    if not college_code:  # 모든 문자가 제거된 경우 대비
                        college_code = f"college-{idx + 1}"

                    colleges_data.append({
                        "code": college_code,
                        "name": college_name,
                        "url": college_url
                    })
                    logger.debug(f"추출된 대학: code='{college_code}', name='{college_name}', url='{college_url}'")

                except Exception as e_parse_element:
                    logger.error(f"개별 대학 링크 요소 파싱 중 오류 (인덱스: {idx}): {e_parse_element}")
                    logger.debug(f"오류 발생 요소 HTML (일부): {link_element.get_attribute('outerHTML')[:200]}")

    except Exception as e_main:
        logger.opt(exception=True).error(f"discover_colleges (HTML 파싱) 실행 중 예외 발생: {e_main}")
        return []

    if not colleges_data:
        logger.warning("추출된 대학 데이터가 없습니다.")
        return []

    try:
        with get_session() as sess:
            updated_count = 0
            added_count = 0
            for c_data in colleges_data:
                obj = sess.query(College).filter_by(code=c_data["code"]).one_or_none()
                if obj:
                    if obj.name != c_data["name"] or obj.url != c_data["url"]:
                        obj.name = c_data["name"]
                        obj.url = c_data["url"]
                        updated_count += 1
                else:
                    obj = College(**c_data)
                    sess.add(obj)
                    added_count += 1
            if updated_count > 0 or added_count > 0:
                sess.commit()
                logger.success(f"대학 정보 DB 업데이트: {added_count}개 추가, {updated_count}개 수정 (총 {len(colleges_data)}개 처리).")
            else:
                logger.info("DB에 변경된 대학 정보가 없습니다.")
    except Exception as e_db:
        logger.opt(exception=True).error(f"대학 정보 DB 저장 중 오류: {e_db}")
        # sess.rollback() # 필요한 경우 롤백

    return colleges_data