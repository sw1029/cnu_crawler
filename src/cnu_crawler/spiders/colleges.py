# src/cnu_crawler/spiders/colleges.py
import asyncio
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin

from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from cnu_crawler.core.browser import get_driver
from cnu_crawler.core.fetcher import fetch_text
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import College, get_session
from cnu_crawler.utils import clean_text
from cnu_crawler.config import ROOT_URL


# --- Helper Functions ---
def _generate_college_code(name: str, prefix: str = "coll") -> str:
    """대학 이름과 접두사를 기반으로 고유 코드를 생성합니다."""
    cleaned_name = re.sub(r'\s+', '', name.lower())
    alnum_name = re.sub(r'[^a-z0-9]', '', cleaned_name)

    # hash(name)의 결과를 문자열로 변환 후 슬라이싱합니다.
    # hash() 결과가 음수일 수 있으므로, str() 변환 후 '-' 문자를 처리하거나,
    # hex()를 사용하여 일관된 형식의 문자열을 얻는 것을 고려할 수 있습니다.
    # 여기서는 간단히 str()을 사용하고, 음수 부호가 포함될 수 있음을 인지합니다.
    # 더 일관된 결과를 위해 hex(hash(name))를 사용하거나, str(abs(hash(name)))을 사용할 수 있습니다.
    hash_str_part = str(hash(name)).replace('-', '')[:6]  # 음수 부호 제거 후 6자리

    return f"{prefix}_{alnum_name[:20]}_{hash_str_part}"  # 수정된 부분


def _save_colleges_to_db(colleges_data: List[Dict], log_prefix: str):
    """수집된 College 정보를 DB에 저장하거나 업데이트합니다."""
    if not colleges_data:
        logger.info(f"[{log_prefix}] DB에 저장할 College 정보가 없습니다.")
        return

    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for c_data in colleges_data:
            existing_college = sess.query(College).filter_by(code=c_data["code"]).one_or_none()
            if existing_college:
                changed = False
                if existing_college.name != c_data["name"]:
                    existing_college.name = c_data["name"]
                    changed = True
                if existing_college.url != c_data["url"]:
                    existing_college.url = c_data["url"]
                    changed = True
                if existing_college.college_type != c_data.get("college_type", existing_college.college_type):
                    existing_college.college_type = c_data.get("college_type", existing_college.college_type)
                    changed = True

                if changed:
                    updated_count += 1
                    logger.trace(f"[{log_prefix}] 기존 College 업데이트: code='{c_data['code']}', name='{c_data['name']}'")
            else:
                new_college = College(**c_data)
                sess.add(new_college)
                added_count += 1
                logger.trace(f"[{log_prefix}] 새 College 추가: code='{c_data['code']}', name='{c_data['name']}'")

        if added_count > 0 or updated_count > 0:
            try:
                sess.commit()
                logger.success(f"[{log_prefix}] College 정보 DB 업데이트: {added_count}개 추가, {updated_count}개 수정.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{log_prefix}] College 정보 DB 저장 중 오류: {e_db}")
                sess.rollback()
        else:
            logger.info(f"[{log_prefix}] DB에 변경된 College 정보가 없습니다.")


# --- Main Discover Functions ---

async def discover_plus_normal_colleges(root_url: str = ROOT_URL) -> List[Dict]:
    logger.info(f"🔍 일반 단과대학 목록 탐색 시작 (출처: {root_url})")  #
    colleges_data: List[Dict] = []
    COLLEGES_CONTAINER_XPATH = "/html/body/div[3]/div/div[3]"
    INDIVIDUAL_COLLEGE_LINK_XPATH = ".//ul//li/a"

    try:
        with get_driver() as driver:
            driver.get(root_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, COLLEGES_CONTAINER_XPATH))
            )
            container_element = driver.find_element(By.XPATH, COLLEGES_CONTAINER_XPATH)
            college_link_elements = container_element.find_elements(By.XPATH, INDIVIDUAL_COLLEGE_LINK_XPATH)

            if not college_link_elements:
                logger.warning(
                    f"[{root_url}] 컨테이너('{COLLEGES_CONTAINER_XPATH}') 내에서 대학 링크 ('{INDIVIDUAL_COLLEGE_LINK_XPATH}')를 찾지 못했습니다.")
                return []

            logger.info(f"[{root_url}] {len(college_link_elements)}개의 일반 단과대학 링크 후보 발견.")  #
            for idx, link_element in enumerate(college_link_elements):
                college_name_raw = link_element.get_attribute("textContent")
                college_name = clean_text(college_name_raw if college_name_raw else "")
                college_url_raw = link_element.get_attribute("href")

                if not college_name or not college_url_raw:
                    logger.warning(f"일반 단과대학 링크에서 이름 또는 URL 누락 (인덱스: {idx}). 건너뜁니다.")
                    continue

                college_url = urljoin(root_url, college_url_raw)
                college_code = _generate_college_code(college_name, prefix="plus_normal")  #

                colleges_data.append({
                    "code": college_code,
                    "name": college_name,
                    "url": college_url,
                    "college_type": "normal_college"
                })

        _save_colleges_to_db(colleges_data, "Plus 일반 단과대학")
        return colleges_data
    except Exception as e:
        logger.opt(exception=True).error(f"Plus 일반 단과대학 목록 탐색 중 예외: {e}")  #
        return []


async def discover_grad_page_colleges_and_depts(grad_info_url: str = "https://grad.cnu.ac.kr/grad/grad/normal-grad.do"):
    logger.info(f"🎓 일반대학원 페이지({grad_info_url})에서 '대학' 단위(소속) 탐색 시작...")  #
    colleges_data: List[Dict] = []

    try:
        html_content = await fetch_text(grad_info_url)
        h4_elements = html_select(html_content, "h4")

        processed_college_names = set()

        if not h4_elements:
            logger.warning(f"'{grad_info_url}' 에서 <h4> 태그 (대학명 후보)를 찾지 못했습니다.")
        else:
            logger.info(f"'{grad_info_url}' 에서 {len(h4_elements)}개의 <h4> 태그 발견. '대학'으로 끝나는지 필터링 시도.")  #
            for name_raw in h4_elements:
                name = clean_text(name_raw)
                if name.endswith("대학") and len(name) > 3 and name not in processed_college_names \
                        and not any(ex in name for ex in ["공지사항", "자료실"]):
                    college_code = _generate_college_code(name, prefix="gradpage")  #
                    colleges_data.append({
                        "code": code,
                        "name": f"{name}(일반대학원소속)",
                        "url": grad_info_url,
                        "college_type": "grad_page_college"
                    })
                    processed_college_names.add(name)
            logger.info(f"일반대학원 페이지에서 {len(colleges_data)}개의 '대학' 단위 정보 추출.")

        if not colleges_data:
            logger.info(f"일반대학원 페이지에서 개별 '대학' 단위를 찾지 못해 '일반대학원' 전체를 하나의 College로 등록합니다.")
            colleges_data.append({
                "code": "grad_school_main_unit",
                "name": "일반대학원(전체)",
                "url": grad_info_url,
                "college_type": "grad_page_college"
            })

        _save_colleges_to_db(colleges_data, "일반대학원 페이지 기반 '대학' 단위")
        return colleges_data
    except Exception as e:
        logger.opt(exception=True).error(f"일반대학원 페이지({grad_info_url}) '대학' 단위 탐색 중 예외: {e}")  #
        return []


async def discover_plus_all_graduate_schools(plus_url: str = ROOT_URL) -> List[Dict]:
    logger.info(f"🔍 Plus ({plus_url}) 전체 대학원 목록 탐색 시작...")
    colleges_data: List[Dict] = []
    GRADUATE_SCHOOL_LINK_XPATH = "/html/body/div[3]/div/div[2]/ul/li/a"

    try:
        with get_driver() as driver:
            driver.get(plus_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.XPATH, GRADUATE_SCHOOL_LINK_XPATH))
            )
            grad_school_link_elements = driver.find_elements(By.XPATH, GRADUATE_SCHOOL_LINK_XPATH)

            if not grad_school_link_elements:
                logger.warning(f"Plus({plus_url})에서 전체 대학원 링크를 찾지 못했습니다 (XPath: {GRADUATE_SCHOOL_LINK_XPATH}).")
                return []

            logger.info(f"Plus({plus_url})에서 {len(grad_school_link_elements)}개의 전체 대학원 링크 후보 발견.")
            for idx, link_element in enumerate(grad_school_link_elements):
                name_raw = link_element.text
                name = clean_text(name_raw if name_raw else "")
                url_raw = link_element.get_attribute("href")

                if not name or not url_raw:
                    logger.warning(f"Plus 전체 대학원 링크에서 이름 또는 URL 누락 (인덱스: {idx}). 건너뜁니다.")
                    continue

                url = urljoin(plus_url, url_raw)
                college_type = "plus_general_grad" if (idx == 0) else "plus_special_grad"
                code_prefix = "plus_gen_grad" if college_type == "plus_general_grad" else "plus_spec_grad"
                college_code = _generate_college_code(name, prefix=code_prefix)

                colleges_data.append({
                    "code": college_code,
                    "name": name,
                    "url": url,
                    "college_type": college_type
                })

        _save_colleges_to_db(colleges_data, "Plus 전체 대학원")
        return colleges_data
    except Exception as e:
        logger.opt(exception=True).error(f"Plus 전체 대학원 목록 탐색 중 예외: {e}")
        return []


async def discover_all_colleges_entrypoint():
    logger.info("모든 College 정보 수집 작업을 시작합니다.")
    await discover_plus_normal_colleges(ROOT_URL)
    await discover_grad_page_colleges_and_depts("https://grad.cnu.ac.kr/grad/grad/normal-grad.do")
    await discover_plus_all_graduate_schools(ROOT_URL)
    logger.info("모든 College 정보 수집 작업 완료.")