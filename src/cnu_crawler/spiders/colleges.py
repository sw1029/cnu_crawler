# src/cnu_crawler/spiders/colleges.py
import asyncio
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin

from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# BeautifulSoup을 사용하기 위한 import (선택적, Selenium만으로도 가능하나 복잡한 HTML 파싱에 유리)
# from bs4 import BeautifulSoup

from cnu_crawler.core.browser import get_driver
from cnu_crawler.core.fetcher import fetch_text  # 정적 페이지 HTML 가져오기용
from cnu_crawler.core.parser import html_select  # BeautifulSoup 기반 파서 헬퍼
from cnu_crawler.storage import College, get_session
from cnu_crawler.utils import clean_text
from cnu_crawler.config import ROOT_URL  #


# --- Helper Functions ---
def _generate_college_code(name: str, prefix: str = "coll") -> str:
    """대학 이름과 접두사를 기반으로 고유 코드를 생성합니다."""
    cleaned_name = re.sub(r'\s+', '', name.lower())  # 공백 제거, 소문자화
    # 한글 등 비-알파벳 문자 처리 (간단히 제거 또는 영문 음차 변환 고려)
    # 여기서는 간단히 비-알파벳문자 제거 후 앞부분 사용
    alnum_name = re.sub(r'[^a-z0-9]', '', cleaned_name)
    return f"{prefix}_{alnum_name[:20]}_{hash(name)[:6]}"  # 이름 해시 일부 추가하여 고유성 증대


def _save_colleges_to_db(colleges_data: List[Dict], log_prefix: str):
    """수집된 College 정보를 DB에 저장하거나 업데이트합니다."""
    if not colleges_data:
        logger.info(f"[{log_prefix}] DB에 저장할 College 정보가 없습니다.")
        return

    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for c_data in colleges_data:
            # code는 반드시 고유해야 함
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
    """
    ROOT_URL (plus.cnu.ac.kr)에서 일반 단과대학 목록을 파싱합니다.
    XPath: /html/body/div[3]/div/div[3] (컨테이너)
           .//ul//li/a (개별 대학 링크)
    """
    logger.info(f"🔍 일반 단과대학 목록 탐색 시작 (출처: {root_url})")
    colleges_data: List[Dict] = []
    # 제공된 XPath
    COLLEGES_CONTAINER_XPATH = "/html/body/div[3]/div/div[3]"
    INDIVIDUAL_COLLEGE_LINK_XPATH = ".//ul//li/a"

    try:
        with get_driver() as driver:  # Selenium 사용
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

            logger.info(f"[{root_url}] {len(college_link_elements)}개의 일반 단과대학 링크 후보 발견.")
            for idx, link_element in enumerate(college_link_elements):
                college_name_raw = link_element.get_attribute("textContent")
                college_name = clean_text(college_name_raw if college_name_raw else "")
                college_url_raw = link_element.get_attribute("href")

                if not college_name or not college_url_raw:
                    logger.warning(f"일반 단과대학 링크에서 이름 또는 URL 누락 (인덱스: {idx}). 건너뜁니다.")
                    continue

                college_url = urljoin(root_url, college_url_raw)

                # 대학 코드 생성 (대학 이름 기반, 접두사 사용)
                college_code = _generate_college_code(college_name, prefix="plus_normal")

                colleges_data.append({
                    "code": college_code,
                    "name": college_name,
                    "url": college_url,
                    "college_type": "normal_college"  # 모델에 정의된 타입 사용
                })

        _save_colleges_to_db(colleges_data, "Plus 일반 단과대학")
        return colleges_data
    except Exception as e:
        logger.opt(exception=True).error(f"Plus 일반 단과대학 목록 탐색 중 예외: {e}")
        return []


async def discover_grad_page_colleges_and_depts(grad_info_url: str = "https://grad.cnu.ac.kr/grad/grad/normal-grad.do"):
    """
    grad.cnu.ac.kr 페이지에서 '대학명'(h4)을 College로, 그 하위 학과(ul/li/a)를 Department로 파싱합니다.
    이 함수는 College와 Department 정보를 함께 처리하거나, College 정보만 반환하고 Department 파싱은
    departments.py에서 하도록 역할 분담할 수 있습니다. 여기서는 College 정보만 우선 생성합니다.
    Department 정보는 departments.py에서 이 College 정보를 바탕으로 파싱하도록 합니다.
    """
    logger.info(f"🎓 일반대학원 페이지({grad_info_url})에서 '대학' 단위(소속) 탐색 시작...")
    colleges_data: List[Dict] = []

    try:
        html_content = await fetch_text(grad_info_url)  # 정적 HTML 가정

        # 사용자 제공 XPath: /html/body/div[1]/div[3]/div[2]/div[3]/div/div/div[1]/h4 대학명
        # 이 XPath는 첫 번째 '대학명'만 가리킵니다. 모든 '대학명'을 포함하는 반복적인 구조를 찾아야 합니다.
        # 예를 들어, 각 대학 섹션이 <div class="department_list02"> 같은 것으로 감싸져 있고, 그 안에 <h4>가 있다면,
        # 선택자는 "//div[@class='department_list02']//h4" 또는 더 정확한 경로가 될 수 있습니다.
        # html_select는 BeautifulSoup 기반이므로 전체 XPath 지원에 한계가 있을 수 있어 CSS 선택자 사용이 권장됩니다.

        # FIXME: 아래 선택자는 `grad.cnu.ac.kr` 페이지의 실제 HTML 구조를 분석하여 정확하게 수정해야 합니다.
        # 각 대학(예: 인문대학, 사회과학대학 등)의 이름(<h4>)을 포함하는 컨테이너를 찾아야 합니다.
        # 예를 들어, 각 대학 섹션이 <div class="college_section"> 같은 태그로 반복된다면,
        # college_sections = html_select(html_content, "div.college_section") # 이런 식으로 섹션을 먼저 찾고
        # for section_html in college_sections:
        #     college_name = html_select(section_html, "h4") # 섹션 내에서 h4 (대학명)
        #     ...

        # 현재는 페이지 전체에서 <h4> 태그 중 "대학"으로 끝나는 것을 찾는 단순한 방식으로 시도합니다.
        # 이는 정확도가 낮을 수 있으므로, 반드시 실제 구조에 맞는 선택자로 개선해야 합니다.
        h4_elements = html_select(html_content, "h4")  # 페이지 내 모든 h4 태그

        processed_college_names = set()

        if not h4_elements:
            logger.warning(f"'{grad_info_url}' 에서 <h4> 태그 (대학명 후보)를 찾지 못했습니다.")
        else:
            logger.info(f"'{grad_info_url}' 에서 {len(h4_elements)}개의 <h4> 태그 발견. '대학'으로 끝나는지 필터링 시도.")
            for name_raw in h4_elements:
                name = clean_text(name_raw)
                # "대학"으로 끝나고, 너무 짧지 않으며, 특정 제외 키워드가 없는 경우를 대학명으로 간주 (휴리스틱)
                if name.endswith("대학") and len(name) > 3 and name not in processed_college_names \
                        and not any(ex in name for ex in ["공지사항", "자료실"]):  # 예시 제외 키워드

                    college_code = _generate_college_code(name, prefix="gradpage")
                    # 이 대학의 URL은 grad_info_url 자체로 설정하거나, 각 대학별 페이지가 있다면 그 URL을 사용해야 합니다.
                    # 여기서는 grad_info_url을 대표 URL로 사용합니다.
                    colleges_data.append({
                        "code": code,
                        "name": f"{name}(일반대학원소속)",  # 출처 명시
                        "url": grad_info_url,  # 이 URL은 대학 대표 URL이 아닐 수 있음. 주의.
                        "college_type": "grad_page_college"  # 모델에 정의된 타입
                    })
                    processed_college_names.add(name)
            logger.info(f"일반대학원 페이지에서 {len(colleges_data)}개의 '대학' 단위 정보 추출.")

        # 만약 위에서 아무것도 찾지 못했거나, "일반대학원" 자체를 하나의 College로 등록하고 싶다면:
        if not colleges_data:
            logger.info(f"일반대학원 페이지에서 개별 '대학' 단위를 찾지 못해 '일반대학원' 전체를 하나의 College로 등록합니다.")
            colleges_data.append({
                "code": "grad_school_main_unit",  # 고유 코드
                "name": "일반대학원(전체)",
                "url": grad_info_url,
                "college_type": "grad_page_college"  # 또는 다른 타입 (예: 'general_graduate_school_itself')
            })

        _save_colleges_to_db(colleges_data, "일반대학원 페이지 기반 '대학' 단위")
        return colleges_data
    except Exception as e:
        logger.opt(exception=True).error(f"일반대학원 페이지({grad_info_url}) '대학' 단위 탐색 중 예외: {e}")
        return []


async def discover_plus_all_graduate_schools(plus_url: str = ROOT_URL) -> List[Dict]:
    """
    ROOT_URL (plus.cnu.ac.kr)의 '대학원' 섹션에서 모든 대학원(일반, 특수, 전문) 링크를 가져옵니다.
    XPath: /html/body/div[3]/div/div[2]/ul/li/a
    """
    logger.info(f"🔍 Plus ({plus_url}) 전체 대학원 목록 탐색 시작...")
    colleges_data: List[Dict] = []
    # 사용자 제공 XPath: /html/body/div[3]/div/div[2]/ul/li[1]/a (일반대학원)
    #                 /html/body/div[3]/div/div[2]/ul/li[2]/a (그 외)
    # 전체를 포함하는 XPath: /html/body/div[3]/div/div[2]/ul/li/a
    GRADUATE_SCHOOL_LINK_XPATH = "/html/body/div[3]/div/div[2]/ul/li/a"

    try:
        with get_driver() as driver:  # Selenium 사용
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

                # college_type 결정: 첫 번째 링크(li[1]/a)는 일반대학원, 나머지는 특수/전문대학원으로 가정
                # XPath 인덱스는 1부터 시작
                college_type = "plus_general_grad" if (idx == 0) else "plus_special_grad"

                # 코드 생성
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
    """ 모든 종류의 College 정보를 수집하는 통합 진입점 함수 """
    logger.info("모든 College 정보 수집 작업을 시작합니다.")

    # 기존 discover_colleges의 역할을 하는 함수 호출 (plus.cnu.ac.kr의 일반 단과대학)
    await discover_plus_normal_colleges(ROOT_URL)

    # grad.cnu.ac.kr 페이지에서 "대학명"들을 College로 등록
    await discover_grad_page_colleges_and_depts("https://grad.cnu.ac.kr/grad/grad/normal-grad.do")

    # plus.cnu.ac.kr의 "대학원" 섹션 링크들을 College로 등록
    await discover_plus_all_graduate_schools(ROOT_URL)

    # 모든 작업 완료 후 DB에서 전체 College 목록을 가져와서 반환할 수도 있으나,
    # 여기서는 각 함수가 DB에 저장하는 것으로 처리하고, 반환값은 사용하지 않음.
    # 스케줄러에서는 DB에서 직접 College 목록을 읽어 다음 단계를 진행.
    logger.info("모든 College 정보 수집 작업 완료.")