# src/cnu_crawler/spiders/departments.py
import re
import json
from urllib.parse import urljoin, urlparse, urlunparse
from typing import List, Dict, Optional

from loguru import logger
from aiohttp import ClientError
# BeautifulSoup을 사용한 상세 파싱이 필요할 경우 (현재는 html_select 사용)
# from bs4 import BeautifulSoup

from cnu_crawler.core.fetcher import fetch_text, fetch_json  # fetch_json은 거의 사용 안 함
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import College, Department, get_session  #
from cnu_crawler.utils import clean_text  #
from cnu_crawler.config import ROOT_URL  # 필요시 사용

# 학과 이름에 포함될 가능성이 있는 키워드 (HTML 파싱 시 검증용)
DEPT_KEYWORDS = ["학과", "학부", "전공", "department", "school of", "major", "division", "과정", "융합"]
# 학과 이름에서 제외할 키워드 (예: 일반 링크 방지)
EXCLUDE_KEYWORDS_IN_NAME = ["대학안내", "입학안내", "대학생활", "커뮤니티", "오시는길", "사이트맵", "소개", "더보기", "바로가기"]


def _generate_department_code(college_code: str, dept_name: str, url: str) -> str:
    """학과 코드를 생성합니다 (college_code 내에서 고유하도록)."""
    cleaned_name = re.sub(r'\s+', '', dept_name.lower())
    alnum_name = re.sub(r'[^a-z0-9]', '', cleaned_name)[:15]  # 이름 일부 사용

    # URL에서 의미 있는 부분 추출 시도 (더 정교한 로직 필요 가능)
    path_parts = [part for part in urlparse(url).path.split('/') if part and not part.endswith((".do", ".jsp"))]
    url_suffix = path_parts[-1][:10] if path_parts else ""

    base_code = f"{college_code[:10]}_{alnum_name}_{url_suffix}"
    # 해시를 추가하여 고유성 보장 시도
    return f"dept_{base_code}_{hash(url + dept_name)[:6]}"[:50]  # 최대 길이 제한


def _extract_notice_url_template_from_page(html_content: str, base_url: str, keywords: List[str]) -> Optional[str]:
    """
    주어진 HTML 내용에서 키워드를 포함하는 링크를 찾아 공지사항 URL 템플릿으로 추론합니다.
    이 함수는 매우 휴리스틱하며, 실제로는 더 정교한 방법이 필요합니다.
    """
    all_links = html_select(html_content, "a", attr="href")  #
    all_texts = html_select(html_content, "a")  #

    for text, href in zip(all_texts, all_links):
        cleaned_text = clean_text(text)
        if any(kw.lower() in cleaned_text.lower() for kw in keywords):
            # 링크가 유효한지, 페이지 파라미터를 어떻게 붙일지 등 추가 분석 필요
            full_url = urljoin(base_url, href)
            # 간단히 ?page={} 또는 &page={} 를 붙이는 형태로 가정
            # 실제로는 페이지 파라미터 이름과 형식을 알아내야 함
            parsed_url = urlparse(full_url)
            # 이미 쿼리가 있다면 &page={}, 없다면 ?page={}
            if parsed_url.query:
                return full_url + "&page={}"
            else:
                return full_url + "?page={}"
    return None


async def _parse_departments_from_grad_page(college: College, html_content: str) -> List[Dict]:
    """
    `grad.cnu.ac.kr` 페이지에서 특정 '대학명' (college.name) 하위의 학과 목록을 파싱합니다.
    요구사항: /html/body/div[1]/div[3]/div[2]/div[3]/div/div/div[1]/h4 대학명
              /html/body/div[1]/div[3]/div[2]/div[3]/div/div/div[1]/ul/li[1]/a == 학과명, href == link
    """
    depts_found: List[Dict] = []
    logger.debug(f"[{college.name}] 일반대학원 페이지 HTML 내용으로 학과 파싱 시작.")

    # FIXME: 아래 선택자들은 `grad.cnu.ac.kr` 페이지의 실제 HTML 구조를 분석하여 매우 정교하게 수정해야 합니다.
    # 현재는 제공된 XPath를 기반으로 CSS 선택자로 변환하려는 시도이며, 정확하지 않을 수 있습니다.
    # 1. 페이지를 "대학명" (h4)을 기준으로 섹션화합니다.
    # 2. 현재 처리 중인 `college.name`과 일치하거나 유사한 `h4` 텍스트를 찾습니다.
    # 3. 해당 `h4`와 관련된 `ul > li > a` 구조에서 학과 정보를 추출합니다.

    # 이 작업은 BeautifulSoup만으로 복잡할 수 있습니다. lxml과 XPath 사용이 더 적합할 수 있으나,
    # 현재 `html_select`는 BeautifulSoup을 사용합니다.
    # 아래는 매우 단순화된 접근이며, 실제로는 `h4`와 `ul`의 관계를 명확히 파악해야 합니다.

    # 예시: 모든 <div class="department_list02"> (대학 구분 컨테이너로 가정)를 찾고,
    # 그 안에서 <h4> (대학명)와 <ul><li><a> (학과)를 연결.
    # college_section_selector = "div.department_list02" # 실제 컨테이너 선택자로 변경
    # sections_html = html_select_elements_as_html(html_content, college_section_selector) # 이런 함수가 있다고 가정

    # 현재 college.name은 "인문대학(일반대학원소속)" 과 같은 형태일 수 있음
    # 여기서 (일반대학원소속) 부분을 제거하고 순수 대학명으로 비교해야 할 수 있음
    target_college_name_pure = college.name.split('(')[0]  # 예: "인문대학"

    # 매우 단순한 접근: 페이지 전체에서 ul > li > a 를 찾고, 부모 구조를 통해 대학명과 연결 시도 (한계 명확)
    # 더 나은 방법: 각 대학명을 감싸는 div를 찾고, 그 div 내의 ul > li > a 를 찾아야 함.
    # 예시: 페이지를 파싱하여 (h4_text, ul_of_depts_html) 쌍을 만듦.
    #       그 후 h4_text가 target_college_name_pure와 일치하는 ul_of_depts_html에서 학과 추출.
    # 이 부분은 상세한 HTML 구조 분석 없이는 정확한 구현이 어렵습니다.

    # 여기서는 "모든 학과 링크"를 가져온 후, 이들이 현재 College 객체에 속한다고 가정하는
    # 매우 단순화된 접근을 사용합니다 (spiders/colleges.py의 discover_grad_page_colleges_and_depts 에서도 유사한 문제)
    # 이 방식은 college.name과 실제 HTML의 h4 대학명을 매칭하지 않으므로 정확도가 떨어집니다.

    # FIXME: 아래 선택자는 /html/body/div[1]/div[3]/div[2]/div[3]/div/div/div[*]/ul/li/a 와 유사한 모든 학과를 가져오도록 해야 함
    #        그리고 가져온 학과가 현재 `college.name`에 해당하는지 확인하는 로직 필요.
    #        가장 좋은 것은, `college.name`과 일치하는 `h4`를 먼저 찾고, 그 `h4`의 형제 또는 자식 요소인 `ul`을 찾는 것입니다.
    department_links_selector = "//div[@class='department_box']//ul/li/a"  # 이전 답변의 예시 선택자

    hrefs = html_select(html_content, department_links_selector, attr="href")
    names = html_select(html_content, department_links_selector)

    if hrefs and names and len(hrefs) == len(names):
        logger.info(f"[{college.name}] 일반대학원 페이지에서 선택자 '{department_links_selector}'로 {len(hrefs)}개 학과 후보 발견.")
        for nm_raw, href_val in zip(names, hrefs):
            nm_cleaned = clean_text(nm_raw)
            if not nm_cleaned or not href_val: continue

            full_url = urljoin(college.url, href_val)  # college.url은 grad.cnu.ac.kr/...

            dept_code = _generate_department_code(college.code, nm_cleaned, full_url)

            # 공지사항 URL 템플릿은 이 학과 페이지(full_url)를 방문하여 찾아야 함
            # 임시로 비워두거나, 일반적인 패턴으로 추론 시도
            # 예: undergrad_tpl = _extract_notice_url_template_from_page(await fetch_text(full_url), full_url, ["공지", "학부"])
            #     grad_tpl = _extract_notice_url_template_from_page(await fetch_text(full_url), full_url, ["공지", "대학원"])

            depts_found.append({
                "code": dept_code, "name": nm_cleaned, "url": full_url,
                "dept_type": "grad_school_dept",  # Department 모델에 정의된 타입
                # "undergrad_notice_url_template": undergrad_tpl, # 실제로는 대학원 공지가 메인일 것
                # "grad_notice_url_template": grad_tpl,
            })
    else:
        logger.warning(f"[{college.name}] 일반대학원 페이지에서 학과 정보를 찾지 못했습니다 (선택자: '{department_links_selector}').")

    return depts_found


async def _parse_departments_from_normal_college(college: College, html_content: str) -> List[Dict]:
    """일반 단과대학 페이지에서 학과 목록을 파싱합니다. (이전 답변의 HTML Fallback 로직 개선)"""
    depts_found: List[Dict] = []
    logger.debug(f"[{college.name}] 일반 단과대학 HTML 내용으로 학과 파싱 시작.")

    selectors_to_try = [
        "a[href*='department']", "a[href*='dept']", "a[href*='major']",
        "a[href*='학부']", "a[href*='학과']",
        "nav a", "div[class*='nav'] a", "div[id*='nav'] a",
        "ul[class*='menu'] li a", "ul[id*='menu'] li a",
        "div.menu_area ul li a", "div.snb_wrap ul li a",
        "div[class*='dept_list'] a", "ul[class*='dept_list'] li a",
        # 다음은 매우 일반적이므로 주의해서 사용하고, 검증 로직이 중요
        # "ul li a",
    ]

    # 휴리스틱: 한 선택자로 찾은 학과 수가 이 범위를 벗어나면 부적절하다고 판단
    MIN_EXPECTED_DEPTS = 1  # 최소 1개는 있어야 함 (단일 학부 대학도 있으므로)
    MAX_EXPECTED_DEPTS = 40  # 매우 큰 단과대학 고려

    temp_html_depts_candidates = {}  # URL을 키로 하여 중복 방지

    for selector_idx, current_selector in enumerate(selectors_to_try):
        logger.debug(f"[{college.name}] HTML 선택자 시도 ({selector_idx + 1}/{len(selectors_to_try)}): '{current_selector}'")
        hrefs = html_select(html_content, current_selector, attr="href")
        names = html_select(html_content, current_selector)

        if hrefs and names and len(hrefs) == len(names):
            current_selector_valid_depts_count = 0
            for i in range(len(hrefs)):
                nm_cleaned = clean_text(names[i])
                href_val = hrefs[i]

                if not nm_cleaned or not href_val or len(nm_cleaned) < 2 or len(nm_cleaned) > 50:
                    continue
                if any(ex_kw in nm_cleaned for ex_kw in EXCLUDE_KEYWORDS_IN_NAME):
                    continue
                # 이름에 학과 키워드가 하나라도 포함되어야 함
                if not any(kw.lower() in nm_cleaned.lower() for kw in DEPT_KEYWORDS):
                    continue

                full_url = urljoin(college.url, href_val)
                # 외부 링크나 현재 대학 URL과 너무 다른 상위 경로 제외 (간단한 체크)
                if not full_url.startswith(college.url.rsplit('/', 1)[0]):  # 현재 college URL의 상위 디렉토리에서 시작하는지
                    if not full_url.startswith("http") or urlparse(full_url).netloc != urlparse(college.url).netloc:
                        continue  # 완전 다른 도메인이거나 프로토콜 없는 이상한 링크

                dept_code = _generate_department_code(college.code, nm_cleaned, full_url)

                if full_url not in temp_html_depts_candidates:  # 중복 URL 방지
                    temp_html_depts_candidates[full_url] = {
                        "code": dept_code, "name": nm_cleaned, "url": full_url,
                        "dept_type": "normal_dept",  #
                        "selector_used": current_selector  # 어떤 선택자로 찾았는지 기록 (디버깅용)
                    }
                    current_selector_valid_depts_count += 1

            # 현재 선택자로 찾은 유효 학과 수가 범위 내에 있고, 이전보다 많이 찾았다면 이 결과를 우선 사용
            if MIN_EXPECTED_DEPTS <= current_selector_valid_depts_count <= MAX_EXPECTED_DEPTS:
                if current_selector_valid_depts_count > len(depts_found):  # 더 많은 (적절한 수의) 학과를 찾은 선택자 사용
                    logger.info(
                        f"[{college.name}] 선택자 '{current_selector}'로 {current_selector_valid_depts_count}개의 유효 학과 정보 발견. 이 결과 사용.")
                    depts_found = [v for k, v in temp_html_depts_candidates.items() if
                                   v["selector_used"] == current_selector]
                    # 이 선택자가 성공적이라고 판단되면 더 이상 다른 선택자 시도 안 함 (선택사항)
                    # break

    # 최종적으로 depts_found가 비어있다면, temp_html_depts_candidates 중 가장 많은 것을 사용하거나, 모두 사용
    if not depts_found and temp_html_depts_candidates:
        logger.warning(f"[{college.name}] 주요 선택자에서 기대 범위 결과를 찾지 못함. 모든 후보({len(temp_html_depts_candidates)})를 임시 사용.")
        depts_found = list(temp_html_depts_candidates.values())
        # 여기서도 너무 많거나 적으면 필터링 필요
        if len(depts_found) > MAX_EXPECTED_DEPTS:
            logger.warning(f"[{college.name}] 후보가 너무 많아 ({len(depts_found)}) 일부만 사용하거나 추가 필터링 필요. (현재는 모두 사용)")
            # depts_found = depts_found[:MAX_EXPECTED_DEPTS] # 예시: 최대 개수 제한

    # `selector_used` 필드는 DB 저장 전에 제거
    return [{k: v for k, v in dept.items() if k != "selector_used"} for dept in depts_found]


async def _create_dept_for_plus_grad_school(college: College) -> List[Dict]:
    """plus.cnu.ac.kr에서 가져온 특수/전문대학원의 경우, 해당 College 자체를 Department로 취급하거나,
       페이지 내에서 공지사항 링크를 찾아 Department의 URL 템플릿에 설정합니다."""
    depts_found: List[Dict] = []
    logger.info(f"[{college.name}] Plus 특수/전문대학원 자체를 학과로 처리 또는 공지 링크 탐색.")

    # 해당 college.url 페이지에서 "공지사항", "notice" 등의 링크를 찾아 URL 템플릿으로 설정 시도
    undergrad_tpl = None
    grad_tpl = None  # 특수/전문대학원은 보통 학부/대학원 구분이 모호하거나 단일
    try:
        html_content = await fetch_text(college.url)
        undergrad_tpl = _extract_notice_url_template_from_page(html_content, college.url, ["공지사항", "notice", "공지"])
        # 대학원 공지가 별도로 있다면 키워드 추가
        # grad_tpl = _extract_notice_url_template_from_page(html_content, college.url, ["대학원공지", "gradnotice"])
        if undergrad_tpl:  # 찾았다면, 보통 대학원도 같은 패턴일 수 있음
            grad_tpl = undergrad_tpl
            logger.info(f"[{college.name}] 공지사항 URL 템플릿 추론: {undergrad_tpl}")

    except Exception as e:
        logger.error(f"[{college.name}] Plus 대학원 페이지({college.url})에서 공지사항 링크 탐색 중 오류: {e}")

    depts_found.append({
        "code": college.code + "_main_dept",  # College 코드를 기반으로 Department 코드 생성
        "name": college.name,  # College 이름을 Department 이름으로 사용
        "url": college.url,  # Department URL도 College URL과 동일하게 설정
        "dept_type": "plus_special_grad_dept",  #
        "undergrad_notice_url_template": undergrad_tpl,
        "grad_notice_url_template": grad_tpl,
        "academic_notice_url_template": undergrad_tpl,  # 학사공지도 일단 동일하게 설정 (추후 확인)
    })
    return depts_found


async def crawl_departments(college: College):
    logger.info(f"🏫 [{college.name} (Type: {college.college_type})] 학과/학부 정보 수집 시작")
    depts_to_save: List[Dict] = []

    # --- 인공지능학과는 하드코딩된 정보로 처리 ---
    # 인공지능학과는 어떤 College에 속하는지 명확하지 않으므로,
    # 여기서는 특정 College(예: 'normal_college' 타입의 '자연과학대학' 등)일 때만 추가하거나,
    # 아니면 College와 무관하게 한 번만 추가되도록 별도 관리 필요.
    # 여기서는 College 루프 내에 있으므로, 특정 College와 연결하거나, 한 번만 실행되도록 플래그 관리 필요.
    # 지금은 AI 학과가 특정 College 소속으로 DB에 들어가지 않는다고 가정하고,
    # 만약 AI학과를 위한 College 객체가 있다면 그 때 처리하도록 함.
    # 또는, `spiders/colleges.py`에서 AI학과를 위한 가상의 College를 만들 수도 있음.
    # 가장 간단한 방법은 `scheduler.py`에서 `crawl_departments` 루프 전에 별도로 AI 학과를 DB에 추가하는 것.
    # 여기서는 `college.code == "ai_college_placeholder"` 와 같은 가상의 조건으로 추가.
    if college.code == "AI_COLLEGE_CODE":  # 이 코드는 colleges.py에서 AI 대학을 위해 생성한 코드여야 함
        logger.info("인공지능학과(하드코딩) 정보를 Department로 추가 시도...")
        # 실제 공지사항 URL 템플릿 확인 필요
        ai_undergrad_tpl = "https://ai.cnu.ac.kr/ai/community/notice.do?mode=list&page={}"  # 예시
        ai_academic_tpl = "https://ai.cnu.ac.kr/ai/community/undergraduate_course_notice.do?mode=list&page={}"  # 예시

        depts_to_save.append({
            "college_id": college.id,  # 이 College가 AI 대학을 나타내야 함
            "code": "cnu_ai_dept",
            "name": "인공지능학과",
            "url": "https://ai.cnu.ac.kr/ai/index.do",
            "dept_type": "ai_hardcoded",  #
            "undergrad_notice_url_template": ai_undergrad_tpl,
            "academic_notice_url_template": ai_academic_tpl,
            "grad_notice_url_template": None  # 대학원 공지 별도 확인
        })

    # --- College 타입에 따른 분기 ---
    if college.college_type == "grad_page_college" or \
            (college.college_type == "plus_general_grad" and "grad.cnu.ac.kr" in college.url):
        # 일반대학원 페이지 (grad.cnu.ac.kr) 에서 학과 파싱
        try:
            html_content = await fetch_text(college.url)
            parsed_depts = await _parse_departments_from_grad_page(college, html_content)
            depts_to_save.extend(parsed_depts)
        except Exception as e:
            logger.error(f"[{college.name}] 일반대학원 페이지 학과 파싱 중 오류: {e}")

    elif college.college_type == "normal_college":
        # 일반 단과대학 페이지에서 학과 파싱 (기존 HTML Fallback 방식 개선)
        try:
            # 일반 단과대학은 JSON API 시도를 먼저 할 수도 있음 (현재는 HTML만 가정)
            # api_url = f"{college.url.rstrip('/')}/department/list.json"
            # try: json_data = await fetch_json(api_url) ...
            # except: html_fallback ...
            html_content = await fetch_text(college.url)
            parsed_depts = await _parse_departments_from_normal_college(college, html_content)
            depts_to_save.extend(parsed_depts)
        except Exception as e:
            logger.error(f"[{college.name}] 일반 단과대학 학과 파싱 중 오류: {e}")

    elif college.college_type == "plus_special_grad" or college.college_type == "plus_general_grad":
        # plus.cnu.ac.kr 에서 가져온 특수/전문대학원 또는 일반대학원 링크의 경우
        # 해당 College 자체를 하나의 Department로 취급하거나,
        # 해당 페이지 내에서 공지사항 링크를 찾아 URL 템플릿으로 설정
        try:
            parsed_depts = await _create_dept_for_plus_grad_school(college)
            depts_to_save.extend(parsed_depts)
        except Exception as e:
            logger.error(f"[{college.name}] Plus 대학원 기반 학과 생성 중 오류: {e}")

    else:
        logger.warning(f"[{college.name}] 알 수 없는 college_type ('{college.college_type}')으로 학과 정보를 처리할 수 없습니다.")

    # --- 최종 DB 저장 ---
    if not depts_to_save:
        logger.warning(f"[{college.name}] 최종적으로 DB에 저장할 학과 정보가 없습니다.")
        return

    # DB 저장 전 college_id 설정 및 중복 제거
    final_unique_depts_for_db = []
    seen_codes_in_college_for_db = set()
    for dept_info_dict in depts_to_save:
        dept_info_dict["college_id"] = college.id  # 현재 College의 ID를 할당
        if dept_info_dict["code"] not in seen_codes_in_college_for_db:
            final_unique_depts_for_db.append(dept_info_dict)
            seen_codes_in_college_for_db.add(dept_info_dict["code"])

    if len(final_unique_depts_for_db) != len(depts_to_save):
        logger.info(f"[{college.name}] DB 저장 전 중복 학과 코드 제거됨: {len(depts_to_save)} -> {len(final_unique_depts_for_db)}개")

    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for d_item_db in final_unique_depts_for_db:
            # DB 저장을 위해 모델 필드에 맞게 데이터 준비
            db_ready_dept_data = {
                "college_id": d_item_db["college_id"],
                "code": d_item_db["code"],
                "name": d_item_db["name"],
                "url": d_item_db["url"],
                "dept_type": d_item_db.get("dept_type", "unknown"),
                "academic_notice_url_template": d_item_db.get("academic_notice_url_template"),
                "undergrad_notice_url_template": d_item_db.get("undergrad_notice_url_template"),
                "grad_notice_url_template": d_item_db.get("grad_notice_url_template"),
                "specific_grad_keyword_notice_url": d_item_db.get("specific_grad_keyword_notice_url")
            }
            obj = (sess.query(Department)
                   .filter_by(college_id=db_ready_dept_data["college_id"],
                              code=db_ready_dept_data["code"]).one_or_none())
            if obj:
                changed = False
                for key, value in db_ready_dept_data.items():
                    if hasattr(obj, key) and getattr(obj, key) != value:
                        setattr(obj, key, value)
                        changed = True
                if changed:
                    updated_count += 1
                    logger.trace(
                        f"[{college.name}] 기존 학과 정보 업데이트: code='{db_ready_dept_data['code']}', name='{db_ready_dept_data['name']}'")

            else:
                obj = Department(**db_ready_dept_data)
                sess.add(obj)
                added_count += 1
                logger.trace(
                    f"[{college.name}] 새 학과 정보 추가: code='{db_ready_dept_data['code']}', name='{db_ready_dept_data['name']}'")

        if added_count > 0 or updated_count > 0:
            try:
                sess.commit()
                logger.success(f"[{college.name}] 학과 정보 DB 최종 업데이트: {added_count}개 추가, {updated_count}개 수정.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{college.name}] 학과 정보 DB 최종 저장 중 오류: {e_db}")
                sess.rollback()
        else:
            logger.info(f"[{college.name}] DB에 변경된 학과 정보가 없습니다 (처리된 고유 학과 수: {len(final_unique_depts_for_db)}).")