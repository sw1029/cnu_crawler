# src/cnu_crawler/spiders/departments.py
import re
import json
from urllib.parse import urljoin, urlparse, urlunparse
from typing import List, Dict, Optional

from loguru import logger
from aiohttp import ClientError

from cnu_crawler.core.fetcher import fetch_text, fetch_json
from cnu_crawler.core.parser import html_select  #
from cnu_crawler.storage import College, Department, get_session
from cnu_crawler.utils import clean_text
from cnu_crawler.config import ROOT_URL

DEPT_KEYWORDS = ["학과", "학부", "전공", "department", "school of", "major", "division", "과정", "융합"]
EXCLUDE_KEYWORDS_IN_NAME = ["대학안내", "입학안내", "대학생활", "커뮤니티", "오시는길", "사이트맵", "소개", "더보기", "바로가기"]


def _generate_department_code(college_code: str, dept_name: str, url: str) -> str:
    cleaned_name = re.sub(r'\s+', '', dept_name.lower())
    alnum_name = re.sub(r'[^a-z0-9]', '', cleaned_name)[:15]
    path_parts = [part for part in urlparse(url).path.split('/') if part and not part.endswith((".do", ".jsp"))]
    url_suffix = path_parts[-1][:10] if path_parts else ""
    base_code = f"{college_code[:10]}_{alnum_name}_{url_suffix}"
    return f"dept_{base_code}_{hash(url + dept_name)[:6]}"[:50]


def _extract_notice_url_template_from_page(html_content: str, base_url: str, keywords: List[str]) -> Optional[str]:
    all_links = html_select(html_content, "a", attr="href")
    all_texts = html_select(html_content, "a")

    for text, href in zip(all_texts, all_links):
        cleaned_text = clean_text(text)
        if any(kw.lower() in cleaned_text.lower() for kw in keywords):
            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            if parsed_url.query:
                return full_url + "&page={}"
            else:
                return full_url + "?page={}"
    return None


async def _parse_departments_from_grad_page(college: College, html_content: str) -> List[Dict]:
    depts_found: List[Dict] = []
    logger.debug(f"[{college.name}] 일반대학원 페이지 HTML 내용으로 학과 파싱 시작.")  #

    # XPath //div[@class='department_box']//ul/li/a 를 CSS 선택자로 변경
    # department_links_selector = "//div[@class='department_box']//ul/li/a" # 기존 XPath
    department_links_selector = "div.department_box ul li a"  # 수정된 CSS 선택자

    hrefs = html_select(html_content, department_links_selector, attr="href")
    names = html_select(html_content, department_links_selector)

    if hrefs and names and len(hrefs) == len(names):
        logger.info(f"[{college.name}] 일반대학원 페이지에서 선택자 '{department_links_selector}'로 {len(hrefs)}개 학과 후보 발견.")
        for nm_raw, href_val in zip(names, hrefs):
            nm_cleaned = clean_text(nm_raw)
            if not nm_cleaned or not href_val: continue

            full_url = urljoin(college.url, href_val)
            dept_code = _generate_department_code(college.code, nm_cleaned, full_url)

            depts_found.append({
                "code": dept_code, "name": nm_cleaned, "url": full_url,
                "dept_type": "grad_school_dept",
            })
    else:
        logger.warning(f"[{college.name}] 일반대학원 페이지에서 학과 정보를 찾지 못했습니다 (선택자: '{department_links_selector}').")

    return depts_found


async def _parse_departments_from_normal_college(college: College, html_content: str) -> List[Dict]:
    depts_found: List[Dict] = []
    logger.debug(f"[{college.name}] 일반 단과대학 HTML 내용으로 학과 파싱 시작.")

    selectors_to_try = [
        "a[href*='department']", "a[href*='dept']", "a[href*='major']",
        "a[href*='학부']", "a[href*='학과']",
        "nav a", "div[class*='nav'] a", "div[id*='nav'] a",
        "ul[class*='menu'] li a", "ul[id*='menu'] li a",
        "div.menu_area ul li a", "div.snb_wrap ul li a",
        "div[class*='dept_list'] a", "ul[class*='dept_list'] li a",
    ]

    MIN_EXPECTED_DEPTS = 1
    MAX_EXPECTED_DEPTS = 40
    temp_html_depts_candidates = {}

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
                if not any(kw.lower() in nm_cleaned.lower() for kw in DEPT_KEYWORDS):
                    continue

                full_url = urljoin(college.url, href_val)
                if not full_url.startswith(college.url.rsplit('/', 1)[0]):
                    if not full_url.startswith("http") or urlparse(full_url).netloc != urlparse(college.url).netloc:
                        continue

                dept_code = _generate_department_code(college.code, nm_cleaned, full_url)

                if full_url not in temp_html_depts_candidates:
                    temp_html_depts_candidates[full_url] = {
                        "code": dept_code, "name": nm_cleaned, "url": full_url,
                        "dept_type": "normal_dept",
                        "selector_used": current_selector
                    }
                    current_selector_valid_depts_count += 1

            if MIN_EXPECTED_DEPTS <= current_selector_valid_depts_count <= MAX_EXPECTED_DEPTS:
                if current_selector_valid_depts_count > len(depts_found):
                    logger.info(
                        f"[{college.name}] 선택자 '{current_selector}'로 {current_selector_valid_depts_count}개의 유효 학과 정보 발견. 이 결과 사용.")
                    depts_found = [v for k, v in temp_html_depts_candidates.items() if
                                   v["selector_used"] == current_selector]

    if not depts_found and temp_html_depts_candidates:
        logger.warning(f"[{college.name}] 주요 선택자에서 기대 범위 결과를 찾지 못함. 모든 후보({len(temp_html_depts_candidates)})를 임시 사용.")
        depts_found = list(temp_html_depts_candidates.values())
        if len(depts_found) > MAX_EXPECTED_DEPTS:
            logger.warning(f"[{college.name}] 후보가 너무 많아 ({len(depts_found)}) 일부만 사용하거나 추가 필터링 필요.")

    return [{k: v for k, v in dept.items() if k != "selector_used"} for dept in depts_found]


async def _create_dept_for_plus_grad_school(college: College) -> List[Dict]:
    depts_found: List[Dict] = []
    logger.info(f"[{college.name}] Plus 특수/전문대학원 자체를 학과로 처리 또는 공지 링크 탐색.")
    undergrad_tpl = None
    grad_tpl = None
    try:
        html_content = await fetch_text(college.url)
        undergrad_tpl = _extract_notice_url_template_from_page(html_content, college.url, ["공지사항", "notice", "공지"])
        if undergrad_tpl:
            grad_tpl = undergrad_tpl
            logger.info(f"[{college.name}] 공지사항 URL 템플릿 추론: {undergrad_tpl}")
    except Exception as e:
        logger.error(f"[{college.name}] Plus 대학원 페이지({college.url})에서 공지사항 링크 탐색 중 오류: {e}")

    depts_found.append({
        "code": college.code + "_main_dept",
        "name": college.name,
        "url": college.url,
        "dept_type": "plus_special_grad_dept",
        "undergrad_notice_url_template": undergrad_tpl,
        "grad_notice_url_template": grad_tpl,
        "academic_notice_url_template": undergrad_tpl,
    })
    return depts_found


async def crawl_departments(college: College):
    logger.info(f"🏫 [{college.name} (Type: {college.college_type})] 학과/학부 정보 수집 시작")  #
    depts_to_save: List[Dict] = []

    if college.code == "AI_COLLEGE_CODE":
        logger.info("인공지능학과(하드코딩) 정보를 Department로 추가 시도...")
        ai_undergrad_tpl = "https://ai.cnu.ac.kr/ai/community/notice.do?mode=list&page={}"
        ai_academic_tpl = "https://ai.cnu.ac.kr/ai/community/undergraduate_course_notice.do?mode=list&page={}"

        depts_to_save.append({
            "college_id": college.id,
            "code": "cnu_ai_dept",
            "name": "인공지능학과",
            "url": "https://ai.cnu.ac.kr/ai/index.do",
            "dept_type": "ai_hardcoded",
            "undergrad_notice_url_template": ai_undergrad_tpl,
            "academic_notice_url_template": ai_academic_tpl,
            "grad_notice_url_template": None
        })

    if college.college_type == "grad_page_college" or \
            (college.college_type == "plus_general_grad" and "grad.cnu.ac.kr" in college.url):
        try:
            html_content = await fetch_text(college.url)
            parsed_depts = await _parse_departments_from_grad_page(college, html_content)  #
            depts_to_save.extend(parsed_depts)
        except Exception as e:  # 이 예외 처리 블록이 로그에 찍힌 라인 288에 해당합니다.
            logger.error(f"[{college.name}] 일반대학원 페이지 학과 파싱 중 오류: {e}")

    elif college.college_type == "normal_college":
        try:
            html_content = await fetch_text(college.url)
            parsed_depts = await _parse_departments_from_normal_college(college, html_content)
            depts_to_save.extend(parsed_depts)
        except Exception as e:
            logger.error(f"[{college.name}] 일반 단과대학 학과 파싱 중 오류: {e}")

    elif college.college_type == "plus_special_grad" or college.college_type == "plus_general_grad":
        try:
            parsed_depts = await _create_dept_for_plus_grad_school(college)
            depts_to_save.extend(parsed_depts)
        except Exception as e:
            logger.error(f"[{college.name}] Plus 대학원 기반 학과 생성 중 오류: {e}")

    else:
        logger.warning(f"[{college.name}] 알 수 없는 college_type ('{college.college_type}')으로 학과 정보를 처리할 수 없습니다.")

    if not depts_to_save:
        logger.warning(f"[{college.name}] 최종적으로 DB에 저장할 학과 정보가 없습니다.")  #
        return

    final_unique_depts_for_db = []
    seen_codes_in_college_for_db = set()
    for dept_info_dict in depts_to_save:
        dept_info_dict["college_id"] = college.id
        if dept_info_dict["code"] not in seen_codes_in_college_for_db:
            final_unique_depts_for_db.append(dept_info_dict)
            seen_codes_in_college_for_db.add(dept_info_dict["code"])

    if len(final_unique_depts_for_db) != len(depts_to_save):
        logger.info(f"[{college.name}] DB 저장 전 중복 학과 코드 제거됨: {len(depts_to_save)} -> {len(final_unique_depts_for_db)}개")

    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for d_item_db in final_unique_depts_for_db:
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