# src/cnu_crawler/spiders/departments.py
import re  # 정규 표현식 모듈 임포트
import json
from urllib.parse import urljoin  # 상대 URL을 절대 URL로 변환하기 위함
from loguru import logger
from aiohttp import ClientError  # aiohttp 관련 예외 처리를 위해

from cnu_crawler.core.fetcher import fetch_json, fetch_text
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import College, Department, get_session
from cnu_crawler.utils import clean_text

# 학과 이름에 포함될 가능성이 있는 키워드 (검증용)
DEPT_KEYWORDS = ["학과", "학부", "전공", "department", "school of", "major", "division"]
# 학과 이름에서 제외할 키워드 (예: 대학 자체 링크 방지)
EXCLUDE_KEYWORDS_IN_NAME = ["대학안내", "입학안내", "대학생활", "커뮤니티", "오시는길", "사이트맵"]


async def crawl_departments(college: College):
    logger.info(f"🏫 [{college.name}] 학부/학과 크롤링 시작")
    dept_list_data_final = []  # 최종적으로 확정된 학과 정보

    # 1. JSON API 시도 (기존 로직 유지)
    api_url = f"{college.url.rstrip('/')}/department/list.json"
    json_api_succeeded = False
    try:
        logger.debug(f"JSON API 시도: {api_url}")
        data = await fetch_json(api_url)

        actual_data_list = []
        if isinstance(data, list):
            actual_data_list = data
        elif isinstance(data, dict) and "departments" in data and isinstance(data["departments"], list):  # 예시 키
            actual_data_list = data["departments"]
        # 다른 가능한 JSON 구조에 대한 처리 추가 가능
        # elif ...

        if not actual_data_list and isinstance(data, dict):  # 만약 다른 키에 데이터가 있을 경우
            logger.warning(f"[{college.name}] JSON API 응답이 직접적인 리스트는 아니지만 dict 형태임. 다른 키 확인 시도. 데이터: {str(data)[:200]}")
            # 여기서 data dict를 탐색하여 리스트를 찾아볼 수 있습니다. (예시로 남겨둠)

        if not actual_data_list and not isinstance(data, list):  # 최종적으로 리스트를 못 찾으면
            logger.warning(f"[{college.name}] JSON API 응답이 예상한 리스트 형태가 아닙니다 (URL: {api_url}). 데이터: {str(data)[:200]}")
            raise ValueError("JSON API 응답 형식이 리스트가 아님")

        temp_json_depts = []
        for d_item in actual_data_list:
            code = d_item.get("deptCd")
            name = d_item.get("deptNm")
            url = d_item.get("url")

            if not all([code, name, url]):
                logger.warning(f"[{college.name}] JSON 항목에 필수 정보(code, name, url)가 누락되었습니다: {d_item}")
                continue

            full_url = urljoin(college.url, url)
            temp_json_depts.append({"code": str(code), "name": clean_text(str(name)), "url": full_url})

        if temp_json_depts:
            logger.info(f"[{college.name}] JSON API를 통해 {len(temp_json_depts)}개 학과 정보 추출 완료.")
            dept_list_data_final.extend(temp_json_depts)  # JSON 결과를 최종 목록에 추가
            json_api_succeeded = True  # JSON API 성공 플래그

    except (ClientError, json.JSONDecodeError, ValueError, TypeError, Exception) as e:
        logger.warning(
            f"[{college.name}] JSON API 호출/파싱 실패 (URL: {api_url}): {type(e).__name__} - {e}. HTML Fallback을 시도합니다.")
        # JSON API 실패 시 dept_list_data_final은 비어있음

    # 2. HTML Fallback 시도 (JSON API 실패 시 또는 JSON API 결과가 없었을 경우)
    if not json_api_succeeded:  # JSON API가 실패했거나, 성공했어도 결과가 없었을 수 있음 (여기선 실패 시에만으로 한정)
        try:
            html_page_url_to_crawl = college.url
            logger.debug(f"HTML Fallback 시도: {html_page_url_to_crawl}")
            html_content = await fetch_text(html_page_url_to_crawl)

            # --- 다양한 선택자 목록 ---
            # 일반적인 네비게이션, 리스트, 콘텐츠 영역 등을 타겟으로 하는 선택자들
            # 우선순위가 높은 (더 구체적이거나 가능성 높은) 선택자를 앞에 배치
            selectors_to_try = [
                "a[href*='department']",  # 기본 선택자 (로그에서 일부 대학 실패)
                "a[href*='dept']",  # 'dept' 포함 링크
                "a[href*='major']",  # 'major' 포함 링크
                "a[href*='학부']",  # '학부' 포함 링크 (URL에 한글이 있는 경우)
                "a[href*='학과']",  # '학과' 포함 링크 (URL에 한글이 있는 경우)
                # 메뉴/네비게이션 구조에 대한 일반적인 선택자
                "nav a",  # <nav> 태그 안의 모든 링크
                "div[class*='nav'] a",  # class에 'nav'를 포함하는 div 안의 링크
                "div[id*='nav'] a",  # id에 'nav'를 포함하는 div 안의 링크
                "ul[class*='menu'] li a",  # class에 'menu'를 포함하는 ul 안의 li 안의 링크
                "ul[id*='menu'] li a",  # id에 'menu'를 포함하는 ul 안의 li 안의 링크
                "div.menu_wrap ul li a",  # 구체적인 메뉴 구조 예시
                "div.snb_wrap ul li a",  # 사이드 네비게이션 바 구조 예시
                # 학과 목록을 직접 담고 있을 가능성이 있는 구조
                "div[class*='dept_list'] a",  # class에 'dept_list' 포함하는 div 안의 링크
                "ul[class*='dept_list'] li a",  # class에 'dept_list' 포함하는 ul 안의 링크
                # 좀 더 일반적인 리스트 아이템 내의 링크
                "ul li a",
                # 콘텐츠 영역 내의 링크 중 특정 패턴 (매우 일반적이므로 주의)
                # "article a", "div.content a"
            ]

            # 휴리스틱: 한 선택자로 찾은 학과 수가 이 범위를 벗어나면 부적절하다고 판단 (조정 가능)
            MIN_EXPECTED_DEPTS = 2
            MAX_EXPECTED_DEPTS = 30  # 매우 큰 단과대학도 고려

            temp_html_depts_candidates = {}  # 선택자별 후보군 저장 (중복 방지용 URL 키)

            for selector_idx, current_selector in enumerate(selectors_to_try):
                logger.debug(
                    f"[{college.name}] HTML 선택자 시도 ({selector_idx + 1}/{len(selectors_to_try)}): '{current_selector}'")
                hrefs = html_select(html_content, current_selector, attr="href")
                names = html_select(html_content, current_selector)  # 링크의 텍스트

                if hrefs and names and len(hrefs) == len(names):
                    logger.trace(f"[{college.name}] 선택자 '{current_selector}'로 {len(hrefs)}개 링크/이름 쌍 발견.")

                    # 현재 선택자로 찾은 유효한 학과 후보
                    current_selector_valid_depts = []

                    for i in range(len(hrefs)):
                        nm_cleaned = clean_text(names[i])
                        href_val = hrefs[i]

                        if not nm_cleaned or not href_val:
                            continue

                        # 1. 이름 검증 (너무 짧거나, 일반적이지 않거나, 제외 키워드 포함)
                        if len(nm_cleaned) < 2 or len(nm_cleaned) > 50:  # 매우 짧거나 긴 이름 제외
                            # logger.trace(f"[{college.name}] 이름 길이 부적절 ({nm_cleaned}). 건너뜁니다.")
                            continue
                        if any(ex_kw in nm_cleaned for ex_kw in EXCLUDE_KEYWORDS_IN_NAME):
                            # logger.trace(f"[{college.name}] 이름에 제외 키워드 포함 ({nm_cleaned}). 건너뜁니다.")
                            continue
                        if not any(kw in nm_cleaned.lower() for kw in DEPT_KEYWORDS):
                            # 이름에 학과 관련 키워드가 전혀 없으면 의심 (하지만 모든 경우에 맞지는 않음)
                            # logger.trace(f"[{college.name}] 이름에 학과 키워드 부재 ({nm_cleaned}). 일단 포함하나 주의.")
                            pass  # 일단은 포함시키고 아래 URL 등으로 추가 판단

                        # 2. URL 검증
                        full_url = urljoin(html_page_url_to_crawl, href_val)
                        if not full_url.startswith(college.url.rstrip('/')):  # 현재 대학 사이트 외부 링크 제외
                            if not full_url.startswith("http"):  # 상대경로였다면 현재 도메인으로 처리된 것임
                                pass
                            elif full_url.split('/')[2] != college.url.split('/')[2]:  # 다른 도메인이면 제외
                                # logger.trace(f"[{college.name}] 외부 도메인 링크 ({full_url}). 건너뜁니다.")
                                continue

                        # 학과 코드 추출 로직 (이전 답변의 로직 활용)
                        code_match = re.search(r'/department[s]?/([\w-]+)', full_url, re.I) or \
                                     re.search(r'dept[C|c]d(?:=|/)(\w+)', full_url, re.I) or \
                                     re.search(r'major(?:=|/)(\w+)', full_url, re.I) or \
                                     re.search(r'/(\w{3,})/?$', full_url.rstrip('/').split('?')[0])  # 마지막 경로 (3글자 이상)

                        dept_code_extracted = ""
                        if code_match:
                            dept_code_extracted = next((g for g in code_match.groups() if g is not None), None)

                        if not dept_code_extracted:
                            path_parts = [part for part in full_url.split('?')[0].split('/') if part]
                            if path_parts and len(path_parts[-1]) > 2 and not path_parts[-1].endswith(
                                    (".do", ".jsp", ".html", ".htm")):
                                dept_code_extracted = path_parts[-1]
                            else:  # 최후의 수단 (이름 기반 - 고유성 낮음)
                                dept_code_extracted = re.sub(r'[^a-z0-9]', '', nm_cleaned.lower().replace(" ", ""))[:15]

                        dept_code_final = clean_text(dept_code_extracted)[:50] if dept_code_extracted else ""

                        if not dept_code_final:
                            # logger.warning(f"[{college.name}] 학과 코드 생성 최종 실패 (이름: {nm_cleaned}, URL: {full_url}).")
                            continue

                        # 후보군에 추가 (URL을 키로 사용하여 중복 방지)
                        if full_url not in temp_html_depts_candidates:
                            candidate_dept_info = {"code": dept_code_final, "name": nm_cleaned, "url": full_url,
                                                   "selector": current_selector}
                            temp_html_depts_candidates[full_url] = candidate_dept_info
                            current_selector_valid_depts.append(candidate_dept_info)

                    # 현재 선택자로 찾은 학과 수가 적절한 범위 내에 있는지 확인
                    if MIN_EXPECTED_DEPTS <= len(current_selector_valid_depts) <= MAX_EXPECTED_DEPTS:
                        logger.info(
                            f"[{college.name}] 선택자 '{current_selector}'로 {len(current_selector_valid_depts)}개의 유효한 학과 정보 후보를 찾았습니다. 이 결과를 사용합니다.")
                        dept_list_data_final.extend(current_selector_valid_depts)  # 첫 성공 결과를 최종 목록에 추가
                        break  # 성공적인 선택자를 찾았으므로 더 이상 다른 선택자 시도 안 함
                    elif current_selector_valid_depts:  # 범위는 벗어났지만 일단 찾긴 찾은 경우
                        logger.debug(
                            f"[{college.name}] 선택자 '{current_selector}'로 {len(current_selector_valid_depts)}개 후보 발견 (기대 범위: {MIN_EXPECTED_DEPTS}~{MAX_EXPECTED_DEPTS}). 다음 선택자 계속 시도.")
                        # 이 후보들을 임시로 저장해두고, 다른 선택자가 모두 실패하면 사용할 수도 있음

            # 모든 선택자를 시도한 후에도 dept_list_data_final이 비어있고,
            # temp_html_depts_candidates에 뭔가 있다면, 그 중 가장 많은 것을 선택 (최후의 수단)
            if not dept_list_data_final and temp_html_depts_candidates:
                logger.warning(f"[{college.name}] 모든 주요 선택자에서 기대 범위 내의 결과를 찾지 못했습니다. 수집된 모든 후보 중 가장 가능성 있는 결과를 선택합니다.")
                # 가장 많은 후보를 생성한 선택자의 결과 또는 다른 휴리스틱 적용 가능
                # 여기서는 간단히 모든 후보를 다 넣어봄 (중복은 URL 키로 제거됨)
                dept_list_data_final.extend(list(temp_html_depts_candidates.values()))
                if dept_list_data_final:
                    logger.info(f"[{college.name}] 최후의 수단으로 {len(dept_list_data_final)}개 학과 정보 후보를 최종 목록에 포함.")

            if not dept_list_data_final:  # HTML Fallback 최종 실패
                logger.warning(f"[{college.name}] HTML에서 학과 링크나 이름을 최종적으로 찾지 못했습니다.")
                logger.debug(f"[{college.name}] 마지막으로 시도된 HTML 내용 (처음 1000자): {html_content[:1000]}")

        except (ClientError, Exception) as e_html:
            logger.error(f"[{college.name}] HTML Fallback 처리 중 심각한 오류 발생: {type(e_html).__name__} - {e_html}")

    # --- 최종 결과 처리 및 DB 저장 ---
    if not dept_list_data_final:
        logger.warning(f"[{college.name}] 최종적으로 학과 정보를 가져오지 못했습니다.")
        return

    # DB 저장 전 중복 제거 (동일 code 학과가 여러 선택자에서 잡혔을 수 있음)
    final_unique_depts_to_save = []
    seen_codes = set()
    for dept_info in dept_list_data_final:
        if dept_info["code"] not in seen_codes:
            final_unique_depts_to_save.append(dept_info)
            seen_codes.add(dept_info["code"])

    if len(final_unique_depts_to_save) != len(dept_list_data_final):
        logger.info(
            f"[{college.name}] DB 저장 전 중복된 학과 코드 제거: {len(dept_list_data_final)} -> {len(final_unique_depts_to_save)}개")

    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for d_item_to_save in final_unique_depts_to_save:
            # 선택자 정보는 DB에 저장하지 않으므로 제거
            d_item_for_db = {k: v for k, v in d_item_to_save.items() if k != "selector"}

            obj = (sess.query(Department)
                   .filter_by(college_id=college.id, code=d_item_for_db["code"]).one_or_none())
            if obj:
                if obj.name != d_item_for_db["name"] or obj.url != d_item_for_db["url"]:
                    obj.name, obj.url = d_item_for_db["name"], d_item_for_db["url"]
                    updated_count += 1
            else:
                obj = Department(college_id=college.id, **d_item_for_db)
                sess.add(obj)
                added_count += 1

        if added_count > 0 or updated_count > 0:
            try:
                sess.commit()
                logger.success(
                    f"[{college.name}] 학과 정보 DB 업데이트 완료: {added_count}개 추가, {updated_count}개 수정 (고유 학과 수: {len(final_unique_depts_to_save)}).")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{college.name}] 학과 정보 DB 저장 중 오류: {e_db}")
                sess.rollback()
        else:
            logger.info(f"[{college.name}] DB에 변경된 학과 정보가 없습니다 (처리된 고유 학과 수: {len(final_unique_depts_to_save)}).")