# src/cnu_crawler/spiders/departments.py
import re  # 정규 표현식 모듈 임포트
import json
from urllib.parse import urljoin  # 상대 URL을 절대 URL로 변환하기 위함
from loguru import logger
from aiohttp import ClientError  # aiohttp 관련 예외 처리를 위해

from cnu_crawler.core.fetcher import fetch_json, fetch_text
from cnu_crawler.core.parser import html_select  #
from cnu_crawler.storage import College, Department, get_session  #
from cnu_crawler.utils import clean_text  #


async def crawl_departments(college: College):
    logger.info(f"🏫 [{college.name}] 학부/학과 크롤링 시작")  #
    dept_list_data = []

    # 1. JSON API 시도
    # API URL은 대학별로 다를 수 있으므로, college 객체에 API URL 정보가 있다면 사용하는 것이 좋습니다.
    # 여기서는 기존 방식을 유지합니다.
    api_url = f"{college.url.rstrip('/')}/department/list.json"  #

    try:
        logger.debug(f"JSON API 시도: {api_url}")  #
        data = await fetch_json(api_url)  #

        if not isinstance(data, list):
            # 응답이 리스트가 아닐 경우, 특정 키 밑에 리스트가 있는지 확인 (예시)
            # if isinstance(data, dict) and "departments" in data and isinstance(data["departments"], list):
            #     data = data["departments"]
            # else:
            logger.warning(f"[{college.name}] JSON API 응답이 리스트 형태가 아닙니다 (URL: {api_url}). 데이터: {str(data)[:200]}")
            raise ValueError("JSON API 응답 형식이 리스트가 아님")

        for d_item in data:
            # API 응답의 키 값들이 다를 수 있으므로 주의
            code = d_item.get("deptCd")  #
            name = d_item.get("deptNm")  #
            url = d_item.get("url")  #

            if not all([code, name, url]):
                logger.warning(f"[{college.name}] JSON 항목에 필수 정보(code, name, url)가 누락되었습니다: {d_item}")
                continue

            # URL이 상대 경로일 경우 절대 경로로 변환
            full_url = urljoin(college.url, url)  # API에서 받은 URL이 상대경로일 수 있으므로 college.url 기준으로 변환

            dept_list_data.append({"code": str(code), "name": clean_text(str(name)), "url": full_url})
        logger.info(f"[{college.name}] JSON API를 통해 {len(dept_list_data)}개 학과 정보 추출 완료.")

    except (ClientError, json.JSONDecodeError, ValueError, TypeError, Exception) as e:  # 더 많은 예외 유형 포함
        logger.warning(f"[{college.name}] JSON API 호출/파싱 실패 (URL: {api_url}): {e}. HTML Fallback을 시도합니다.")  #

        # 2. HTML Fallback 시도
        try:
            # 학과 목록이 있는 실제 페이지 URL. college.url 자체가 아닐 수 있음.
            # 예를 들어, 대학 메인 페이지 -> "학부/학과 안내" 메뉴 클릭 후의 페이지일 수 있음.
            # 이 URL을 정확히 파악하는 것이 중요.
            html_page_url_to_crawl = college.url
            logger.debug(f"HTML Fallback 시도: {html_page_url_to_crawl}")  #
            html_content = await fetch_text(html_page_url_to_crawl)

            # --- 선택자 전략 ---
            # 각 대학 웹사이트는 HTML 구조가 다를 수 있으므로, 여러 선택자를 시도하거나
            # 대학별로 다른 선택자 로직을 적용해야 할 수 있습니다.

            # 기본 선택자 (로그에서 실패한 선택자)
            primary_selector = "a[href*='department']"  #

            # 추가적으로 시도해볼 수 있는 선택자 목록 (실제 웹사이트 구조에 맞게 수정 필요)
            # 예시: 경상대학(cem.cnu.ac.kr)의 경우, 메뉴 구조를 분석하여
            #       <nav id="gnb"> 또는 <div class="department_menu_container"> 내부의 링크들을 찾아야 할 수 있습니다.
            #       정확한 선택자는 해당 웹사이트의 HTML을 직접 봐야 알 수 있습니다.
            alternative_selectors = [
                # "div.department_list_class ul li a", # 예시1: 특정 클래스 내의 목록 아이템 링크
                # "nav#main_navigation a[href*='/dept/']", # 예시2: 특정 ID를 가진 네비게이션 내 링크
                # 만약 경상대학의 학과 링크가 특별한 패턴(예: <a href="/sub/page/dept_code.html">)을 가진다면,
                # "a[href^='/sub/page/']" 와 같이 시작 부분을 지정할 수도 있습니다.
            ]

            found_depts = False
            selectors_to_try = [primary_selector] + alternative_selectors

            for selector_idx, current_selector in enumerate(selectors_to_try):
                if found_depts:  # 이미 학과를 찾았다면 루프 종료
                    break

                logger.debug(f"[{college.name}] HTML 선택자 시도 ({selector_idx + 1}): '{current_selector}'")
                hrefs = html_select(html_content, current_selector, attr="href")
                names = html_select(html_content, current_selector)  # 링크의 텍스트를 이름으로 사용

                if hrefs and names and len(hrefs) == len(names):
                    logger.info(f"[{college.name}] 선택자 '{current_selector}'로 {len(hrefs)}개의 잠재적 학과 링크 발견.")
                    for nm, href_val in zip(names, hrefs):
                        nm_cleaned = clean_text(nm)
                        if not nm_cleaned:  # 이름이 비어있으면 건너뜀
                            logger.warning(f"[{college.name}] 빈 학과 이름 발견 (링크: {href_val}). 건너뜁니다.")
                            continue

                        # URL 정규화 (상대 경로 -> 절대 경로)
                        full_url = urljoin(html_page_url_to_crawl, href_val)

                        # 학과 코드 추출 로직
                        # URL 경로에서 특정 부분을 학과 코드로 사용하거나, 더 복잡한 규칙 적용 가능
                        # 예: /dept_code/ 또는 ?dept_id=dept_code 등
                        code_match = re.search(r'/department[s]?/([\w-]+)', full_url, re.I) or \
                                     re.search(r'dept[C|c]d=(\w+)', full_url, re.I) or \
                                     re.search(r'/(\w+)/?$', full_url.rstrip('/'))  # URL 마지막 경로 요소

                        dept_code = ""
                        if code_match:
                            # 여러 그룹 중 첫 번째 유효한 그룹 사용
                            dept_code = next((g for g in code_match.groups() if g is not None), None)

                        if not dept_code:  # 정규식으로 못 찾으면 URL 일부 또는 이름 기반으로 생성 (최후의 수단)
                            path_parts = [part for part in full_url.split('/') if part]
                            if path_parts:
                                dept_code = path_parts[-1].split('.')[0].split('?')[0]  # 예: intro.do -> intro
                            else:
                                dept_code = re.sub(r'\s+', '_', nm_cleaned.lower())  # 이름 기반 (고유성 보장 안될 수 있음)
                            logger.trace(f"[{college.name}] URL '{full_url}' 에서 정규식으로 코드 추출 실패. 대체 코드: '{dept_code}'")

                        dept_code = clean_text(dept_code)[:50]  # 코드 길이 제한 및 정리

                        if not dept_code:  # 코드 생성 최종 실패시
                            logger.warning(f"[{college.name}] 학과 코드 생성 실패 (이름: {nm_cleaned}, URL: {full_url}). 건너뜁니다.")
                            continue

                        dept_list_data.append({"code": dept_code, "name": nm_cleaned, "url": full_url})

                    if dept_list_data:  # 현재 선택자로 데이터를 성공적으로 추가했다면
                        found_depts = True  # 성공 플래그 설정
                        # 이전에 실패한 선택자로 인해 dept_list_data에 중복이 있을 수 있으므로,
                        # 실제로는 이 단계에서 dept_list_data를 초기화하고 다시 채우는 것이 더 안전할 수 있습니다.
                        # 또는 set을 사용하여 중복을 제거하는 로직 추가.
                        # 여기서는 간단하게 첫 성공 시 중단하는 것으로 가정.
                        break  # 성공했으므로 다른 선택자 시도 중단
                elif hrefs or names:  # 링크와 이름 개수가 안 맞는 경우
                    logger.warning(
                        f"[{college.name}] 선택자 '{current_selector}'로 찾은 링크({len(hrefs)})와 이름({len(names)}) 개수가 불일치합니다.")

            if not found_depts:
                logger.warning(
                    f"[{college.name}] HTML에서 학과 링크나 이름을 최종적으로 찾지 못했습니다. (마지막 시도 선택자: '{current_selector}')")  #
                # 디버깅을 위해 현재 HTML 내용 일부를 로깅
                logger.debug(f"[{college.name}] 현재 HTML 내용 (처음 1000자): {html_content[:1000]}")


        except (ClientError, Exception) as e_html:
            logger.error(f"[{college.name}] HTML Fallback 처리 중 심각한 오류 발생: {e_html}")
            # HTML 가져오기 자체에 실패한 경우이므로, dept_list_data는 비어있을 가능성이 높음

    if not dept_list_data:
        logger.warning(f"[{college.name}] 최종적으로 학과 정보를 가져오지 못했습니다.")  #
        return

    # DB 저장 로직
    with get_session() as sess:
        added_count = 0
        updated_count = 0
        for d_item_to_save in dept_list_data:
            obj = (sess.query(Department)
                   .filter_by(college_id=college.id, code=d_item_to_save["code"]).one_or_none())
            if obj:
                if obj.name != d_item_to_save["name"] or obj.url != d_item_to_save["url"]:
                    obj.name, obj.url = d_item_to_save["name"], d_item_to_save["url"]
                    updated_count += 1
            else:
                obj = Department(college_id=college.id, **d_item_to_save)
                sess.add(obj)
                added_count += 1

        if added_count > 0 or updated_count > 0:
            try:
                sess.commit()
                logger.success(f"[{college.name}] 학과 정보 DB 업데이트 완료: {added_count}개 추가, {updated_count}개 수정.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{college.name}] 학과 정보 DB 저장 중 오류: {e_db}")
                sess.rollback()
        else:
            logger.info(f"[{college.name}] DB에 변경된 학과 정보가 없습니다.")