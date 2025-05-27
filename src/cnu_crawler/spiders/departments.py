# cnu_crawler/spiders/departments.py
from loguru import logger
import json  # aiohttp는 이미 JSONDecodeError를 발생시킬 수 있지만, 명시적 import
from aiohttp import ClientError  # fetcher에서 발생할 수 있는 예외

from cnu_crawler.core.fetcher import fetch_json, fetch_text
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import College, Department, get_session


async def crawl_departments(college: College):
    logger.info(f"🏫 [{college.name}] 학부/학과 크롤링 시작")
    dept_list_data = []

    # ① JSON API 시도
    # FIXME: 실제 API 경로로 수정 필요. '/departmentList.do', '/getDeptList.json' 등 다양할 수 있음.
    api_url = f"{college.url.rstrip('/')}/department/list.json"  #

    try:
        logger.debug(f"JSON API 시도: {api_url}")
        data = await fetch_json(api_url)  #

        # FIXME: 실제 API 응답 구조에 따라 아래 키들을 수정해야 합니다.
        # 예: data가 리스트가 아니라 dict 안에 있다면 data = data.get('departments', [])
        if not isinstance(data, list):
            logger.warning(f"[{college.name}] JSON API 응답이 리스트가 아닙니다. Fallback 시도. 데이터: {str(data)[:200]}")
            raise ValueError("JSON API 응답 형식이 다릅니다.")  # Fallback 로직으로 넘어가기 위함

        for d_item in data:
            # FIXME: 'deptCd', 'deptNm', 'url' 키가 실제 API 응답과 다를 경우 수정 필요.
            code = d_item.get("deptCd")  #
            name = d_item.get("deptNm")  #
            url = d_item.get("url")  #

            if not all([code, name, url]):
                logger.warning(f"[{college.name}] JSON 항목에 필수 정보(code, name, url)가 누락: {d_item}")
                continue
            dept_list_data.append({"code": str(code), "name": str(name), "url": str(url)})
        logger.info(f"[{college.name}] JSON API를 통해 {len(dept_list_data)}개 학과 정보 추출 완료.")

    except (
    ClientError, json.JSONDecodeError, ValueError, TypeError, Exception) as e:  # TypeError 추가 (data가 None일 경우 등)
        logger.warning(f"[{college.name}] JSON API 호출/파싱 실패 ({api_url}): {e}. HTML Fallback 시도.")

        # ② 정적 HTML fallback
        try:
            # college.url이 실제 학과 목록이 있는 페이지인지 확인 필요
            html_page_url = college.url  # 이 URL이 학과 목록을 포함해야 함
            logger.debug(f"HTML Fallback 시도: {html_page_url}")
            html = await fetch_text(html_page_url)  #

            # FIXME: 아래 CSS 선택자들은 웹사이트 구조 변경 시 반드시 함께 수정되어야 합니다.
            # 선택자는 최대한 구체적이면서도 깨지기 쉽지 않게 작성하는 것이 중요합니다.
            # 예: 학과 링크 선택자: 'div.department_list > ul > li > a'
            # 예: 학과명 선택자: 'div.department_list > ul > li > a > span.name' (만약 이름이 span 안에 있다면)

            # 현재 로직은 링크와 이름을 별도로 가져오는데, 이는 불안정할 수 있습니다.
            # 하나의 반복 단위(예: 각 학과를 감싸는 div)를 먼저 찾고, 그 안에서 링크와 이름을 찾는 것이 더 안정적입니다.
            # department_container_selector = "div.dept_item_selector" # 예시
            # containers = some_new_html_select_containers(html, department_container_selector)
            # for container_html in containers:
            #    href = html_first(container_html, "a.dept_link_selector", attr="href")
            #    name = html_first(container_html, "span.dept_name_selector")

            # 현재 코드 기반 수정:
            # 학과 링크 선택자 (href에 'department' 또는 유사한 키워드가 포함된 <a> 태그)
            dept_link_selector = "a[href*='department']"  #
            # 학과 이름 선택자 (위와 동일한 <a> 태그 내부의 텍스트)
            dept_name_selector = "a[href*='department']"  #

            hrefs = html_select(html, dept_link_selector, attr="href")  #
            names = html_select(html, dept_name_selector)  #

            if not hrefs or not names:
                logger.warning(f"[{college.name}] HTML에서 학과 링크나 이름을 찾지 못했습니다. (선택자: '{dept_link_selector}')")
            elif len(hrefs) != len(names):
                logger.warning(f"[{college.name}] 추출된 학과 링크({len(hrefs)}개)와 이름({len(names)}개)의 수가 다릅니다. HTML 구조 확인 필요.")
            else:
                for nm, href_val in zip(names, hrefs):
                    # FIXME: URL로부터 학과 코드를 추출하는 방식. URL 구조 변경 시 수정 필요.
                    # href_val.split("/")[-2]는 URL이 /path/to/dept_code/ 형태일 때 유효.
                    #  더 안정적인 방법은 정규표현식이나 URL query parameter 사용일 수 있음.
                    try:
                        # URL 정규화 (상대 경로 -> 절대 경로)
                        if not href_val.startswith(('http://', 'https://')):
                            from urllib.parse import urljoin
                            href_val = urljoin(html_page_url, href_val)

                        # 코드 추출 로직 개선 (더 많은 케이스 고려)
                        code_match = re.search(r'/department/(\w+)/', href_val) or \
                                     re.search(r'deptCd=(\w+)', href_val) or \
                                     re.search(r'/(\w+)/?$', href_val.rstrip('/'))  # /abc 또는 /abc/

                        if code_match:
                            dept_code = code_match.group(1)
                        else:
                            # Fallback: 기존 방식 또는 고유 ID 생성 (예: 해시값)
                            dept_code = href_val.split("/")[-2] if len(href_val.split("/")) > 2 else href_val  #
                            logger.trace(f"[{college.name}] '{href_val}' 에서 코드 추출에 정규식 실패. 기본 분할 사용: {dept_code}")

                        dept_list_data.append({"code": dept_code, "name": nm.strip(), "url": href_val})
                    except Exception as ex_parse:
                        logger.error(f"[{college.name}] 학과 정보 파싱 중 오류 (이름: {nm}, 링크: {href_val}): {ex_parse}")
                logger.info(f"[{college.name}] HTML Fallback을 통해 {len(dept_list_data)}개 학과 정보 추출 완료.")

        except (ClientError, Exception) as e_html:
            logger.error(f"[{college.name}] HTML Fallback 처리 중 심각한 오류 발생: {e_html}")
            # 여기서 비어있는 dept_list_data로 DB 업데이트 로직이 실행될 수 있음 (의도된 동작인지 확인)

    if not dept_list_data:
        logger.warning(f"[{college.name}] 최종적으로 학과 정보를 가져오지 못했습니다.")
        return

    with get_session() as sess:
        for d in dept_list_data:
            # college_id는 현재 college 객체의 id를 사용
            obj = (sess.query(Department)
                   .filter_by(college_id=college.id, code=d["code"]).one_or_none())  #
            if obj:
                obj.name, obj.url = d["name"], d["url"]  #
            else:
                obj = Department(college_id=college.id, **d)  #
                sess.add(obj)  #
        try:
            sess.commit()  #
            logger.success(f"[{college.name}] 총 {len(dept_list_data)}개 학과 정보 DB 업데이트 완료.")
        except Exception as e_db:
            logger.opt(exception=True).error(f"[{college.name}] 학과 정보 DB 저장 중 오류: {e_db}")
            sess.rollback()