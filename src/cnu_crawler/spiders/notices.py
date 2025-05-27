# src/cnu_crawler/spiders/notices.py
import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlunparse  # urlparse, urlunparse 추가

from loguru import logger
from aiohttp import ClientError

from cnu_crawler.core.fetcher import fetch_json, fetch_text
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import Department, Notice, get_session
from cnu_crawler.utils import clean_text, parse_date_flexible
from cnu_crawler.config import (
    REQUEST_DELAY_NOTICE_PAGE_SECONDS,
    REQUEST_DELAY_DEPARTMENT_SECONDS
)

BOARD_CODES = {
    "undergrad": "board?code=undergrad_notice",
    "grad": "board?code=grad_notice"
}


def college_code_from_url(college_url: str) -> Optional[str]:
    try:
        hostname = college_url.split('/')[2]
        return hostname.split('.')[0]
    except IndexError:
        logger.warning(f"URL에서 대학 코드를 추출할 수 없습니다: {college_url}")
        return None


def get_notice_list_url(dept: Department, board_key: str, page: int) -> str:
    # dept.url에서 # 이후 부분을 제거하여 실제 base URL을 만듭니다.
    parsed_dept_url = urlparse(dept.url.rstrip("/"))
    # scheme, netloc, path만 사용하고 query, fragment는 제거
    # path가 비어있으면 '/'로 설정 (예: https://example.com#frag -> https://example.com/)
    path_for_dept_base = parsed_dept_url.path if parsed_dept_url.path else '/'
    department_base_url = urlunparse((parsed_dept_url.scheme, parsed_dept_url.netloc, path_for_dept_base, '', '', ''))
    department_base_url = department_base_url.rstrip('/')  # 다시 한번 우측 / 제거

    logger.trace(f"[{dept.name}] 원본 dept.url: {dept.url}, # 제거 후 base URL: {department_base_url}")

    # --- !! 중요 !! ---
    # 각 학과별 실제 공지사항 URL 구조에 맞게 이 부분을 상세히 수정해야 합니다.
    # current_college_code = college_code_from_url(dept.url) # dept.url 대신 department_base_url 사용 고려
    # if current_college_code == "nursing" and "menu" in dept.url: # 간호대학 URL 특성 반영 예시
    #     # FIXME: 간호대학의 실제 공지사항 목록 URL로 수정 (예: `#` 이전 URL + 실제 경로)
    #     # 예: "https://nursing.cnu.ac.kr/nursing/board/undergrad_notice.do"
    #     # department_base_url = "https://nursing.cnu.ac.kr" # 실제 도메인으로
    #     # board_path_segment = "실제_게시판_경로/list.do" # 또는 board?code=xxx
    #     pass
    # elif current_college_code == "cem" and "menu" in dept.url: # 경상대학 URL 특성 반영 예시
    #     # FIXME: 경상대학의 실제 공지사항 목록 URL로 수정
    #     pass

    # 기본 URL 생성 규칙
    board_path_segment = BOARD_CODES.get(board_key)
    if not board_path_segment:
        logger.error(f"[{dept.name}] 유효하지 않은 board_key: {board_key}에 대한 BOARD_CODE 없음")
        return f"invalid_board_key_for_{dept.name}_{board_key}"

    # department_base_url이 파일명(.do 등)으로 끝나는 경우, 그 앞에 board_path_segment를 붙이면 안됨.
    # 이 부분은 각 대학 사이트 구조에 따라 매우 달라질 수 있으므로,
    # 가장 확실한 것은 각 Department 객체에 정확한 게시판 URL 템플릿을 갖도록 하는 것입니다.
    # 임시방편으로, department_base_url이 특정 확장자로 끝나면 그 앞까지만 사용하도록 시도.

    temp_base = department_base_url
    # `.do`나 `.jsp` 등으로 끝나는 경우, 해당 파일명을 포함한 경로가 아닌,
    # 상위 디렉토리에 board_path_segment를 적용해야 할 가능성이 높습니다.
    # 예: https://example.com/path/to/page.do -> /board?code=... 를 붙이면 404
    #     https://example.com/path/to/board?code=... 가 되어야 할 수 있음
    # 이는 대학별로 규칙을 만들어야 정확합니다.
    # 아래는 매우 일반적인 가정이므로, 실제로는 더 정교한 로직 또는 학과별 URL 템플릿이 필요합니다.
    if any(temp_base.lower().endswith(ext) for ext in ['.do', '.jsp', '.php', '.html', '.htm']):
        # 마지막 '/'를 찾아 그 이전까지를 base로 삼으려는 시도.
        # 하지만 dept.url 자체가 게시판 목록이 아닌 학과 메인페이지일 가능성이 높으므로,
        # 이 방식이 항상 옳지는 않습니다.
        # logger.debug(f"URL이 파일명으로 끝나는 것으로 간주: {temp_base}. 상위 경로 사용 시도.")
        # temp_base = temp_base.rsplit('/', 1)[0]
        # 위와 같이 수정하면 department_base_url이 이미 /로 끝나면 문제가 될 수 있음
        # 가장 안전한 것은 department_base_url에 BOARD_CODES[board_key]를 그대로 붙이는 것입니다.
        # (단, BOARD_CODES의 값이 절대경로(/로 시작)가 아니거나, 완전한 URL이 아니어야 함)
        # 현재 BOARD_CODES는 상대경로 형태이므로, 바로 붙여봅니다.
        pass  # 현재 로직에서는 department_base_url에 바로 board_path_segment를 붙입니다.

    final_url_base = f"{temp_base}/{board_path_segment}"
    if '?' in final_url_base:  # board_path_segment에 이미 '?'가 있는 경우
        return f"{final_url_base}&page={page}"
    else:
        return f"{final_url_base}?page={page}"


# 이하 crawl_board, crawl_department_notices 함수는 이전 답변의 내용과 동일하게 유지합니다.
# (첫 페이지만 가져오고, 에러 처리 로직이 개선된 버전)

async def crawl_board(dept: Department, board_key: str):
    page = 1
    inserted_count = 0
    max_pages_to_crawl = 1  # 첫 페이지만 가져오도록 설정

    delay_per_page = REQUEST_DELAY_NOTICE_PAGE_SECONDS

    logger.info(f"📄 [{dept.name} ({board_key})] 공지사항 첫 페이지만 수집 시작")

    with get_session() as sess:
        last_notice = (sess.query(Notice)
                       .filter_by(dept_id=dept.id, board=board_key)
                       .order_by(Notice.post_id.desc())
                       .first())
        last_post_id_db = last_notice.post_id if last_notice else "0"
    logger.debug(f"[{dept.name} ({board_key})] DB의 마지막 게시글 ID: {last_post_id_db} (첫 페이지만 수집 시 참고용)")

    consecutive_404_errors = 0

    while page <= max_pages_to_crawl:
        list_url = get_notice_list_url(dept, board_key, page)
        if "invalid_board_key" in list_url:
            logger.error(f"[{dept.name} ({board_key})] 유효한 공지사항 목록 URL을 생성할 수 없습니다. 수집 중단.")
            break

        logger.debug(f"페이지 {page} 공지사항 목록 요청: {list_url}")
        posts_data: List[Dict] = []
        stop_crawling_current_board = False
        current_page_fetch_successful = False

        try:
            data = await fetch_json(list_url)
            current_page_posts = data.get("posts") if isinstance(data, dict) else data

            if not isinstance(current_page_posts, list):
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 응답의 'posts'가 리스트가 아님 ({list_url}). HTML Fallback 시도.")
                raise ValueError("JSON API 응답 형식이 리스트가 아님")

            logger.trace(f"[{dept.name} ({board_key})] JSON API 성공. {len(current_page_posts)}개 항목 수신.")
            for p_item in current_page_posts:
                post_id_str = str(p_item.get("id", ""))
                title = clean_text(str(p_item.get("title", "")))
                raw_url = p_item.get("url", "")
                date_str = p_item.get("date", "")

                if not all([post_id_str, title, raw_url, date_str]):
                    logger.warning(f"[{dept.name} ({board_key})] JSON 항목에 필수 정보 누락: {p_item}")
                    continue

                if post_id_str.isdigit() and last_post_id_db.isdigit():
                    if int(post_id_str) <= int(last_post_id_db):
                        stop_crawling_current_board = True;
                        break
                elif post_id_str <= last_post_id_db and post_id_str != "":
                    stop_crawling_current_board = True;
                    break

                parsed_date = parse_date_flexible(date_str)
                if not parsed_date:
                    logger.warning(
                        f"[{dept.name} ({board_key})] 날짜 파싱 실패 (ID: {post_id_str}, 날짜: '{date_str}'). 건너뜁니다.")
                    continue

                full_url = urljoin(list_url, raw_url)
                posts_data.append({
                    "dept_id": dept.id, "board": board_key, "post_id": post_id_str,
                    "title": title, "url": full_url, "posted_at": parsed_date
                })

            if posts_data: current_page_fetch_successful = True
            consecutive_404_errors = 0
            if stop_crawling_current_board: break

        except (ClientError, json.JSONDecodeError, ValueError, Exception) as e_json:
            if isinstance(e_json, ClientError) and hasattr(e_json, 'status') and e_json.status == 404:
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
                consecutive_404_errors += 1
            elif isinstance(e_json, asyncio.TimeoutError):
                logger.warning(f"[{dept.name} ({board_key})] JSON API 호출 시간 초과 ({list_url}). HTML Fallback 시도.")
            elif isinstance(e_json, ClientError):
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출 중 연결 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")
            elif isinstance(e_json, json.JSONDecodeError):  # JSONDecodeError를 명시적으로 처리 (로그 메시지 개선)
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 파싱 실패 ({list_url}): {e_json}. 응답이 JSON 형식이 아닙니다. HTML Fallback 시도.")
            else:
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 처리 중 기타 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")

            try:
                html_content = await fetch_text(list_url)
                # HTML Fallback 로직이 비어있거나, 해당 사이트의 HTML 구조에 맞는 파서가 필요합니다.
                # 아래는 일반적인 예시이며, 실제 사이트 구조에 맞춰 CSS 선택자를 수정해야 합니다.
                ids_html = html_select(html_content, "td.no")  # 예시 선택자
                titles_html = html_select(html_content, "td.title a")  # 예시 선택자
                links_html = html_select(html_content, "td.title a", "href")  # 예시 선택자
                dates_html = html_select(html_content, "td.date")  # 예시 선택자

                min_len = min(len(ids_html), len(titles_html), len(links_html), len(dates_html))
                if min_len == 0 and (len(ids_html) + len(titles_html) + len(links_html) + len(dates_html) > 0):
                    logger.warning(f"[{dept.name} ({board_key})] HTML에서 일부 정보만 추출됨. 파싱 건너뜀. URL: {list_url}")
                elif min_len > 0:
                    logger.trace(f"[{dept.name} ({board_key})] HTML Fallback으로 {min_len}개 항목 후보 발견. URL: {list_url}")

                for i in range(min_len):
                    post_id_str = clean_text(ids_html[i])
                    if not post_id_str.isdigit():
                        id_match_from_url = re.search(r'(?:idx|id|no|seq)=(\d+)', links_html[i], re.I)
                        if id_match_from_url:
                            post_id_str = id_match_from_url.group(1)
                        else:
                            logger.warning(
                                f"[{dept.name} ({board_key})] HTML 항목 ID가 숫자가 아니고 URL에서 추출 불가 ('{ids_html[i]}'). 건너뜁니다.")
                            continue

                    if post_id_str.isdigit() and last_post_id_db.isdigit():
                        if int(post_id_str) <= int(last_post_id_db):
                            stop_crawling_current_board = True;
                            break
                    elif post_id_str <= last_post_id_db and post_id_str != "":
                        stop_crawling_current_board = True;
                        break

                    title = clean_text(titles_html[i])
                    raw_url = links_html[i]
                    date_str = dates_html[i]

                    parsed_date = parse_date_flexible(date_str)
                    if not parsed_date:
                        logger.warning(
                            f"[{dept.name} ({board_key})] HTML 날짜 파싱 실패 (ID: {post_id_str}, 날짜: '{date_str}'). 건너뜁니다.")
                        continue

                    full_url = urljoin(list_url, raw_url)
                    posts_data.append({
                        "dept_id": dept.id, "board": board_key, "post_id": post_id_str,
                        "title": title, "url": full_url, "posted_at": parsed_date
                    })

                if posts_data: current_page_fetch_successful = True
                consecutive_404_errors = 0
                if stop_crawling_current_board: break

            except ClientError as e_html_fetch:
                if hasattr(e_html_fetch, 'status') and e_html_fetch.status == 404:
                    logger.error(
                        f"[{dept.name} ({board_key})] HTML Fallback URL 접근 실패 - 404 Not Found ({list_url}): {e_html_fetch.message}")
                    consecutive_404_errors += 1
                elif isinstance(e_html_fetch, ClientError):
                    logger.error(
                        f"[{dept.name} ({board_key})] HTML Fallback URL 접근 중 연결 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
            except Exception as e_html_parse:
                logger.error(f"[{dept.name} ({board_key})] HTML Fallback 파싱 중 알 수 없는 오류 ({list_url}): {e_html_parse}")

        if stop_crawling_current_board:
            logger.info(f"[{dept.name} ({board_key})] 증분 수집 조건으로 인해 첫 페이지 수집 중단.")
        elif not current_page_fetch_successful and consecutive_404_errors >= 1:  # 첫 페이지가 404이거나 연결 실패
            logger.warning(f"[{dept.name} ({board_key})] 첫 페이지부터 404 오류 또는 연결 실패. 해당 게시판 수집 중단. URL: {list_url}")
        elif not current_page_fetch_successful:  # 404는 아니지만 다른 이유로 데이터 못 얻음
            logger.info(f"[{dept.name} ({board_key})] 첫 페이지에서 데이터를 가져오지 못했습니다. URL: {list_url}")

        if posts_data:
            try:
                with get_session() as sess:
                    sess.bulk_insert_mappings(Notice, posts_data)
                    sess.commit()
                inserted_count += len(posts_data)
                logger.debug(f"[{dept.name} ({board_key})] 첫 페이지에서 {len(posts_data)}건 DB 저장 완료.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{dept.name} ({board_key})] 공지사항 DB 저장 중 오류: {e_db}")

        break

    if inserted_count > 0:
        logger.success(f"📄 [{dept.name} ({board_key})] 첫 페이지 새 공지 총 {inserted_count}건 수집 완료.")
    else:
        logger.info(f"📄 [{dept.name} ({board_key})] 첫 페이지에서 새로운 공지사항이 없거나 가져오지 못했습니다.")


async def crawl_department_notices(dept: Department):
    delay_before_dept_crawl = REQUEST_DELAY_DEPARTMENT_SECONDS
    if delay_before_dept_crawl > 0:
        logger.trace(f"'{dept.name}' 학과 공지사항 수집 시작 전 {delay_before_dept_crawl:.1f}초 대기...")
        await asyncio.sleep(delay_before_dept_crawl)

    for board_key_val in BOARD_CODES:
        try:
            await crawl_board(dept, board_key_val)
        except Exception as e:
            logger.opt(exception=True).error(f"[{dept.name} ({board_key_val})] 게시판 크롤링 함수 실행 중 최종 예외 발생: {e}")