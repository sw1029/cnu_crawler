# src/cnu_crawler/spiders/notices.py
import asyncio
import json
import re  # HTML에서 ID 추출 시 필요할 수 있음
from datetime import datetime
from typing import Dict, List
from urllib.parse import urljoin

from loguru import logger
from aiohttp import ClientError  # aiohttp 관련 예외 처리

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


def get_notice_list_url(dept: Department, board_key: str, page: int) -> str:
    department_base_url = dept.url.rstrip("/")

    # --- !! 중요 !! ---
    # 각 학과별 실제 공지사항 URL 구조에 맞게 이 부분을 상세히 수정해야 합니다.
    # dept.name 또는 dept.code를 사용하여 분기하는 예시입니다.
    # 실제 학과 코드나 이름, URL 패턴을 확인하여 적용하세요.

    # 예시: 공과대학 대학원 ('eng'는 College 코드, 'archi'는 Department 코드일 수 있음)
    # 로그에서 'eng.cnu.ac.kr/eng/department/aerospace.do' 와 같은 URL이 dept.url로 사용됨
    if college_code_from_url(dept.url) == "eng" and board_key == "grad":
        # FIXME: 공과대학 대학원의 실제 공지사항 목록 URL 템플릿으로 수정
        # 예: "https://eng.cnu.ac.kr/eng/notice/grad.do?page={}"
        # 아래는 기존 방식을 따르되, 문제가 있다면 이 부분을 수정해야 함을 명시
        pass  # 특별한 규칙이 없다면 아래 기본 규칙으로
    elif college_code_from_url(dept.url) == "art" and board_key == "undergrad":
        # FIXME: 예술대학 학부의 실제 공지사항 목록 URL 템플릿으로 수정
        pass

    # 기본 URL 생성 규칙
    board_path_segment = BOARD_CODES.get(board_key)
    if not board_path_segment:
        logger.error(f"[{dept.name}] 유효하지 않은 board_key: {board_key}에 대한 BOARD_CODE 없음")
        return f"invalid_board_key_for_{dept.name}_{board_key}"

    # department_base_url (예: https://eng.cnu.ac.kr/eng/department/aerospace.do)
    # board_path_segment (예: board?code=grad_notice)
    # 결합 결과 예시: https://eng.cnu.ac.kr/eng/department/aerospace.do/board?code=grad_notice&page=1
    # 이 URL이 404를 반환한다면, 이 결합 방식 또는 BOARD_CODES 또는 department_base_url 자체가 잘못된 것임.
    # 많은 경우 .do 와 같은 파일명 뒤에 /를 붙이고 경로를 추가하면 404가 발생합니다.
    # 실제로는 department_base_url에서 파일명을 제거하고 board_path_segment를 붙이거나,
    # 완전히 다른 URL 구조를 사용해야 할 수 있습니다.

    # 임시 수정: dept.url이 .do 등으로 끝나면, 그 앞부분까지만 사용 시도
    if department_base_url.endswith(".do") or department_base_url.endswith(".jsp"):
        # department_base_url = department_base_url.rsplit('/', 1)[0] # 예: .../aerospace.do -> ...
        # 위와 같이 수정하면 의도치 않은 결과가 나올 수 있으므로,
        # 각 학과별 정확한 URL 규칙을 파악하는 것이 중요합니다.
        # 여기서는 원래 로직을 유지하고, get_notice_list_url 함수 자체의 개선이 필요함을 인지합니다.
        pass

    final_url_base = f"{department_base_url}/{board_path_segment}"
    if '?' in final_url_base:
        return f"{final_url_base}&page={page}"
    else:
        return f"{final_url_base}?page={page}"


def college_code_from_url(college_url: str) -> Optional[str]:
    # URL에서 대학 코드를 추출하는 간단한 예시 (예: https://eng.cnu.ac.kr -> eng)
    try:
        return college_url.split('/')[2].split('.')[0]
    except IndexError:
        return None


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

    consecutive_404_errors = 0  # 연속 404 오류 카운터 (첫 페이지만 가져오므로 큰 의미는 없을 수 있음)

    while page <= max_pages_to_crawl:  # 이 루프는 page=1일 때만 실행됨
        list_url = get_notice_list_url(dept, board_key, page)
        if "invalid_board_key" in list_url:
            logger.error(f"[{dept.name} ({board_key})] 유효한 공지사항 목록 URL을 생성할 수 없습니다. 수집 중단.")
            break

        logger.debug(f"페이지 {page} 공지사항 목록 요청: {list_url}")
        posts_data: List[Dict] = []
        stop_crawling_current_board = False
        current_page_fetch_successful = False

        try:  # JSON API 시도
            data = await fetch_json(list_url)
            current_page_posts = data.get("posts") if isinstance(data, dict) else data

            if not isinstance(current_page_posts, list):
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 응답의 'posts'가 리스트가 아님 ({list_url}). HTML Fallback 시도.")
                raise ValueError("JSON API 응답 형식이 리스트가 아님")

            logger.trace(f"[{dept.name} ({board_key})] JSON API 성공. {len(current_page_posts)}개 항목 수신.")
            for p_item in current_page_posts:
                # ... (JSON 파싱 및 증분 비교 로직은 이전과 동일) ...
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
            # === 수정된 오류 처리 부분 ===
            if isinstance(e_json, ClientError) and hasattr(e_json, 'status') and e_json.status == 404:  # type: ignore
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
                consecutive_404_errors += 1
            elif isinstance(e_json, asyncio.TimeoutError):
                logger.warning(f"[{dept.name} ({board_key})] JSON API 호출 시간 초과 ({list_url}). HTML Fallback 시도.")
            elif isinstance(e_json, ClientError):  # ClientConnectorError 등 status가 없는 ClientError
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출 중 연결 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")
                # 연결 오류 시에는 404가 아니므로 consecutive_404_errors를 증가시키지 않을 수 있음
                # 또는 특정 횟수 이상 발생 시 해당 학과 건너뛰기 등의 로직 추가 가능
            else:  # JSONDecodeError, ValueError, 기타 Exception
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 파싱 실패 또는 기타 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")
            # === 수정 끝 ===

            # HTML Fallback 시도
            try:
                html_content = await fetch_text(list_url)
                ids_html = html_select(html_content, "td.no")
                titles_html = html_select(html_content, "td.title a")
                links_html = html_select(html_content, "td.title a", "href")
                dates_html = html_select(html_content, "td.date")

                min_len = min(len(ids_html), len(titles_html), len(links_html), len(dates_html))
                if min_len == 0 and (len(ids_html) + len(titles_html) + len(links_html) + len(dates_html) > 0):
                    logger.warning(f"[{dept.name} ({board_key})] HTML에서 일부 정보만 추출됨. 파싱 건너뜀.")
                elif min_len > 0:
                    logger.trace(f"[{dept.name} ({board_key})] HTML Fallback 성공. {min_len}개 항목 후보 발견.")

                for i in range(min_len):
                    # ... (HTML 파싱 및 증분 비교 로직은 이전과 동일) ...
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
                # === 수정된 오류 처리 부분 ===
                if hasattr(e_html_fetch, 'status') and e_html_fetch.status == 404:  # type: ignore
                    logger.error(
                        f"[{dept.name} ({board_key})] HTML Fallback 처리 중 HTTP 오류 - 404 Not Found ({list_url}): {e_html_fetch.message}")
                    consecutive_404_errors += 1
                elif isinstance(e_html_fetch, ClientError):  # status 없는 ClientError
                    logger.error(
                        f"[{dept.name} ({board_key})] HTML Fallback 처리 중 연결 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
                else:  # 기타 예외
                    logger.error(
                        f"[{dept.name} ({board_key})] HTML Fallback 처리 중 알 수 없는 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
                # === 수정 끝 ===
            except Exception as e_html_parse:
                logger.error(f"[{dept.name} ({board_key})] HTML Fallback 파싱 중 알 수 없는 오류 ({list_url}): {e_html_parse}")

        # 루프 종료 조건 (첫 페이지만 가져오므로, 여기서 항상 break 됩니다)
        if stop_crawling_current_board:
            logger.info(f"[{dept.name} ({board_key})] 증분 수집 조건으로 인해 첫 페이지 수집 중단.")
        elif not current_page_fetch_successful and consecutive_404_errors >= 1:
            logger.warning(f"[{dept.name} ({board_key})] 첫 페이지부터 404 오류 발생 또는 연결 실패. 해당 게시판 수집 중단.")
        elif not current_page_fetch_successful:
            logger.info(f"[{dept.name} ({board_key})] 첫 페이지에서 데이터를 가져오지 못했습니다.")

        if posts_data:
            try:
                with get_session() as sess:
                    sess.bulk_insert_mappings(Notice, posts_data)
                    sess.commit()
                inserted_count += len(posts_data)
                logger.debug(f"[{dept.name} ({board_key})] 첫 페이지에서 {len(posts_data)}건 DB 저장 완료.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{dept.name} ({board_key})] 공지사항 DB 저장 중 오류: {e_db}")

        break  # 첫 페이지만 처리하므로 루프를 명시적으로 종료

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
        except Exception as e:  # crawl_board 내에서 발생하는 예외는 이미 상세히 로깅될 것이므로, 여기서는 간단히만
            logger.opt(exception=True).error(f"[{dept.name} ({board_key_val})] 게시판 크롤링 함수 실행 중 최종 예외 발생: {e}")