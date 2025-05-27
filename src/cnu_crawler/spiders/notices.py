# src/cnu_crawler/spiders/notices.py
import asyncio
import json
from datetime import datetime
from typing import Dict, List
from urllib.parse import urljoin

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


def get_notice_list_url(dept: Department, board_key: str, page: int) -> str:
    department_base_url = dept.url.rstrip("/")

    # --- !! 중요 !! ---
    # 아래는 학과별 URL 규칙을 적용하는 예시 부분입니다.
    # 실제 각 학과의 정확한 공지사항 URL 구조에 맞게 이 부분을 상세히 수정해야 합니다.
    # 예를 들어, dept.code (학과 고유 코드) 또는 dept.name을 사용하여 분기할 수 있습니다.

    # 예시: 공과대학 대학원, 예술대학 학부 등 특정 학과에 대한 규칙
    # if dept.name == "공과대학" and board_key == "grad":
    #     # FIXME: 공과대학 대학원 공지사항의 실제 URL 템플릿으로 수정
    #     # 예: actual_board_url = f"https://eng.cnu.ac.kr/eng/real/grad_notice_path.do?pageNo={page}"
    #     # return actual_board_url
    #     pass # 특별 규칙이 없다면 아래 기본 규칙으로
    # elif dept.name == "예술대학" and board_key == "undergrad":
    #     # FIXME: 예술대학 학부 공지사항의 실제 URL 템플릿으로 수정
    #     # 예: actual_board_url = f"https://art.cnu.ac.kr/art/real/undergrad_path.do?page_num={page}"
    #     # return actual_board_url
    #     pass

    # 기본 URL 생성 규칙
    board_path_segment = BOARD_CODES.get(board_key)
    if not board_path_segment:
        logger.error(f"[{dept.name}] 유효하지 않은 board_key: {board_key}에 대한 BOARD_CODE 없음")
        return f"invalid_board_key_for_{dept.name}_{board_key}"

    final_url_base = f"{department_base_url}/{board_path_segment}"
    if '?' in final_url_base:
        return f"{final_url_base}&page={page}"
    else:
        return f"{final_url_base}?page={page}"


async def crawl_board(dept: Department, board_key: str):
    page = 1  # 항상 첫 페이지만 대상으로 함
    inserted_count = 0
    # max_pages_to_crawl 변수를 1로 설정하여 첫 페이지만 크롤링하도록 합니다.
    max_pages_to_crawl = 1

    delay_per_page = REQUEST_DELAY_NOTICE_PAGE_SECONDS

    logger.info(f"📄 [{dept.name} ({board_key})] 공지사항 첫 페이지만 수집 시작")

    # 증분 수집을 위한 last_post_id_db 로직은 첫 페이지만 가져올 경우,
    # 기존 DB 내용과 비교하는 용도로는 계속 유효할 수 있습니다.
    # 만약 첫 페이지의 모든 글을 항상 새로 가져오고 싶다면 이 부분은 생략 가능합니다.
    with get_session() as sess:
        last_notice = (sess.query(Notice)
                       .filter_by(dept_id=dept.id, board=board_key)
                       .order_by(Notice.post_id.desc())
                       .first())
        last_post_id_db = last_notice.post_id if last_notice else "0"
    logger.debug(f"[{dept.name} ({board_key})] DB의 마지막 게시글 ID: {last_post_id_db} (첫 페이지만 수집 시 참고용)")

    consecutive_404_errors = 0

    # while 루프는 이제 최대 한 번만 실행됩니다 (max_pages_to_crawl = 1 이므로).
    # 또는 루프 후 바로 break 하는 방식으로도 구현 가능합니다.
    while page <= max_pages_to_crawl:
        # 첫 페이지만 가져오므로 페이지 간 delay는 필요 없어짐 (page > 1 조건이 항상 false)
        # if page > 1 and delay_per_page > 0:
        #     logger.trace(f"[{dept.name} ({board_key})] 다음 페이지 요청 전 {delay_per_page:.1f}초 대기...")
        #     await asyncio.sleep(delay_per_page)

        list_url = get_notice_list_url(dept, board_key, page)
        if "invalid_board_key" in list_url:
            logger.error(f"[{dept.name} ({board_key})] 유효한 공지사항 목록 URL을 생성할 수 없습니다. 수집 중단.")
            break  # URL 생성 실패 시 루프 종료

        logger.debug(f"페이지 {page} 공지사항 목록 요청: {list_url}")
        posts_data: List[Dict] = []
        stop_crawling_current_board = False  # 첫 페이지만 가져오므로, 증분 비교 결과에 따라 중단될 수 있음
        current_page_fetch_successful = False

        try:  # JSON API 시도
            data = await fetch_json(list_url)
            current_page_posts = data.get("posts") if isinstance(data, dict) else data

            if not isinstance(current_page_posts, list):
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 응답의 'posts'가 리스트가 아님 ({list_url}). 데이터: {str(data)[:200]}. HTML Fallback 시도.")
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

                # 첫 페이지만 가져오더라도, 이미 DB에 있는 글은 건너뛰기 위한 증분 비교
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
            if isinstance(e_json, ClientError) and e_json.status == 404:  # type: ignore
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
                consecutive_404_errors += 1
            elif isinstance(e_json, asyncio.TimeoutError):
                logger.warning(f"[{dept.name} ({board_key})] JSON API 호출 시간 초과 ({list_url}). HTML Fallback 시도.")
            else:
                logger.warning(
                    f"[{dept.name} ({board_key})] JSON API 호출/파싱 실패 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")

            try:  # HTML Fallback
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
                logger.error(
                    f"[{dept.name} ({board_key})] HTML Fallback 처리 중 HTTP 오류 ({list_url}): {e_html_fetch.status}, {e_html_fetch.message}")
                if e_html_fetch.status == 404:  # type: ignore
                    logger.warning(f"[{dept.name} ({board_key})] 페이지 {page} (URL: {list_url})가 존재하지 않음 (404).")
                    consecutive_404_errors += 1
            except Exception as e_html_parse:
                logger.error(f"[{dept.name} ({board_key})] HTML Fallback 파싱 중 알 수 없는 오류 ({list_url}): {e_html_parse}")

        # 루프 종료 조건 (첫 페이지만 가져오므로, 여기서 항상 break 됩니다)
        if stop_crawling_current_board:
            logger.info(f"[{dept.name} ({board_key})] 증분 수집 조건으로 인해 첫 페이지 수집 중단.")
        elif not current_page_fetch_successful and consecutive_404_errors >= 1:  # 첫 페이지가 404인 경우
            logger.warning(f"[{dept.name} ({board_key})] 첫 페이지부터 404 오류 발생. 해당 게시판 수집 중단.")
        elif not current_page_fetch_successful:  # 404는 아니지만 데이터 못 얻은 경우
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

        # 첫 페이지만 처리하므로 루프를 빠져나갑니다.
        break  # while page <= max_pages_to_crawl 루프를 여기서 종료시킵니다.
        # page += 1 # 이 줄은 더 이상 필요 없습니다.

    if inserted_count > 0:
        logger.success(f"📄 [{dept.name} ({board_key})] 첫 페이지 새 공지 총 {inserted_count}건 수집 완료.")
    else:  # inserted_count가 0인 다양한 경우 (증분으로 중단, 데이터 없음, 404 등)
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
            logger.opt(exception=True).error(f"[{dept.name} ({board_key_val})] 게시판 크롤링 함수 실행 중 예외 발생: {e}")