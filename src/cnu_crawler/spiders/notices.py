# src/cnu_crawler/spiders/notices.py
import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Coroutine, Any, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

from loguru import logger
from aiohttp import ClientError

from cnu_crawler.core.fetcher import fetch_text, fetch_json
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import Department, Notice, get_session  #
from cnu_crawler.utils import clean_text, parse_date_flexible  #
from cnu_crawler.config import (  #
    REQUEST_DELAY_NOTICE_PAGE_SECONDS,
    REQUEST_DELAY_DEPARTMENT_SECONDS
)

# 게시판 유형 상수 정의
BOARD_TYPE_ACADEMIC = "academic"
BOARD_TYPE_UNDERGRAD = "undergrad"
BOARD_TYPE_GRAD = "grad"
BOARD_TYPE_GRAD_KEYWORD = "grad_keyword_found"


# BOARD_CODES는 이제 사용되지 않거나, get_notice_list_url에서 최후의 수단으로만 사용됩니다.
# 사용자의 요구사항 "업데이트한 url만 사용"에 따라, BOARD_CODES 기반의 URL 생성은 제거하는 것이 좋습니다.
# BOARD_CODES = {
#     BOARD_TYPE_UNDERGRAD: "board?code=undergrad_notice",
#     BOARD_TYPE_GRAD: "board?code=grad_notice"
# }

def college_code_from_url(college_url: str) -> Optional[str]:  # 이전 답변에서 추가됨
    try:
        hostname = college_url.split('/')[2]
        return hostname.split('.')[0]
    except IndexError:
        logger.warning(f"URL에서 대학 코드를 추출할 수 없습니다: {college_url}")
        return None


def get_notice_list_url(dept: Department, board_type: str, page: int) -> Optional[str]:
    """
    Department 객체에 저장된 URL 템플릿과 board_type을 사용하여 공지사항 목록 URL을 생성합니다.
    템플릿이 없으면 None을 반환하여 해당 게시판 수집을 건너뛰도록 합니다.
    """
    url_template: Optional[str] = None

    if board_type == BOARD_TYPE_ACADEMIC:
        url_template = dept.academic_notice_url_template
    elif board_type == BOARD_TYPE_UNDERGRAD:
        url_template = dept.undergrad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD:
        url_template = dept.grad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD_KEYWORD:
        url_template = dept.specific_grad_keyword_notice_url

    if not url_template:
        logger.trace(f"[{dept.name}] 게시판 유형 '{board_type}'에 대한 URL 템플릿이 DB에 설정되지 않았습니다. 수집을 건너뜁니다.")
        return None  # 템플릿이 없으면 URL 생성 불가 -> 이 게시판 유형은 수집 안 함

    try:
        # URL 템플릿에 페이지 번호 플레이스홀더 처리 (예: {page} 또는 {})
        if "{page}" in url_template:
            return url_template.replace("{page}", str(page))
        elif "{}" in url_template:  # 단순 format 플레이스홀더
            return url_template.format(page)
        else:
            # URL 템플릿에 페이지 플레이스홀더가 없는 경우, 페이지 파라미터를 추가하는 방식.
            # 이 방식은 URL 템플릿이 페이지 파라미터 없이도 유효한 목록 URL일 때를 가정합니다.
            # 또는 템플릿 자체가 이미 page=1을 포함하고 있을 수도 있습니다.
            # 여기서는 일반적인 page 파라미터를 추가합니다.
            parsed_template = urlparse(url_template)
            query_params = parse_qs(parsed_template.query)

            page_param_name = "page"  # 기본 페이지 파라미터 이름
            # 실제로는 다양한 페이지 파라미터 이름(pageNo, p, pg 등)을 고려해야 할 수 있음
            # 또는 URL 템플릿 자체에 이 정보가 포함되도록 하는 것이 더 좋음.

            query_params[page_param_name] = [str(page)]
            new_query = urlencode(query_params, doseq=True)
            # fragment는 목록 URL에 일반적으로 불필요하므로 제거
            return urlunparse((parsed_template.scheme, parsed_template.netloc, parsed_template.path,
                               parsed_template.params, new_query, ''))

    except Exception as e:
        logger.error(f"[{dept.name}] URL 템플릿 ('{url_template}') 처리 중 오류 (page={page}, board_type='{board_type}'): {e}")
        return None


async def _parse_notice_page_content(dept: Department, board_type: str, list_url: str, last_post_id_db: str) -> Tuple[
    List[Dict], bool]:
    # 이 함수의 내용은 이전 답변과 동일하게 유지 (JSON 우선 파싱, HTML Fallback, 증분 비교 등)
    # 단, Notice 저장 시 source_display_name 설정 로직은 board_type에 따라 유지
    posts_data: List[Dict] = []
    stop_crawling = False
    fetch_successful = False

    try:
        logger.trace(f"[{dept.name} ({board_type})] JSON API 시도: {list_url}")
        data = await fetch_json(list_url)  #
        current_page_posts = data.get("posts") if isinstance(data, dict) else data

        if not isinstance(current_page_posts, list):
            logger.warning(
                f"[{dept.name} ({board_type})] JSON API 응답의 'posts'가 리스트가 아님 ({list_url}). HTML Fallback 시도 예정. 데이터: {str(data)[:200]}")
            raise ValueError("JSON API 응답 형식이 리스트가 아님")

        logger.trace(f"[{dept.name} ({board_type})] JSON API 성공. {len(current_page_posts)}개 항목 수신.")
        for p_item in current_page_posts:
            post_id_str = str(p_item.get("id", "")).strip()
            title = clean_text(str(p_item.get("title", "")))
            raw_url = p_item.get("url", "")
            date_str = p_item.get("date", "")

            if not all([post_id_str, title, raw_url, date_str]):
                logger.warning(f"[{dept.name} ({board_type})] JSON 항목에 필수 정보 누락: {p_item}")
                continue

            if post_id_str.isdigit() and last_post_id_db.isdigit():
                if int(post_id_str) <= int(last_post_id_db): stop_crawling = True; break
            elif post_id_str <= last_post_id_db and post_id_str != "":
                stop_crawling = True; break

            parsed_date = parse_date_flexible(date_str)  #
            if not parsed_date: logger.warning(
                f"[{dept.name} ({board_type})] 날짜 파싱 실패 (ID: {post_id_str}, 날짜: '{date_str}'). 건너뜁니다."); continue

            full_url = urljoin(list_url, raw_url)
            notice_item = {"dept_id": dept.id, "board": board_type, "post_id": post_id_str,
                           "title": title, "url": full_url, "posted_at": parsed_date}
            if board_type == BOARD_TYPE_GRAD_KEYWORD:
                notice_item["source_display_name"] = f"{dept.name} 대학원"  #
            posts_data.append(notice_item)

        if posts_data: fetch_successful = True
        if stop_crawling: return posts_data, stop_crawling

    except (ClientError, json.JSONDecodeError, ValueError, Exception) as e_json:
        log_msg_prefix = f"[{dept.name} ({board_type})] JSON API"
        if isinstance(e_json, ClientError) and hasattr(e_json, 'status') and e_json.status == 404:  #
            logger.warning(f"{log_msg_prefix} 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
        elif isinstance(e_json, json.JSONDecodeError):
            logger.warning(f"{log_msg_prefix} 파싱 실패 ({list_url}): {e_json}. HTML Fallback 시도.")
        elif isinstance(e_json, asyncio.TimeoutError):  #
            logger.warning(f"{log_msg_prefix} 호출 시간 초과 ({list_url}). HTML Fallback 시도.")
        elif isinstance(e_json, ClientError):  #
            logger.warning(
                f"{log_msg_prefix} 호출 중 연결 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")
        else:
            logger.warning(
                f"{log_msg_prefix} 처리 중 기타 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")

        try:
            logger.trace(f"[{dept.name} ({board_type})] HTML Fallback 시도: {list_url}")
            html_content = await fetch_text(list_url)

            ids_html = html_select(html_content, "td.no")  #
            titles_html = html_select(html_content, "td.title a")  #
            links_html = html_select(html_content, "td.title a", "href")  #
            dates_html = html_select(html_content, "td.date")  #

            min_len = min(len(ids_html), len(titles_html), len(links_html), len(dates_html))
            if min_len == 0 and sum(map(len, [ids_html, titles_html, links_html, dates_html])) > 0:
                logger.warning(f"[{dept.name} ({board_type})] HTML에서 일부 정보만 추출됨. 파싱 건너뜀. URL: {list_url}")
            elif min_len > 0:
                logger.trace(f"[{dept.name} ({board_type})] HTML Fallback으로 {min_len}개 항목 후보 발견. URL: {list_url}")

            for i in range(min_len):
                post_id_str = clean_text(ids_html[i])
                if not post_id_str.isdigit():
                    id_match_from_url = re.search(r'(?:idx|id|no|seq|docSn)=(\d+)', links_html[i], re.I)
                    if id_match_from_url:
                        post_id_str = id_match_from_url.group(1)
                    else:
                        logger.warning(
                            f"[{dept.name} ({board_type})] HTML 항목 ID가 숫자가 아니고 URL에서 추출 불가 ('{ids_html[i]}'). 건너뜁니다."); continue

                if post_id_str.isdigit() and last_post_id_db.isdigit():
                    if int(post_id_str) <= int(last_post_id_db): stop_crawling = True; break
                elif post_id_str <= last_post_id_db and post_id_str != "":
                    stop_crawling = True; break

                title = clean_text(titles_html[i])
                raw_url = links_html[i]
                date_str = dates_html[i]
                parsed_date = parse_date_flexible(date_str)
                if not parsed_date: logger.warning(
                    f"[{dept.name} ({board_type})] HTML 날짜 파싱 실패 (ID: {post_id_str}, 날짜: '{date_str}'). 건너뜁니다."); continue

                full_url = urljoin(list_url, raw_url)
                notice_item = {"dept_id": dept.id, "board": board_type, "post_id": post_id_str,
                               "title": title, "url": full_url, "posted_at": parsed_date}
                if board_type == BOARD_TYPE_GRAD_KEYWORD:
                    notice_item["source_display_name"] = f"{dept.name} 대학원"
                posts_data.append(notice_item)

            if posts_data: fetch_successful = True
            if stop_crawling: return posts_data, stop_crawling

        except ClientError as e_html_fetch:  #
            log_msg_prefix_html = f"[{dept.name} ({board_type})] HTML Fallback"
            if hasattr(e_html_fetch, 'status') and e_html_fetch.status == 404:  #
                logger.error(f"{log_msg_prefix_html} URL 접근 실패 - 404 Not Found ({list_url}): {e_html_fetch.message}")
            elif isinstance(e_html_fetch, ClientError):  #
                logger.error(
                    f"{log_msg_prefix_html} URL 접근 중 연결 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
            else:
                logger.error(
                    f"{log_msg_prefix_html} URL 접근 중 알 수 없는 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
        except Exception as e_html_parse:
            logger.error(f"[{dept.name} ({board_type})] HTML Fallback 파싱 중 알 수 없는 오류 ({list_url}): {e_html_parse}")

    if not fetch_successful:
        logger.warning(f"[{dept.name} ({board_type})] 최종적으로 페이지에서 데이터를 가져오지 못했습니다. URL: {list_url}")

    return posts_data, stop_crawling


async def crawl_board(dept: Department, board_type: str):
    page = 1
    inserted_count = 0

    logger.info(f"📄 [{dept.name} ({board_type})] 공지사항 첫 페이지만 수집 시작")

    with get_session() as sess:
        last_notice = (sess.query(Notice)
                       .filter_by(dept_id=dept.id, board=board_type)
                       .order_by(Notice.post_id.desc())
                       .first())
        last_post_id_db = last_notice.post_id if last_notice else "0"
    logger.debug(f"[{dept.name} ({board_type})] DB의 마지막 게시글 ID: {last_post_id_db}")

    list_url = get_notice_list_url(dept, board_type, page)
    if not list_url:
        # get_notice_list_url 내부에서 이미 로그를 남기므로 여기서는 추가 로그 없이 종료
        return

    logger.debug(f"페이지 {page} ({board_type}) 공지사항 목록 요청: {list_url}")

    posts_to_save, stop_increment_crawl = await _parse_notice_page_content(dept, board_type, list_url, last_post_id_db)

    if stop_increment_crawl and not posts_to_save:
        logger.info(f"[{dept.name} ({board_type})] 증분 조건에 따라 첫 페이지에서 새로운 공지사항이 없습니다.")
    elif not posts_to_save:
        logger.info(f"[{dept.name} ({board_type})] 첫 페이지에서 데이터를 가져오지 못했습니다 (URL: {list_url}).")

    if posts_to_save:
        try:
            with get_session() as sess:
                sess.bulk_insert_mappings(Notice, posts_to_save)
                sess.commit()
            inserted_count = len(posts_to_save)
            logger.debug(f"[{dept.name} ({board_type})] 첫 페이지에서 {inserted_count}건 DB 저장 완료.")
        except Exception as e_db:
            logger.opt(exception=True).error(f"[{dept.name} ({board_type})] 공지사항 DB 저장 중 오류: {e_db}")

    if inserted_count > 0:
        logger.success(f"📄 [{dept.name} ({board_type})] 첫 페이지 새 공지 총 {inserted_count}건 수집 완료.")
    else:
        logger.info(f"📄 [{dept.name} ({board_type})] 첫 페이지에서 새로운 공지사항이 없거나 가져오지 못했습니다.")


async def find_and_attempt_parse_board_by_keyword(dept: Department, keywords: List[str], board_type_for_db: str,
                                                  search_url: str) -> bool:
    """
    주어진 search_url에서 keywords를 포함하는 링크를 찾아 해당 링크의 첫 페이지만 파싱 시도.
    성공적으로 링크를 찾고 해당 URL 템플릿을 Department 객체에 설정하면 True 반환.
    """
    logger.debug(f"[{dept.name}] '{search_url}' 에서 '{keywords}' 키워드로 '{board_type_for_db}' 게시판 링크 탐색 시도...")
    try:
        html_content = await fetch_text(search_url)
        found_board_url_template = None

        all_links_href = html_select(html_content, "a", attr="href")
        all_links_text = html_select(html_content, "a")

        for text, href in zip(all_links_text, all_links_href):
            cleaned_text = clean_text(text)
            if any(kw.lower() in cleaned_text.lower() for kw in keywords):
                potential_url = urljoin(search_url, href)
                parsed_link = urlparse(potential_url)
                query_params = parse_qs(parsed_link.query)
                common_page_params = ['page', 'pageNo', 'pageNum', 'pg', 'p', 'start']
                for p_key in common_page_params: query_params.pop(p_key, None)
                new_query = urlencode(query_params, doseq=True)
                base_link_for_template = urlunparse(
                    (parsed_link.scheme, parsed_link.netloc, parsed_link.path, parsed_link.params, new_query, ''))

                if "?" in base_link_for_template and not base_link_for_template.endswith("?"):
                    found_board_url_template = base_link_for_template + "&page={page}"
                elif "?" not in base_link_for_template:
                    found_board_url_template = base_link_for_template + "?page={page}"
                else:
                    found_board_url_template = base_link_for_template + "page={page}"

                logger.info(
                    f"[{dept.name}] 키워드 '{keywords}' 일치 링크 발견: '{cleaned_text}' -> {potential_url}. 생성된 템플릿: {found_board_url_template}")
                break

        if found_board_url_template:
            # 찾은 URL 템플릿을 Department 객체에 저장 (DB에 반영)
            with get_session() as sess:
                db_dept = sess.query(Department).filter_by(id=dept.id).first()
                if db_dept:
                    if board_type_for_db == BOARD_TYPE_ACADEMIC:
                        if db_dept.academic_notice_url_template != found_board_url_template:
                            db_dept.academic_notice_url_template = found_board_url_template
                            logger.info(f"[{dept.name}] 학사공지 URL 템플릿 업데이트: {found_board_url_template}")
                    elif board_type_for_db == BOARD_TYPE_UNDERGRAD:  # 예시: 다른 타입도 동일하게 처리
                        if db_dept.undergrad_notice_url_template != found_board_url_template:
                            db_dept.undergrad_notice_url_template = found_board_url_template
                            logger.info(f"[{dept.name}] 학부공지 URL 템플릿 업데이트: {found_board_url_template}")
                    # ... 다른 board_type에 대한 업데이트 로직 ...
                    elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                        if db_dept.specific_grad_keyword_notice_url != found_board_url_template:
                            db_dept.specific_grad_keyword_notice_url = found_board_url_template
                            logger.info(f"[{dept.name}] 대학원키워드 공지 URL 템플릿 업데이트: {found_board_url_template}")
                    sess.commit()
                    # 현재 dept 객체에도 반영 (이미 DB와 동기화된 객체라면 필요 없을 수 있으나, 명시적 반영)
                    if hasattr(dept, board_type_for_db.lower() + "_notice_url_template"):
                        setattr(dept, board_type_for_db.lower() + "_notice_url_template", found_board_url_template)
                    elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                        dept.specific_grad_keyword_notice_url = found_board_url_template

            await crawl_board(dept, board_type_for_db)  # 업데이트된 템플릿으로 바로 파싱 시도
            return True
        else:
            logger.info(f"[{dept.name}] '{search_url}'에서 '{keywords}' 관련 링크를 찾지 못했습니다.")
            return False
    except Exception as e:
        logger.error(f"[{dept.name}] '{search_url}'에서 '{keywords}' 게시판 탐색/파싱 중 오류: {e}")
        return False


async def crawl_grad_keyword_notices_simplified(dept: Department):
    """ "대학원" 키워드 관련 공지를 단순화된 방식으로 탐색 및 파싱 시도 """
    if dept.specific_grad_keyword_notice_url:  # 이미 DB에 URL 템플릿이 있다면 사용
        logger.info(f"[{dept.name}] 이미 설정된 '대학원' 관련 공지 URL({dept.specific_grad_keyword_notice_url}) 사용 시도.")
        await crawl_board(dept, BOARD_TYPE_GRAD_KEYWORD)
    else:
        # 학과 메인 페이지(dept.url)에서 "대학원 공지" 등의 키워드로 링크 탐색 시도
        grad_notice_keywords = ["대학원공지", "대학원 게시판", "대학원 자료실", "대학원 일반소식", "석사공지", "박사공지"]
        parsed_grad_keyword_board = await find_and_attempt_parse_board_by_keyword(
            dept, grad_notice_keywords, BOARD_TYPE_GRAD_KEYWORD, dept.url
        )
        if not parsed_grad_keyword_board:
            logger.info(f"[{dept.name}] 학과 메인 페이지에서 '대학원' 관련 명시적 공지 링크를 찾지 못했습니다.")


async def crawl_department_notices(dept: Department):
    delay_seconds = REQUEST_DELAY_DEPARTMENT_SECONDS
    if delay_seconds > 0:
        logger.trace(f"'{dept.name}' 학과 공지사항 전체 수집 시작 전 {delay_seconds:.1f}초 대기...")
        await asyncio.sleep(delay_seconds)

    parsed_academic = False
    # 1. 학사공지: DB에 저장된 academic_notice_url_template 사용
    if dept.academic_notice_url_template:
        logger.info(f"[{dept.name}] 설정된 학사공지 URL 템플릿으로 수집 시도.")
        await crawl_board(dept, BOARD_TYPE_ACADEMIC)
        parsed_academic = True
    else:
        # 템플릿이 없다면, 학과 메인 페이지에서 "학사공지" 키워드로 링크를 찾아보고,
        # 찾으면 해당 링크를 academic_notice_url_template으로 업데이트 후 파싱 시도
        logger.info(f"[{dept.name}] 학사공지 URL 템플릿 미설정. '{dept.url}'에서 '학사공지' 키워드 탐색 시도.")
        academic_keywords = ["학사공지", "학사안내", "학부학사", "학사일정"]
        # find_and_attempt_parse_board_by_keyword가 성공하면 내부적으로 crawl_board 호출 및 dept 객체 템플릿 업데이트
        parsed_academic = await find_and_attempt_parse_board_by_keyword(
            dept, academic_keywords, BOARD_TYPE_ACADEMIC, dept.url
        )

    # 2. 일반 공지사항 (학부/대학원)
    # 학사공지 파싱을 시도했는지 여부와 관계없이 (또는 parsed_academic 여부에 따라 조건부로) 실행 가능
    # 여기서는 학사공지를 찾지 못했거나, 또는 항상 일반 공지도 확인하는 로직
    if not parsed_academic:  # 학사공지를 찾지 못했거나 시도하지 않은 경우에만 일반공지 진행 (선택적 로직)
        logger.info(f"[{dept.name}] 학사공지를 찾지 못했거나 URL 템플릿이 없어 일반 공지사항으로 넘어갑니다.")

    # 학부 공지
    if dept.undergrad_notice_url_template:
        logger.info(f"[{dept.name}] 설정된 학부 공지사항 URL 템플릿으로 수집 시도.")
        await crawl_board(dept, BOARD_TYPE_UNDERGRAD)
    # elif dept.dept_type not in ["grad_school_dept", ...]: # 학부 공지 템플릿 없고, 대학원 전용 아니면 기본 시도 (제거 - 명시적 템플릿만 사용)
    #     logger.debug(f"[{dept.name}] 학부 공지 URL 템플릿 미설정. 기본 'undergrad' 타입 시도 안함.")

    # 대학원 공지
    if dept.grad_notice_url_template:
        logger.info(f"[{dept.name}] 설정된 대학원 공지사항 URL 템플릿으로 수집 시도.")
        await crawl_board(dept, BOARD_TYPE_GRAD)
    # elif dept.dept_type in ["grad_school_dept", ...] or "대학원" in dept.name: # 대학원 공지 템플릿 없고, 대학원 관련이면 기본 시도 (제거)
    #    logger.debug(f"[{dept.name}] 대학원 공지 URL 템플릿 미설정. 기본 'grad' 타입 시도 안함.")

    # 3. "대학원" 키워드 관련 공지 (조건부 실행)
    #    (이 로직은 학과 메인 페이지를 다시 스캔하므로, 부하를 줄이기 위해 필요한 경우에만 실행)
    if dept.dept_type in ["grad_school_dept", "plus_special_grad_dept", "plus_general_grad_dept"] or \
            "대학원" in dept.name or \
            dept.specific_grad_keyword_notice_url:  # 특정 URL이 이미 설정된 경우 포함
        await crawl_grad_keyword_notices_simplified(dept)