# src/cnu_crawler/spiders/notices.py
import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Coroutine, Any, Tuple  # <- Tuple 임포트 추가
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode

from loguru import logger
from aiohttp import ClientError

from cnu_crawler.core.fetcher import fetch_text, fetch_json
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import Department, Notice, get_session
from cnu_crawler.utils import clean_text, parse_date_flexible
from cnu_crawler.config import (
    REQUEST_DELAY_NOTICE_PAGE_SECONDS,
    REQUEST_DELAY_DEPARTMENT_SECONDS
)

BOARD_TYPE_ACADEMIC = "academic"
BOARD_TYPE_UNDERGRAD = "undergrad"
BOARD_TYPE_GRAD = "grad"
BOARD_TYPE_GRAD_KEYWORD = "grad_keyword_found"

BOARD_CODES = {  # 이전 답변에서 유지된 부분, get_notice_list_url 에서 사용될 수 있음
    BOARD_TYPE_UNDERGRAD: "board?code=undergrad_notice",
    BOARD_TYPE_GRAD: "board?code=grad_notice"
}


def college_code_from_url(college_url: str) -> Optional[str]:
    try:
        hostname = college_url.split('/')[2]
        return hostname.split('.')[0]
    except IndexError:
        logger.warning(f"URL에서 대학 코드를 추출할 수 없습니다: {college_url}")
        return None


def get_notice_list_url(dept: Department, board_type: str, page: int) -> Optional[str]:
    url_template: Optional[str] = None

    if board_type == BOARD_TYPE_ACADEMIC:
        url_template = dept.academic_notice_url_template
    elif board_type == BOARD_TYPE_UNDERGRAD:
        url_template = dept.undergrad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD:
        url_template = dept.grad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD_KEYWORD:
        url_template = dept.specific_grad_keyword_notice_url

    # URL 템플릿이 없는 경우, BOARD_CODES를 사용하여 기본 경로 시도 (이전 로직 호환성)
    if not url_template and board_type in BOARD_CODES:
        department_base_url = dept.url.rstrip("/")
        # dept.url에서 # 이후 부분 제거 (이전 답변의 로직)
        parsed_dept_url = urlparse(department_base_url)
        path_for_dept_base = parsed_dept_url.path if parsed_dept_url.path else '/'
        clean_base_url = urlunparse((parsed_dept_url.scheme, parsed_dept_url.netloc, path_for_dept_base, '', '', ''))
        clean_base_url = clean_base_url.rstrip('/')

        board_path_segment = BOARD_CODES.get(board_type)
        if board_path_segment:
            # 이 조합 방식은 여전히 404 가능성이 높으므로, 학과별 템플릿 설정이 최선
            final_url_base = f"{clean_base_url}/{board_path_segment}"
            if '?' in final_url_base:
                url_template = final_url_base + "&page={page}"  # 페이지 플레이스홀더 사용
            else:
                url_template = final_url_base + "?page={page}"
            logger.debug(
                f"[{dept.name} ({board_type})] URL 템플릿 미설정, BOARD_CODES 기반 URL 생성: {url_template.format(page=page)}")

    if not url_template:
        logger.trace(f"[{dept.name}] 게시판 유형 '{board_type}'에 대한 최종 URL 템플릿을 찾을 수 없습니다.")
        return None

    try:
        if "{page}" in url_template:
            return url_template.replace("{page}", str(page))
        elif "{}" in url_template:
            return url_template.format(page)
        else:
            parsed_template = urlparse(url_template)
            query_params = parse_qs(parsed_template.query)
            page_param_name = "page"
            query_params[page_param_name] = [str(page)]
            new_query = urlencode(query_params, doseq=True)
            return urlunparse((parsed_template.scheme, parsed_template.netloc, parsed_template.path,
                               parsed_template.params, new_query, ''))

    except Exception as e:
        logger.error(f"[{dept.name}] URL 템플릿 ('{url_template}') 처리 중 오류 (page={page}, board_type='{board_type}'): {e}")
        return None


# Tuple을 사용하는 함수 정의
async def _parse_notice_page_content(dept: Department, board_type: str, list_url: str, last_post_id_db: str) -> Tuple[
    List[Dict], bool]:
    posts_data: List[Dict] = []
    stop_crawling = False
    fetch_successful = False

    try:
        logger.trace(f"[{dept.name} ({board_type})] JSON API 시도: {list_url}")
        data = await fetch_json(list_url)
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

            parsed_date = parse_date_flexible(date_str)
            if not parsed_date: logger.warning(
                f"[{dept.name} ({board_type})] 날짜 파싱 실패 (ID: {post_id_str}, 날짜: '{date_str}'). 건너뜁니다."); continue

            full_url = urljoin(list_url, raw_url)
            notice_item = {"dept_id": dept.id, "board": board_type, "post_id": post_id_str,
                           "title": title, "url": full_url, "posted_at": parsed_date}
            if board_type == BOARD_TYPE_GRAD_KEYWORD:
                notice_item["source_display_name"] = f"{dept.name} 대학원"
            posts_data.append(notice_item)

        if posts_data: fetch_successful = True
        if stop_crawling: return posts_data, stop_crawling

    except (ClientError, json.JSONDecodeError, ValueError, Exception) as e_json:
        log_msg_prefix = f"[{dept.name} ({board_type})] JSON API"
        if isinstance(e_json, ClientError) and hasattr(e_json, 'status') and e_json.status == 404:
            logger.warning(f"{log_msg_prefix} 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
        elif isinstance(e_json, json.JSONDecodeError):
            logger.warning(f"{log_msg_prefix} 파싱 실패 ({list_url}): {e_json}. HTML Fallback 시도.")
        elif isinstance(e_json, asyncio.TimeoutError):
            logger.warning(f"{log_msg_prefix} 호출 시간 초과 ({list_url}). HTML Fallback 시도.")
        elif isinstance(e_json, ClientError):
            logger.warning(
                f"{log_msg_prefix} 호출 중 연결 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")
        else:
            logger.warning(
                f"{log_msg_prefix} 처리 중 기타 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")

        try:
            logger.trace(f"[{dept.name} ({board_type})] HTML Fallback 시도: {list_url}")
            html_content = await fetch_text(list_url)

            ids_html = html_select(html_content, "td.no")
            titles_html = html_select(html_content, "td.title a")
            links_html = html_select(html_content, "td.title a", "href")
            dates_html = html_select(html_content, "td.date")

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

        except ClientError as e_html_fetch:
            log_msg_prefix_html = f"[{dept.name} ({board_type})] HTML Fallback"
            if hasattr(e_html_fetch, 'status') and e_html_fetch.status == 404:
                logger.error(f"{log_msg_prefix_html} URL 접근 실패 - 404 Not Found ({list_url}): {e_html_fetch.message}")
            elif isinstance(e_html_fetch, ClientError):
                logger.error(
                    f"{log_msg_prefix_html} URL 접근 중 연결 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
            else:  # 이 경우는 거의 발생 안 함 (ClientError가 아닌 경우)
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
    if not list_url:  # get_notice_list_url이 None을 반환하면 (템플릿 없거나 오류)
        logger.error(f"[{dept.name} ({board_type})] 공지사항 목록 URL을 얻을 수 없어 수집을 중단합니다.")
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
    logger.debug(f"[{dept.name}] '{search_url}' 에서 '{keywords}' 키워드로 '{board_type_for_db}' 게시판 링크 탐색 시도...")
    try:
        html_content = await fetch_text(search_url)
        found_board_url_template = None  # 페이지 파라미터가 포함된 템플릿 또는 기본 URL

        all_links_href = html_select(html_content, "a", attr="href")
        all_links_text = html_select(html_content, "a")

        for text, href in zip(all_links_text, all_links_href):
            cleaned_text = clean_text(text)
            if any(kw.lower() in cleaned_text.lower() for kw in keywords):
                potential_url = urljoin(search_url, href)
                # 이 URL이 실제 목록 페이지인지, 아니면 상세 페이지인지, 페이지 파라미터는 무엇인지 등 분석 필요
                # 여기서는 간단히 이 URL을 기본으로 하고, page 파라미터를 붙이는 템플릿으로 가정
                parsed_link = urlparse(potential_url)
                # query에서 page 관련 파라미터 제거 시도 (매우 단순한 방식)
                query_params = parse_qs(parsed_link.query)
                # 일반적인 페이지 파라미터 이름들
                common_page_params = ['page', 'pageNo', 'pageNum', 'pg', 'p', 'start']
                for p_key in common_page_params: query_params.pop(p_key, None)

                new_query = urlencode(query_params, doseq=True)
                base_link_for_template = urlunparse(
                    (parsed_link.scheme, parsed_link.netloc, parsed_link.path, parsed_link.params, new_query, ''))

                # 템플릿 생성: ?page={} 또는 &page={}
                if "?" in base_link_for_template:
                    found_board_url_template = base_link_for_template + "&page={page}"
                else:
                    found_board_url_template = base_link_for_template + "?page={page}"

                logger.info(
                    f"[{dept.name}] 키워드 '{keywords}' 일치 링크 발견: '{cleaned_text}' -> {potential_url}. 생성된 템플릿: {found_board_url_template}")
                break

        if found_board_url_template:
            # 찾은 URL 템플릿을 Department 객체에 임시로 설정하여 crawl_board에서 사용
            # (주의: 이 방식은 Department 객체의 상태를 변경하므로, 동시성 문제나 의도치 않은 효과를 유발할 수 있음.
            #  더 나은 방식은 crawl_board가 URL 템플릿을 직접 인자로 받거나,
            #  Department 객체를 복사하여 수정 후 전달하는 것입니다.)
            original_template = None
            if board_type_for_db == BOARD_TYPE_ACADEMIC:
                original_template = dept.academic_notice_url_template
                dept.academic_notice_url_template = found_board_url_template
            elif board_type_for_db == BOARD_TYPE_UNDERGRAD:
                original_template = dept.undergrad_notice_url_template
                dept.undergrad_notice_url_template = found_board_url_template
            # ... 다른 board_type에 대한 처리 ...
            elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                original_template = dept.specific_grad_keyword_notice_url
                dept.specific_grad_keyword_notice_url = found_board_url_template

            await crawl_board(dept, board_type_for_db)

            # 원래 템플릿으로 복원
            if original_template is not None:  # original_template이 None이 아닌 경우에만 복원 시도
                if board_type_for_db == BOARD_TYPE_ACADEMIC:
                    dept.academic_notice_url_template = original_template
                elif board_type_for_db == BOARD_TYPE_UNDERGRAD:
                    dept.undergrad_notice_url_template = original_template
                elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                    dept.specific_grad_keyword_notice_url = original_template

            return True
        else:
            logger.info(f"[{dept.name}] '{search_url}'에서 '{keywords}' 관련 링크를 찾지 못했습니다.")
            return False
    except Exception as e:
        logger.error(f"[{dept.name}] '{search_url}'에서 '{keywords}' 게시판 탐색/파싱 중 오류: {e}")
        return False


async def crawl_grad_keyword_notices_simplified(dept: Department):
    if dept.specific_grad_keyword_notice_url:
        logger.info(f"[{dept.name}] 이미 설정된 '대학원' 관련 공지 URL 사용 시도: {dept.specific_grad_keyword_notice_url}")
        await crawl_board(dept, BOARD_TYPE_GRAD_KEYWORD)
    else:
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

    parsed_any_notice = False

    if dept.academic_notice_url_template:
        logger.info(f"[{dept.name}] 설정된 학사공지 URL 템플릿으로 수집 시도.")
        await crawl_board(dept, BOARD_TYPE_ACADEMIC)
        parsed_any_notice = True
    else:
        academic_keywords = ["학사공지", "학사안내", "학부학사"]
        parsed_academic = await find_and_attempt_parse_board_by_keyword(
            dept, academic_keywords, BOARD_TYPE_ACADEMIC, dept.url
        )
        if parsed_academic: parsed_any_notice = True

    if not parsed_any_notice or dept.undergrad_notice_url_template or dept.grad_notice_url_template:
        if not parsed_any_notice:
            logger.info(f"[{dept.name}] 학사공지 관련 정보를 찾지 못했거나 URL 템플릿이 없어 일반 공지사항으로 넘어갑니다.")

        if dept.undergrad_notice_url_template:
            logger.info(f"[{dept.name}] 설정된 학부 공지사항 URL 템플릿으로 수집 시도.")
            await crawl_board(dept, BOARD_TYPE_UNDERGRAD)
        elif dept.dept_type not in ["grad_school_dept", "plus_special_grad_dept", "plus_general_grad_dept"]:
            logger.debug(f"[{dept.name}] 학부 공지 URL 템플릿 미설정. 기본 'undergrad' 타입으로 시도.")
            await crawl_board(dept, BOARD_TYPE_UNDERGRAD)

        if dept.grad_notice_url_template:
            logger.info(f"[{dept.name}] 설정된 대학원 공지사항 URL 템플릿으로 수집 시도.")
            await crawl_board(dept, BOARD_TYPE_GRAD)
        elif dept.dept_type in ["grad_school_dept", "plus_special_grad_dept",
                                "plus_general_grad_dept"] or "대학원" in dept.name:
            logger.debug(f"[{dept.name}] 대학원 관련 학과이나 대학원 공지 URL 템플릿 미설정. 기본 'grad' 타입으로 시도.")
            await crawl_board(dept, BOARD_TYPE_GRAD)

    if dept.dept_type in ["grad_school_dept", "plus_special_grad_dept", "plus_general_grad_dept"] or \
            "대학원" in dept.name or \
            dept.specific_grad_keyword_notice_url:
        await crawl_grad_keyword_notices_simplified(dept)