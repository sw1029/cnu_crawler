# src/cnu_crawler/spiders/notices.py
import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Coroutine, Any  # Coroutine, Any 추가
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

# BOARD_TYPES는 Department 모델의 URL 템플릿 필드와 연관지어 사용될 수 있음
# 또는, 각 crawl_board 호출 시 명시적으로 board_type을 지정
BOARD_TYPE_ACADEMIC = "academic"
BOARD_TYPE_UNDERGRAD = "undergrad"
BOARD_TYPE_GRAD = "grad"
BOARD_TYPE_GRAD_KEYWORD = "grad_keyword_found"  # "대학원" 키워드로 찾은 공지


def get_notice_list_url(dept: Department, board_type: str, page: int) -> Optional[str]:
    """
    Department 객체에 저장된 URL 템플릿과 board_type을 사용하여 공지사항 목록 URL을 생성합니다.
    """
    url_template: Optional[str] = None

    if board_type == BOARD_TYPE_ACADEMIC:
        url_template = dept.academic_notice_url_template
    elif board_type == BOARD_TYPE_UNDERGRAD:
        url_template = dept.undergrad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD:
        url_template = dept.grad_notice_url_template
    elif board_type == BOARD_TYPE_GRAD_KEYWORD:
        # specific_grad_keyword_notice_url은 페이지 파라미터가 없는 단일 URL일 수 있음
        # 또는 페이지네이션이 있는 목록 URL일 수도 있음. 여기서는 목록 URL 템플릿으로 가정.
        url_template = dept.specific_grad_keyword_notice_url
    else:
        logger.warning(f"[{dept.name}] 알 수 없는 board_type: '{board_type}'")
        return None

    if not url_template:
        logger.trace(f"[{dept.name}] 게시판 유형 '{board_type}'에 대한 URL 템플릿이 설정되지 않았습니다.")
        return None

    try:
        # URL 템플릿에 페이지 번호 플레이스홀더 처리
        if "{page}" in url_template:
            return url_template.replace("{page}", str(page))
        elif "{}" in url_template:  # 단순 format 플레이스홀더
            return url_template.format(page)
        else:
            # 페이지 파라미터가 없는 URL이거나, 직접 추가해야 하는 경우
            # 템플릿 자체가 이미 완전한 1페이지 URL일 수 있음 (페이지 파라미터가 없는 경우)
            # 여기서는 페이지 파라미터를 추가하는 형태로 가정
            parsed_template = urlparse(url_template)
            query_params = parse_qs(parsed_template.query)

            # 페이지 파라미터 이름 추론 (매우 기본적인 방식)
            page_param_name = "page"  # 기본값
            # 실제로는 더 많은 페이지 파라미터 이름 (pageNo, p, pageNum 등)을 확인해야 함

            query_params[page_param_name] = [str(page)]
            new_query = urlencode(query_params, doseq=True)
            # fragment는 유지하지 않음 (일반적으로 목록 API에는 fragment 불필요)
            return urlunparse((parsed_template.scheme, parsed_template.netloc, parsed_template.path,
                               parsed_template.params, new_query, ''))

    except Exception as e:
        logger.error(f"[{dept.name}] URL 템플릿 ('{url_template}') 처리 중 오류 (page={page}, board_type='{board_type}'): {e}")
        return None


async def _parse_notice_page_content(dept: Department, board_type: str, list_url: str, last_post_id_db: str) -> Tuple[
    List[Dict], bool]:
    """
    주어진 list_url에서 공지사항 내용을 파싱합니다. (JSON 우선, 실패 시 HTML)
    반환: (추출된 공지사항 dict 리스트, 증분 수집 중단 여부)
    """
    posts_data: List[Dict] = []
    stop_crawling = False
    fetch_successful = False

    try:  # JSON API 시도
        logger.trace(f"[{dept.name} ({board_type})] JSON API 시도: {list_url}")
        data = await fetch_json(list_url)  #
        # FIXME: 실제 API 응답 구조에 맞게 'posts' 키 및 내부 필드명('id', 'title' 등) 수정 필요
        current_page_posts = data.get("posts") if isinstance(data, dict) else data

        if not isinstance(current_page_posts, list):
            logger.warning(
                f"[{dept.name} ({board_type})] JSON API 응답의 'posts'가 리스트가 아님 ({list_url}). HTML Fallback 시도 예정. 데이터: {str(data)[:200]}")
            raise ValueError("JSON API 응답 형식이 리스트가 아님")  # HTML Fallback 유도

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
            if board_type == BOARD_TYPE_GRAD_KEYWORD:  # "대학원" 키워드로 찾은 공지
                notice_item["source_display_name"] = f"{dept.name} 대학원"  #
            posts_data.append(notice_item)

        if posts_data: fetch_successful = True
        if stop_crawling: return posts_data, stop_crawling  # 증분 중단 시 바로 반환

    except (ClientError, json.JSONDecodeError, ValueError, Exception) as e_json:
        # 에러 로깅 (이전 답변의 상세 로깅 참고하여 적용)
        log_msg_prefix = f"[{dept.name} ({board_type})] JSON API"
        if isinstance(e_json, ClientError) and hasattr(e_json, 'status') and e_json.status == 404:
            logger.warning(f"{log_msg_prefix} 호출 실패 - 404 Not Found ({list_url}). HTML Fallback 시도.")
        elif isinstance(e_json, json.JSONDecodeError):
            logger.warning(f"{log_msg_prefix} 파싱 실패 ({list_url}): {e_json}. HTML Fallback 시도.")
        # ... 기타 ClientError, TimeoutError 등 상세 로깅 ...
        else:
            logger.warning(
                f"{log_msg_prefix} 처리 중 오류 ({list_url}): {type(e_json).__name__} - {e_json}. HTML Fallback 시도.")

        # HTML Fallback 시도
        try:
            logger.trace(f"[{dept.name} ({board_type})] HTML Fallback 시도: {list_url}")
            html_content = await fetch_text(list_url)

            # FIXME: 각 사이트의 HTML 구조에 맞는 정확한 CSS 선택자로 수정해야 합니다.
            # 아래는 일반적인 게시판 목록 테이블 구조에 대한 예시입니다.
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
                if not post_id_str.isdigit():  # '공지' 등 숫자 아닌 ID 처리
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
                logger.error(f"{log_msg_prefix_html} URL 접근 실패 - 404 Not Found ({list_url}): {e_html_fetch.message}")  #
            # ... 기타 ClientError 상세 로깅 ...
            else:
                logger.error(
                    f"{log_msg_prefix_html} URL 접근 중 오류 ({list_url}): {type(e_html_fetch).__name__} - {e_html_fetch}")
        except Exception as e_html_parse:
            logger.error(f"[{dept.name} ({board_type})] HTML Fallback 파싱 중 알 수 없는 오류 ({list_url}): {e_html_parse}")

    if not fetch_successful:  # JSON, HTML 모두 실패 또는 데이터 없음
        logger.warning(f"[{dept.name} ({board_type})] 최종적으로 페이지에서 데이터를 가져오지 못했습니다. URL: {list_url}")

    return posts_data, stop_crawling


async def crawl_board(dept: Department, board_type: str):
    """특정 학과의 특정 게시판 유형에 대해 첫 페이지만 크롤링합니다."""
    page = 1  # 첫 페이지만 대상
    inserted_count = 0

    logger.info(f"📄 [{dept.name} ({board_type})] 공지사항 첫 페이지만 수집 시작")

    with get_session() as sess:  # DB 연결 세션 가져오기
        last_notice = (sess.query(Notice)
                       .filter_by(dept_id=dept.id, board=board_type)
                       .order_by(Notice.post_id.desc())
                       .first())
        last_post_id_db = last_notice.post_id if last_notice else "0"
    logger.debug(f"[{dept.name} ({board_type})] DB의 마지막 게시글 ID: {last_post_id_db}")

    list_url = get_notice_list_url(dept, board_type, page)
    if not list_url or "invalid_board_key" in list_url:
        logger.error(f"[{dept.name} ({board_type})] 유효한 공지사항 목록 URL을 생성할 수 없어 수집을 중단합니다.")
        # 이전 로그에서 이 메시지가 없으므로, get_notice_list_url이 항상 유효한 문자열을 반환한다고 가정
        # 또는 URL 생성 실패시 None을 반환하고 여기서 체크
        if not list_url: return

    logger.debug(f"페이지 {page} ({board_type}) 공지사항 목록 요청: {list_url}")

    # _parse_notice_page_content 함수 호출 (첫 페이지만)
    # 이 함수는 (파싱된 공지 리스트, 증분 중단 여부)를 반환
    posts_to_save, stop_increment_crawl = await _parse_notice_page_content(dept, board_type, list_url, last_post_id_db)

    if stop_increment_crawl and not posts_to_save:  # 증분으로 인해 가져올 새 글이 없는 경우
        logger.info(f"[{dept.name} ({board_type})] 증분 조건에 따라 첫 페이지에서 새로운 공지사항이 없습니다.")
    elif not posts_to_save:  # 증분 중단은 아니지만, 파싱 결과가 없는 경우 (404, 빈 페이지 등)
        logger.info(f"[{dept.name} ({board_type})] 첫 페이지에서 데이터를 가져오지 못했습니다 (URL: {list_url}).")  #

    if posts_to_save:
        try:
            with get_session() as sess:
                # Notice 모델에 source_display_name 필드가 있어야 함
                sess.bulk_insert_mappings(Notice, posts_to_save)
                sess.commit()
            inserted_count = len(posts_to_save)
            logger.debug(f"[{dept.name} ({board_type})] 첫 페이지에서 {inserted_count}건 DB 저장 완료.")
        except Exception as e_db:
            logger.opt(exception=True).error(f"[{dept.name} ({board_type})] 공지사항 DB 저장 중 오류: {e_db}")

    if inserted_count > 0:
        logger.success(f"📄 [{dept.name} ({board_type})] 첫 페이지 새 공지 총 {inserted_count}건 수집 완료.")
    else:  # inserted_count가 0인 모든 경우
        logger.info(f"📄 [{dept.name} ({board_type})] 첫 페이지에서 새로운 공지사항이 없거나 가져오지 못했습니다.")  #


async def find_and_attempt_parse_board_by_keyword(dept: Department, keywords: List[str], board_type_for_db: str,
                                                  search_url: str) -> bool:
    """
    주어진 search_url에서 keywords를 포함하는 링크를 찾아 해당 링크의 첫 페이지만 파싱 시도.
    성공 여부 (링크를 찾고 파싱 시도를 했는지)를 반환.
    """
    logger.debug(f"[{dept.name}] '{search_url}' 에서 '{keywords}' 키워드로 '{board_type_for_db}' 게시판 링크 탐색 시도...")
    try:
        html_content = await fetch_text(search_url)
        found_board_url = None

        all_links_href = html_select(html_content, "a", attr="href")
        all_links_text = html_select(html_content, "a")

        for text, href in zip(all_links_text, all_links_href):
            cleaned_text = clean_text(text)
            if any(kw.lower() in cleaned_text.lower() for kw in keywords):
                # 링크가 유효한 게시판 목록 URL인지 추가 검증 필요 (예: 특정 패턴 포함 여부)
                # 여기서는 첫 번째 발견 링크를 사용한다고 가정
                potential_url = urljoin(search_url, href)
                # 이미 페이지 파라미터가 있다면 제거하고 템플릿화 시도
                parsed_link = urlparse(potential_url)
                # query에서 page 관련 파라미터 제거 (매우 단순한 방식)
                # query_params = parse_qs(parsed_link.query)
                # for page_key in ['page', 'pageNo', 'pageNum', 'pg']: query_params.pop(page_key, None)
                # new_query = urlencode(query_params, doseq=True)
                # base_link_for_template = urlunparse((parsed_link.scheme, parsed_link.netloc, parsed_link.path, parsed_link.params, new_query, ''))

                # 여기서는 발견된 URL을 그대로 사용하고, get_notice_list_url에서 페이지 파라미터 처리
                found_board_url = potential_url
                logger.info(
                    f"[{dept.name}] 키워드 '{keywords}' 일치 링크 발견: '{cleaned_text}' -> {found_board_url} (게시판 타입: {board_type_for_db})")
                break

        if found_board_url:
            # Department 객체의 해당 board_type URL 템플릿을 임시로 설정하거나,
            # get_notice_list_url이 이 URL을 직접 사용할 수 있도록 수정 필요.
            # 여기서는 Department 모델에 저장된 템플릿이 우선이라고 가정하고,
            # 만약 이 함수가 찾은 URL이 더 정확하다면, 해당 Department 객체의 URL 템플릿을 업데이트해야 함.
            # 지금은 찾은 URL을 기반으로 임시 URL 템플릿을 만들어 crawl_board 호출 시도.

            # dept 객체를 직접 수정하는 것은 side effect를 유발할 수 있으므로 주의.
            # 여기서는 get_notice_list_url이 잘 동작하도록 해당 템플릿 필드를 임시 설정.
            # 더 좋은 방법은 crawl_board가 URL을 직접 받도록 하는 것.
            temp_original_templates = {
                BOARD_TYPE_ACADEMIC: dept.academic_notice_url_template,
                BOARD_TYPE_UNDERGRAD: dept.undergrad_notice_url_template,
                BOARD_TYPE_GRAD: dept.grad_notice_url_template,
                BOARD_TYPE_GRAD_KEYWORD: dept.specific_grad_keyword_notice_url
            }

            # 페이지 파라미터가 이미 있는지 확인하고 템플릿 생성
            parsed_found_url = urlparse(found_board_url)
            query_found = parse_qs(parsed_found_url.query)
            if any(p_key in query_found for p_key in ['page', 'pageNo', 'pageNum', 'pg']):  # 이미 페이지 파라미터가 있다면
                # 해당 파라미터를 {}로 교체하는 정교한 로직 필요. 여기서는 단순화.
                # 또는 found_board_url을 page=1로 간주하고 파싱
                # 여기서는 get_notice_list_url이 처리하도록 원본 URL을 템플릿처럼 사용
                url_template_for_crawl = found_board_url
            else:  # 페이지 파라미터가 없다면 추가
                url_template_for_crawl = found_board_url + ("&page={}" if "?" in found_board_url else "?page={}")

            if board_type_for_db == BOARD_TYPE_ACADEMIC:
                dept.academic_notice_url_template = url_template_for_crawl
            elif board_type_for_db == BOARD_TYPE_UNDERGRAD:
                dept.undergrad_notice_url_template = url_template_for_crawl
            elif board_type_for_db == BOARD_TYPE_GRAD:
                dept.grad_notice_url_template = url_template_for_crawl
            elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                dept.specific_grad_keyword_notice_url = url_template_for_crawl

            await crawl_board(dept, board_type_for_db)

            # 원래 템플릿으로 복원 (주의: 이 방식은 동시성 문제 발생 가능. 객체 상태 변경은 신중해야 함)
            if board_type_for_db == BOARD_TYPE_ACADEMIC:
                dept.academic_notice_url_template = temp_original_templates[BOARD_TYPE_ACADEMIC]
            elif board_type_for_db == BOARD_TYPE_UNDERGRAD:
                dept.undergrad_notice_url_template = temp_original_templates[BOARD_TYPE_UNDERGRAD]
            elif board_type_for_db == BOARD_TYPE_GRAD:
                dept.grad_notice_url_template = temp_original_templates[BOARD_TYPE_GRAD]
            elif board_type_for_db == BOARD_TYPE_GRAD_KEYWORD:
                dept.specific_grad_keyword_notice_url = temp_original_templates[BOARD_TYPE_GRAD_KEYWORD]

            return True  # 링크 찾고 파싱 시도함
        else:
            logger.info(f"[{dept.name}] '{search_url}'에서 '{keywords}' 관련 링크를 찾지 못했습니다.")
            return False

    except Exception as e:
        logger.error(f"[{dept.name}] '{search_url}'에서 '{keywords}' 게시판 탐색/파싱 중 오류: {e}")
        return False


async def crawl_grad_keyword_notices_simplified(dept: Department):
    """
    "대학원" 키워드 관련 공지를 매우 단순화된 방식으로 탐색 및 파싱 시도.
    학과 메인 페이지(dept.url)에서 "대학원" "공지" 등의 키워드를 포함하는 링크를 찾음.
    """
    # "대학원" 자체를 지칭하는 이름의 Department 객체 (예: dept.name == "일반대학원")는 이 로직을 건너뛸 수 있음
    if "대학원" not in dept.name and dept.dept_type not in ["grad_school_dept", "plus_special_grad_dept",
                                                         "plus_general_grad_dept"]:
        # 일반 학과의 경우, "대학원 과정" 등에 대한 공지가 별도로 있는지 확인 시도
        # 이 로직은 매우 부정확할 수 있음
        logger.debug(f"[{dept.name}] 일반 학과로 간주, '대학원 공지' 등 키워드 탐색 시도 (매우 휴리스틱).")

    # Department 모델에 specific_grad_keyword_notice_url이 이미 설정되어 있다면 그것을 사용
    if dept.specific_grad_keyword_notice_url:
        logger.info(f"[{dept.name}] 이미 설정된 '대학원' 관련 공지 URL 사용 시도: {dept.specific_grad_keyword_notice_url}")
        await crawl_board(dept, BOARD_TYPE_GRAD_KEYWORD)  # 페이지 번호는 get_notice_list_url에서 처리
    else:
        # 학과 메인 페이지(dept.url)에서 "대학원 공지", "대학원 게시판", "일반소식(대학원)" 등의 링크 탐색
        # 이 부분은 find_and_attempt_parse_board_by_keyword 함수와 유사한 로직 사용 가능
        grad_notice_keywords = ["대학원공지", "대학원 게시판", "대학원 자료실", "대학원 일반소식", "석사공지", "박사공지"]
        # "학사공지"나 "일반소식"은 너무 일반적이므로 "대학원"과 조합된 키워드 우선

        parsed_grad_keyword_board = await find_and_attempt_parse_board_by_keyword(
            dept, grad_notice_keywords, BOARD_TYPE_GRAD_KEYWORD, dept.url
        )
        if not parsed_grad_keyword_board:
            logger.info(f"[{dept.name}] 학과 메인 페이지에서 '대학원' 관련 명시적 공지 링크를 찾지 못했습니다.")
            # 추가적으로, "대학원"이라는 텍스트 주변의 구조를 분석하는 것은 현재 프레임워크에서 어려움
            # "10글자 이하의 '대학원' 포함 항목 -> 하위 학사공지/일반소식 -> 링크 없으면 해당 영역 파싱" 은
            # 현재 aiohttp + beautifulsoup 기반으로는 매우 복잡하고, Selenium 및 정교한 DOM 분석 필요


async def crawl_department_notices(dept: Department):
    """
    주어진 Department 객체에 대해 정의된 순서대로 공지사항을 크롤링합니다.
    1. 학사공지 (academic)
    2. 일반공지 (undergrad/grad) - 학사공지 없었을 경우
    3. 대학원 키워드 관련 공지 (grad_keyword_found)
    """
    # 각 작업 전 딜레이
    delay_seconds = REQUEST_DELAY_DEPARTMENT_SECONDS
    if delay_seconds > 0:
        logger.trace(f"'{dept.name}' 학과 공지사항 전체 수집 시작 전 {delay_seconds:.1f}초 대기...")
        await asyncio.sleep(delay_seconds)

    parsed_any_notice = False

    # 1. "학사공지" 우선 탐색 및 파싱 시도
    # Department 모델에 academic_notice_url_template이 설정되어 있다면 직접 사용
    if dept.academic_notice_url_template:
        logger.info(f"[{dept.name}] 설정된 학사공지 URL 템플릿으로 수집 시도.")
        await crawl_board(dept, BOARD_TYPE_ACADEMIC)
        parsed_any_notice = True  # 시도 자체를 성공으로 간주 (내부에서 결과 로깅)
    else:
        # 학과 메인 페이지(dept.url)에서 "학사공지", "학사안내" 등의 링크를 찾아 파싱 시도
        # 이 로직은 find_and_attempt_parse_board_by_keyword 함수로 대체 가능
        academic_keywords = ["학사공지", "학사안내", "학부학사"]  # 더 많은 키워드 추가 가능
        parsed_academic = await find_and_attempt_parse_board_by_keyword(
            dept, academic_keywords, BOARD_TYPE_ACADEMIC, dept.url
        )
        if parsed_academic: parsed_any_notice = True

    # 2. "학사공지"를 찾지 못했거나 파싱 시도 후 결과가 없다면, 일반 공지사항(학부/대학원) 파싱
    #    (현재는 parsed_any_notice로 이전 단계의 성공 여부만 판단, 실제 데이터 유무는 crawl_board에서 로깅)
    #    또는, 학사공지와 별개로 항상 일반공지도 가져오려면 이 if 조건 제거
    if not parsed_any_notice or dept.undergrad_notice_url_template or dept.grad_notice_url_template:  # 학사공지 시도 안했거나, 일반 공지 템플릿이 있다면
        if not parsed_any_notice:
            logger.info(f"[{dept.name}] 학사공지 관련 정보를 찾지 못했거나 URL 템플릿이 없어 일반 공지사항으로 넘어갑니다.")

        if dept.undergrad_notice_url_template:
            logger.info(f"[{dept.name}] 설정된 학부 공지사항 URL 템플릿으로 수집 시도.")
            await crawl_board(dept, BOARD_TYPE_UNDERGRAD)
            if not parsed_any_notice: parsed_any_notice = True  # 일반 공지 시도 자체를 기록
        elif dept.dept_type not in ["grad_school_dept", "plus_special_grad_dept",
                                    "plus_general_grad_dept"]:  # 대학원 전용이 아닌 경우, 기본 undergrad 시도
            logger.debug(f"[{dept.name}] 학부 공지 URL 템플릿 미설정. 기본 'undergrad' 타입으로 시도.")
            await crawl_board(dept, BOARD_TYPE_UNDERGRAD)  # get_notice_list_url 에서 BOARD_CODES 기본값 사용 시도
            if not parsed_any_notice: parsed_any_notice = True

        if dept.grad_notice_url_template:  # 대학원 공지가 설정된 경우
            logger.info(f"[{dept.name}] 설정된 대학원 공지사항 URL 템플릿으로 수집 시도.")
            await crawl_board(dept, BOARD_TYPE_GRAD)
            if not parsed_any_notice: parsed_any_notice = True
        elif dept.dept_type in ["grad_school_dept", "plus_special_grad_dept",
                                "plus_general_grad_dept"] or "대학원" in dept.name:
            # 대학원 관련 학과인데 grad_notice_url_template이 없는 경우, 기본 'grad' 타입 시도
            logger.debug(f"[{dept.name}] 대학원 관련 학과이나 대학원 공지 URL 템플릿 미설정. 기본 'grad' 타입으로 시도.")
            await crawl_board(dept, BOARD_TYPE_GRAD)
            if not parsed_any_notice: parsed_any_notice = True

    # 3. "대학원" 키워드 관련 공지 탐색 및 파싱 (조건부 실행)
    #    - dept_type이 대학원 관련이거나, 이름에 '대학원'이 포함된 경우
    #    - 또는 모든 학과에 대해 시도해 볼 수도 있으나, 부하와 정확도 문제
    if dept.dept_type in ["grad_school_dept", "plus_special_grad_dept", "plus_general_grad_dept"] or \
            "대학원" in dept.name or \
            dept.specific_grad_keyword_notice_url:  # 특정 URL이 이미 설정된 경우 포함
        await crawl_grad_keyword_notices_simplified(dept)
        # 이 결과도 parsed_any_notice에 반영할 수 있으나, 이미 다른 공지를 가져왔을 가능성이 높음

    if not parsed_any_notice:
        logger.warning(f"[{dept.name}] 어떤 유형의 공지사항도 성공적으로 수집 시도하지 못했습니다 (URL 템플릿 부재 또는 탐색 실패).")