# cnu_crawler/spiders/notices.py
from datetime import datetime
from typing import Dict, List
from loguru import logger
import json  # aiohttp는 이미 JSONDecodeError를 발생시킬 수 있지만, 명시적 import
from aiohttp import ClientError  # fetcher에서 발생할 수 있는 예외
from urllib.parse import urljoin  # 상대 URL을 절대 URL로 변환하기 위함

from cnu_crawler.core.fetcher import fetch_json, fetch_text
from cnu_crawler.core.parser import html_select
from cnu_crawler.storage import Department, Notice, get_session
from cnu_crawler.utils import clean_text  # 제목 등 텍스트 정제용 (필요시)

# FIXME: 실제 게시판 코드나 URL 파라미터가 변경되었다면 수정 필요.
BOARD_CODES = {  #
    "undergrad": "board?code=undergrad_notice",  #
    "grad": "board?code=grad_notice"  #
}


# 다양한 날짜 형식을 시도하기 위한 헬퍼 함수
def parse_date_flexible(date_str: str) -> datetime | None:
    if not date_str:
        return None

    # 일반적인 형식들을 순서대로 시도
    formats_to_try = [
        "%Y-%m-%dT%H:%M:%S",  # ISO 부분 (TZ 정보 없이)
        "%Y-%m-%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%y-%m-%d",  # 24-05-28
        "%y.%m.%d",
    ]
    # 원본 코드의 ISO 형식 처리
    if "T" in date_str:  #
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))  # # Z를 offset으로 명시
        except ValueError:
            pass  # 다른 포맷 시도

    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str, fmt)  #
        except ValueError:
            continue

    logger.warning(f"날짜 문자열 파싱 실패 (지원하지 않는 형식): '{date_str}'")
    return None


async def crawl_board(dept: Department, board_key: str):
    """단일 게시판(학부/대학원) 증분 수집."""
    base_url_dept = dept.url.rstrip("/")  #
    page = 1
    inserted_count = 0
    max_pages_to_crawl = 20  # 무한 루프 방지용 (필요시 조정)

    logger.info(f"📄 [{dept.name} ({board_key})] 공지사항 수집 시작")

    with get_session() as sess:
        last_notice = (sess.query(Notice)
                       .filter_by(dept_id=dept.id, board=board_key)  #
                       .order_by(Notice.post_id.desc())  #
                       .first())
        last_post_id_db = last_notice.post_id if last_notice else "0"  #
        logger.debug(f"[{dept.name} ({board_key})] 마지막으로 수집된 게시글 ID: {last_post_id_db}")

    while page <= max_pages_to_crawl:
        # FIXME: 실제 공지사항 목록 URL 구조에 맞게 수정. page 파라미터 이름 등 확인.
        # 예: /list.do?page=1&boardId=xxx
        board_path = BOARD_CODES.get(board_key)
        if not board_path:
            logger.error(f"[{dept.name}] 유효하지 않은 board_key: {board_key}")
            return

        # list_url = f"{base_url_dept}/{board_path}&page={page}" #
        # URL 조합 시 '?'가 이미 board_path에 있는지, base_url_dept에 이미 query string이 있는지 등을 고려해야 함
        # 좀 더 안전한 방법:
        if '?' in board_path:
            list_url = f"{base_url_dept}/{board_path}&page={page}"
        else:
            list_url = f"{base_url_dept}/{board_path}?page={page}"

        logger.debug(f"페이지 {page} 공지사항 목록 요청: {list_url}")
        posts_data = []
        stop_crawling_current_board = False

        try:  # JSON API 시도
            data = await fetch_json(list_url)  #

            # FIXME: 실제 API 응답에서 게시글 목록을 담고 있는 키로 수정.
            # 예: data.get('result', {}).get('list', [])
            current_page_posts = data.get("posts") if isinstance(data, dict) else data  #
            if not isinstance(current_page_posts, list):
                logger.warning(f"JSON API 응답의 'posts'가 리스트가 아닙니다 ({list_url}). Fallback 시도. 데이터: {str(data)[:200]}")
                raise ValueError("JSON API 응답 형식이 리스트가 아님")

            for p_item in current_page_posts:
                # FIXME: 아래 키들은 실제 API 응답에 맞게 수정되어야 합니다.
                post_id = str(p_item.get("id"))  #
                title = p_item.get("title")  #
                raw_url = p_item.get("url")  #
                date_str = p_item.get("date")  #

                if not all([post_id, title, raw_url, date_str]):
                    logger.warning(f"JSON 항목에 필수 정보 누락 (id, title, url, date): {p_item}")
                    continue

                # 증분 수집 로직: DB의 마지막 ID보다 작거나 같으면 중단
                # 주의: 게시판에 따라 ID가 문자열이거나, 숫자가 아니거나, 순서가 뒤죽박죽일 수 있음.
                # 이 경우, 날짜 기반으로 증분 수집하거나, 더 복잡한 비교 로직 필요.
                try:
                    if post_id.isdigit() and last_post_id_db.isdigit():
                        if int(post_id) <= int(last_post_id_db):
                            stop_crawling_current_board = True
                            break
                    elif post_id <= last_post_id_db:  # 문자열 비교 (일부 경우에만 유효)
                        stop_crawling_current_board = True
                        break
                except ValueError:  # ID가 숫자로 변환 안될 때 (예: '공지', '중요' 등)
                    logger.trace(f"Post ID '{post_id}'는 숫자형이 아님. 증분 비교에서 건너뜀.")
                    pass  # 일단 계속 진행 (최신 글이 아닐 수도 있음)

                posted_at_dt = parse_date_flexible(date_str)
                if not posted_at_dt:
                    logger.warning(f"날짜 파싱 실패로 게시글 건너뜀: ID={post_id}, Date='{date_str}'")
                    continue

                # URL 절대 경로로 변환
                # full_url = raw_url if raw_url.startswith("http") else urljoin(base_url_dept, raw_url) # # base_url_dept 또는 list_url 사용
                full_url = urljoin(list_url, raw_url)  # API/HTML 목록 페이지 URL 기준으로 상대경로 해석

                posts_data.append({  #
                    "dept_id": dept.id,  #
                    "board": board_key,  #
                    "post_id": post_id,  #
                    "title": clean_text(title),  #
                    "url": full_url,  #
                    "posted_at": posted_at_dt  #
                })
            if stop_crawling_current_board:
                logger.info(f"[{dept.name} ({board_key})] 페이지 {page}에서 이전에 수집한 게시글 ID({last_post_id_db})에 도달하여 수집 중단.")
                break

        except (ClientError, json.JSONDecodeError, ValueError, Exception) as e:
            logger.warning(f"JSON API 호출/파싱 실패 ({list_url}): {e}. HTML Fallback 시도.")

            try:  # HTML Fallback
                html = await fetch_text(list_url)  #

                # FIXME: 아래 CSS 선택자들은 웹사이트 HTML 구조 변경 시 반드시 수정되어야 합니다.
                # 게시글 ID, 제목, 링크, 날짜 등을 포함하는 가장 바깥쪽 반복 요소를 먼저 선택하는 것이 안정적일 수 있습니다.
                # 예: notice_item_selector = "table.board_list > tbody > tr"
                # items = soup.select(notice_item_selector)
                # for item_html in items:
                #    post_id = html_first(item_html, "td.no_column_selector")
                #    ...

                # 현재 코드 기반 수정:
                # 게시글 번호 선택자 (예: <td class="no">...</td>)
                ids_selector = "td.no"  #
                # 게시글 제목 선택자 (예: <td class="title"><a>...</a></td>)
                titles_selector = "td.title a"  #
                # 게시글 링크 선택자 (제목과 동일한 <a> 태그의 href 속성)
                links_selector = "td.title a"  #
                # 게시글 날짜 선택자 (예: <td class="date">...</td>)
                dates_selector = "td.date"  #

                post_ids_html = html_select(html, ids_selector)  #
                titles_html = html_select(html, titles_selector)  #
                links_html = html_select(html, links_selector, attr="href")  #
                dates_html = html_select(html, dates_selector)  #

                if not all([post_ids_html, titles_html, links_html, dates_html]):
                    logger.warning(f"HTML에서 일부 필수 정보(ID, 제목, 링크, 날짜)를 찾지 못했습니다. 페이지: {page}, URL: {list_url}")

                min_len = min(len(post_ids_html), len(titles_html), len(links_html), len(dates_html))
                if len(post_ids_html) != min_len or len(titles_html) != min_len or \
                        len(links_html) != min_len or len(dates_html) != min_len:
                    logger.warning(
                        f"HTML에서 추출된 게시글 정보의 개수가 일치하지 않습니다. (IDs: {len(post_ids_html)}, Titles: {len(titles_html)}, Links: {len(links_html)}, Dates: {len(dates_html)}). 최소 개수({min_len})만큼만 처리합니다.")

                for i in range(min_len):
                    post_id = str(post_ids_html[i]).strip()
                    # '공지', '중요' 등의 텍스트 ID 처리
                    if not post_id.isdigit():
                        # 실제 ID가 다른 곳에 있거나, 링크에서 추출해야 할 수 있음.
                        # 여기서는 일단 고유성을 위해 해시값 등으로 대체하거나, 건너뛸 수 있음.
                        # 예: import hashlib; post_id = hashlib.md5(links_html[i].encode()).hexdigest()[:8]
                        logger.trace(f"HTML에서 숫자 아닌 ID 발견: '{post_id}'. 링크 기반으로 ID 대체 시도 또는 건너뜀.")
                        # 또는 링크에서 ID를 추출하는 로직 추가
                        match_id_from_url = re.search(r'postId=(\d+)|articleNo=(\d+)|bbsSn=(\d+)', links_html[i])
                        if match_id_from_url:
                            post_id = next(g for g in match_id_from_url.groups() if g is not None)
                        else:  # 정 ID를 못찾겠으면 제목+날짜로 임시 ID (매우 불안정)
                            # post_id = f"html_{hashlib.md5((titles_html[i] + dates_html[i]).encode()).hexdigest()[:8]}"
                            # 또는 그냥 건너뛰기
                            logger.warning(f"HTML에서 '{post_ids_html[i]}' ID 처리 불가. 게시글 건너뜀: {titles_html[i]}")
                            continue

                    # 증분 수집 로직 (HTML fallback에서도 동일하게 적용)
                    try:
                        if post_id.isdigit() and last_post_id_db.isdigit():
                            if int(post_id) <= int(last_post_id_db):
                                stop_crawling_current_board = True
                                break
                        elif post_id <= last_post_id_db:
                            stop_crawling_current_board = True
                            break
                    except ValueError:
                        logger.trace(f"HTML Post ID '{post_id}'는 숫자형이 아님. 증분 비교에서 건너뜀.")
                        pass

                    title = titles_html[i]
                    raw_url = links_html[i]
                    date_str = dates_html[i]

                    posted_at_dt = parse_date_flexible(date_str)
                    if not posted_at_dt:
                        logger.warning(f"HTML 날짜 파싱 실패로 게시글 건너뜀: ID={post_id}, Date='{date_str}'")
                        continue

                    # full_url = raw_url if raw_url.startswith("http") else urljoin(base_url_dept, raw_url) #
                    full_url = urljoin(list_url, raw_url)  # HTML 목록 페이지 URL 기준으로 상대경로 해석

                    posts_data.append({  #
                        "dept_id": dept.id,  #
                        "board": board_key,  #
                        "post_id": post_id,  #
                        "title": clean_text(title),  #
                        "url": full_url,  #
                        "posted_at": posted_at_dt  #
                    })
                if stop_crawling_current_board:
                    logger.info(
                        f"[{dept.name} ({board_key})] HTML Fallback 페이지 {page}에서 이전에 수집한 게시글 ID({last_post_id_db})에 도달하여 수집 중단.")
                    break

            except (ClientError, Exception) as e_html:
                logger.error(f"HTML Fallback 처리 중 심각한 오류 발생 ({list_url}): {e_html}")
                # 이 경우 해당 페이지는 건너뛰고 다음 페이지로 넘어갈 수 있음
                page += 1
                continue

        if not posts_data and not stop_crawling_current_board:  # 현재 페이지에서 아무것도 못가져왔고, 증분 중단도 아니면
            logger.info(f"[{dept.name} ({board_key})] 페이지 {page}에서 더 이상 가져올 게시글이 없습니다.")
            break

        if posts_data:
            try:
                with get_session() as sess:
                    sess.bulk_insert_mappings(Notice, posts_data)  #
                    sess.commit()  #
                inserted_count += len(posts_data)
                logger.debug(f"[{dept.name} ({board_key})] 페이지 {page}에서 {len(posts_data)}건 DB 저장 완료.")
            except Exception as e_db:
                logger.opt(exception=True).error(f"[{dept.name} ({board_key})] 공지사항 DB 저장 중 오류: {e_db}")
                # 일부 저장 실패시 rollback 고려

        if stop_crawling_current_board:  # 이미 위에서 break 했지만, 명시적으로 한 번 더
            break

        page += 1

    if inserted_count > 0:
        logger.success(f"📄 [{dept.name} ({board_key})] 새 공지 총 {inserted_count}건 수집 완료.")
    else:
        logger.info(f"📄 [{dept.name} ({board_key})] 새로운 공지사항이 없습니다.")


async def crawl_department_notices(dept: Department):
    for board_key in BOARD_CODES:  #
        try:
            await crawl_board(dept, board_key)  #
        except Exception as e:
            logger.opt(exception=True).error(f"[{dept.name} ({board_key})] 게시판 크롤링 중 예외 발생: {e}")