# src/cnu_crawler/spiders/manual_linker.py (새 파일)
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from loguru import logger

from cnu_crawler.storage import College, Department, get_session
from cnu_crawler.config import MANUAL_NOTICE_LINKS_FILE
from cnu_crawler.utils import clean_text


def _parse_manual_line(line: str) -> Optional[Tuple[str, str, str]]:
    """TXT 파일의 한 줄을 파싱하여 (대학명, 학과/대학원명, URL) 튜플로 반환합니다."""
    parts = [part.strip() for part in line.split(',')]
    if len(parts) == 3:
        college_name = clean_text(parts[0])
        dept_or_grad_name = clean_text(parts[1]) if parts[1] != '-' else None
        url = parts[2]
        if college_name and url:  # 대학명과 URL은 필수
            return college_name, dept_or_grad_name, url
    logger.warning(f"잘못된 형식의 라인 건너뜀: {line}")
    return None


def _create_url_template(url: str) -> str:
    """주어진 URL을 페이지 파라미터 플레이스홀더를 포함하는 템플릿으로 변환합니다."""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    # 기존 페이지 파라미터 (page, p, pageNo 등) 제거
    common_page_params = ['page', 'p', 'pageNo', 'pageNum', 'pg', 'start']
    for p_key in common_page_params:
        query_params.pop(p_key, None)

    # 새 페이지 파라미터 {page} 추가 (또는 적절한 이름 사용)
    # 여기서는 'page'를 기본으로 사용
    # query_params['page'] = ['{page}'] # 이렇게 하면 urlencode 시 {page}가 인코딩될 수 있음

    # 기존 쿼리 문자열을 유지하고, page={page}를 추가하거나 대체하는 것이 더 안전할 수 있음
    # 여기서는 단순화를 위해 기존 쿼리를 제거하고 page={page}만 사용하거나,
    # 기존 쿼리가 없다면 ?page={page}, 있다면 &page={page}를 붙임

    # 가장 간단한 방식: URL에 ?가 없으면 ?page={}, 있으면 &page={} 추가
    # (이미 페이지 파라미터가 있는 경우 중복될 수 있으므로 주의)
    if parsed_url.query and any(pp in parsed_url.query for pp in common_page_params):
        # 이미 페이지 파라미터가 있는 경우, 해당 부분을 {page}로 대체하는 정교한 로직 필요
        # 여기서는 일단 기존 URL을 최대한 유지하고, 만약 page 파라미터가 이미 있다면 그 부분을
        # 활용하도록 get_notice_list_url 함수에 맡기고, 원본 URL을 저장할 수도 있음.
        # 또는, 페이지 파라미터를 제거하고 ?page={page}를 강제로 붙임.
        new_query = urlencode(query_params, doseq=True)
        base_url_for_template = urlunparse(
            (parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, new_query, ''))
        if "?" in base_url_for_template and not base_url_for_template.endswith("?"):
            return base_url_for_template + "&page={page}"
        elif "?" not in base_url_for_template:
            return base_url_for_template + "?page={page}"
        else:  # ?로 끝나는 경우
            return base_url_for_template + "page={page}"

    else:  # 페이지 파라미터가 아예 없는 URL
        return url.rstrip('/') + "?page={page}"


async def update_department_notice_urls_from_file():
    """MANUAL_NOTICE_LINKS_FILE을 읽어 Department의 공지사항 URL 템플릿을 업데이트합니다."""
    if not MANUAL_NOTICE_LINKS_FILE.exists():
        logger.warning(f"수동 공지사항 링크 파일({MANUAL_NOTICE_LINKS_FILE})을 찾을 수 없습니다. 업데이트를 건너뜁니다.")
        return

    logger.info(f"'{MANUAL_NOTICE_LINKS_FILE}' 파일에서 수동 공지사항 링크를 읽어 DB 업데이트 시작...")
    updated_count = 0
    not_found_count = 0

    with get_session() as sess:
        with open(MANUAL_NOTICE_LINKS_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):  # 빈 줄이나 주석 건너뛰기
                    continue

                parsed_data = _parse_manual_line(line)
                if not parsed_data:
                    logger.warning(f"라인 {line_num}: 파싱 실패 - '{line}'")
                    continue

                college_name_txt, dept_or_grad_name_txt, url_txt = parsed_data

                # 1. College 찾기
                college_obj = sess.query(College).filter(College.name == college_name_txt).first()
                if not college_obj:
                    # College 이름에 "(일반대학원소속)" 등이 붙어있을 수 있으므로, 부분 일치도 고려 가능
                    # 예: College.name.like(f"%{college_name_txt}%")
                    # 여기서는 정확히 일치하는 경우만 처리
                    logger.warning(f"라인 {line_num}: College '{college_name_txt}'를 DB에서 찾을 수 없습니다.")
                    not_found_count += 1
                    continue

                # 2. Department 찾기
                dept_obj: Optional[Department] = None
                target_dept_name = dept_or_grad_name_txt

                # 학과명이 '-' 이거나 없는 경우: College 자체의 공지사항으로 간주 (이런 경우는 드뭄)
                # 또는, College 이름 + " 전체 공지" 등으로 Department를 만들거나,
                # College 객체에 직접 공지 URL 템플릿 필드를 추가하는 것을 고려.
                # 현재 Department 모델은 College에 종속되므로, '-'를 학과명으로 갖는 Department를 찾아야 함.
                # 여기서는 dept_or_grad_name_txt가 있는 경우만 처리.
                if not target_dept_name:  # 학과명/대학원명이 '-' 또는 비어있는 경우
                    # 이 경우, College 객체에 직접 URL을 저장하거나,
                    # 'college_main_notice'와 같은 특별한 Department를 만들어야 함.
                    # 현재 로직에서는 건너뜀. 필요시 이 부분 확장.
                    logger.info(
                        f"라인 {line_num}: 학과명/대학원명이 지정되지 않았습니다 ('{college_name_txt}', '{url_txt}'). College 레벨 공지로 처리하려면 별도 로직 필요.")
                    continue

                dept_obj = sess.query(Department).filter(
                    Department.college_id == college_obj.id,
                    Department.name == target_dept_name
                ).first()

                if not dept_obj:
                    logger.warning(
                        f"라인 {line_num}: Department '{target_dept_name}' (College: '{college_name_txt}')를 DB에서 찾을 수 없습니다.")
                    # 유사 이름 검색 또는 신규 Department 생성 로직 추가 가능 (주의)
                    not_found_count += 1
                    continue

                # 3. URL 템플릿 생성 및 Department 객체 업데이트
                url_template = _create_url_template(url_txt)

                # 어떤 종류의 공지사항 URL인지 결정 필요 (학사, 학부, 대학원)
                # TXT 파일에 명시되지 않았으므로, 휴리스틱 또는 기본값 사용
                # 예: 이름에 "대학원"이 포함되면 grad_notice_url_template에 저장
                #     아니면 undergrad_notice_url_template에 저장
                #     "학사" 관련 키워드가 URL이나 이름에 있으면 academic_notice_url_template

                target_template_field = None
                if "학사" in target_dept_name or "academic" in url_txt.lower():
                    target_template_field = "academic_notice_url_template"
                elif "대학원" in target_dept_name or dept_obj.dept_type in ["grad_school_dept", "plus_special_grad_dept",
                                                                         "plus_general_grad_dept"]:
                    target_template_field = "grad_notice_url_template"
                else:  # 기본적으로 학부 공지로 간주
                    target_template_field = "undergrad_notice_url_template"

                current_template_val = getattr(dept_obj, target_template_field)
                if current_template_val != url_template:
                    setattr(dept_obj, target_template_field, url_template)
                    logger.info(
                        f"Department '{dept_obj.name}' ({college_obj.name})의 '{target_template_field}'를 '{url_template}' (원본 URL: {url_txt}) (으)로 업데이트합니다.")
                    updated_count += 1
                else:
                    logger.trace(
                        f"Department '{dept_obj.name}' ({college_obj.name})의 '{target_template_field}'는 이미 '{url_template}'입니다.")

        if updated_count > 0:
            try:
                sess.commit()
                logger.success(
                    f"수동 공지사항 링크 파일 처리 완료: {updated_count}개 Department 업데이트, {not_found_count}개 항목 DB 매칭 실패.")
            except Exception as e:
                logger.error(f"수동 공지사항 링크 DB 커밋 중 오류: {e}")
                sess.rollback()
        else:
            logger.info(f"수동 공지사항 링크 파일 처리 완료: 업데이트된 Department 없음, {not_found_count}개 항목 DB 매칭 실패.")