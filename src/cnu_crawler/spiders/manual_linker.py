# src/cnu_crawler/spiders/txt_processor.py (새 파일 또는 manual_linker.py 대체)
import re  # _generate_department_code_from_txt 에서 사용
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from loguru import logger

from cnu_crawler.storage import College, Department, get_session  #
from cnu_crawler.config import MANUAL_NOTICE_LINKS_FILE  #
from cnu_crawler.utils import clean_text  #


def _parse_txt_line(line: str) -> Optional[Tuple[str, Optional[str], str]]:
    """TXT 파일의 한 줄을 파싱하여 (대학명, 학과/대학원명, 공지사항링크) 튜플로 반환합니다."""
    parts = [part.strip() for part in line.split(',')]
    if len(parts) == 3:
        college_name = clean_text(parts[0])
        # 학과명 '-' 처리: None으로 변환
        dept_or_grad_name = clean_text(parts[1]) if parts[1] != '-' else None
        url = parts[2].strip()  # URL은 공백 제거만
        if college_name and url:
            return college_name, dept_or_grad_name, url
    logger.warning(f"잘못된 형식의 TXT 라인 건너뜀: {line}")
    return None


def _create_url_template_from_link(link_url: str) -> str:
    """
    주어진 공지사항 목록 링크 URL을 페이지 파라미터 플레이스홀더를 포함하는 템플릿으로 변환합니다.
    예: https://example.com/notice.do?page=1 -> https://example.com/notice.do?page={page}
        https://example.com/notice/list -> https://example.com/notice/list?page={page}
    """
    parsed_url = urlparse(link_url)
    query_params = parse_qs(parsed_url.query)

    # 일반적인 페이지 파라미터 이름들
    common_page_params = ['page', 'p', 'pageNo', 'pageNum', 'pg', 'start', 'currentPage']

    # 기존 페이지 파라미터 제거 (더 일반적인 {page} 플레이스홀더 사용 위함)
    # 또는, 특정 페이지 파라미터가 있다면 그 이름을 {page}로 대체하는 방식도 가능
    for p_key in common_page_params:
        query_params.pop(p_key, None)

    # {page} 플레이스홀더를 위한 파라미터 이름 (예: 'page')
    page_param_for_template = "page"

    # 기존 쿼리 문자열을 유지하면서 page={page} 추가/대체하기보다,
    # 명시적으로 page={page} 만 남기거나, 쿼리가 없으면 ?page={page} 추가
    # 여기서는 기존 쿼리를 보존하고 page={page}를 추가 (중복 가능성 있음, 더 정교한 처리 필요)

    # 현재는 단순하게 ?page={page} 또는 &page={page} 를 붙이는 형태로 가정
    # 더 나은 방법은 URL 템플릿을 TXT 파일에 직접 명시하는 것일 수 있음 (예: ...notice.do?pg={page})

    # 기존 쿼리를 유지하지 않고, ?page={page} 또는 &page={page}만 붙이는 방식
    # path_only_url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, '', ''))
    # return path_only_url + "?page={page}"

    # 현재 URL에서 페이지 파라미터를 제거하고, '{page}'를 사용하는 템플릿으로 만듦
    new_query_string = urlencode(query_params, doseq=True)
    base_url_for_template = urlunparse((
        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
        parsed_url.params, new_query_string, ''  # fragment 제거
    ))

    if "?" in base_url_for_template and not base_url_for_template.endswith("?"):
        return base_url_for_template + f"&{page_param_for_template}={{page}}"
    elif "?" not in base_url_for_template:
        return base_url_for_template + f"?{page_param_for_template}={{page}}"
    else:  # ?로 끝나는 경우
        return base_url_for_template + f"{page_param_for_template}={{page}}"


def _generate_department_code_from_txt(college_code: str, dept_name_or_placeholder: str, notice_url: str) -> str:
    """TXT 정보 기반으로 Department 코드를 생성합니다."""
    name_part = re.sub(r'\s+', '', dept_name_or_placeholder.lower())
    name_alnum = re.sub(r'[^a-z0-9]', '', name_part)[:15]

    # URL에서 일부를 가져와 코드에 반영 (고유성 증대 목적)
    parsed_url = urlparse(notice_url)
    path_end_part = parsed_url.path.split('/')[-1][:10] if parsed_url.path.split('/')[-1] else "na"

    base_code = f"{college_code[:10]}_{name_alnum}_{path_end_part}"
    return f"txt_dept_{base_code}_{hash(notice_url + dept_name_or_placeholder)[:6]}"[:50]


async def process_manual_links_file():
    """
    MANUAL_NOTICE_LINKS_FILE을 읽어 Department 정보를 생성/업데이트하고,
    공지사항 URL 템플릿을 설정합니다.
    """
    if not MANUAL_NOTICE_LINKS_FILE.exists():  #
        logger.warning(f"수동 공지사항 링크 파일({MANUAL_NOTICE_LINKS_FILE})을 찾을 수 없습니다. Department 생성을 건너뜁니다.")
        return

    logger.info(f"'{MANUAL_NOTICE_LINKS_FILE}' 파일에서 Department 정보 생성/업데이트 시작...")
    created_count = 0
    updated_count = 0
    skipped_college_not_found = 0

    with get_session() as sess:  #
        with open(MANUAL_NOTICE_LINKS_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):  # 주석이나 빈 줄 건너뛰기
                    continue

                parsed_data = _parse_txt_line(line)
                if not parsed_data:
                    logger.warning(f"라인 {line_num}: 파싱 실패 - '{line}'")
                    continue

                college_name_txt, dept_name_txt_opt, notice_link_txt = parsed_data

                college_obj = sess.query(College).filter(College.name == college_name_txt).first()
                if not college_obj:
                    logger.warning(f"라인 {line_num}: College '{college_name_txt}'를 DB에서 찾을 수 없습니다. 해당 라인 건너뜁니다.")
                    skipped_college_not_found += 1
                    continue

                # 학과명이 '-' 또는 None인 경우, College 이름을 기반으로 대표 Department 생성 시도
                # 또는, College 자체에 대한 공지 링크로 처리 (모델 수정 필요)
                # 여기서는 학과명이 있는 경우만 처리 (요구사항에 따라 수정 가능)
                if not dept_name_txt_opt:
                    # 예시: College 이름을 Department 이름으로 사용하고, 코드는 College 코드 기반으로 생성
                    dept_name_to_use = college_obj.name + " (대표)"  # 또는 college_obj.name
                    dept_code_to_use = _generate_department_code_from_txt(college_obj.code, dept_name_to_use,
                                                                          notice_link_txt)
                    dept_url_to_use = college_obj.url  # College URL을 Department URL로 사용
                    dept_type_to_use = college_obj.college_type + "_main_notice"  # 새로운 dept_type
                    logger.info(
                        f"라인 {line_num}: 학과명 없음. College '{college_name_txt}'의 대표 공지로 처리 시도 (학과명: '{dept_name_to_use}').")
                else:
                    dept_name_to_use = dept_name_txt_opt
                    # Department URL은 TXT의 notice_link_txt의 base URL로 할지, 아니면 별도 관리할지 정책 필요
                    # 여기서는 notice_link_txt의 base를 Department URL로 사용 (단, 공지사항 목록이 아닌 학과 메인 URL이 더 적합)
                    # 지금은 학과 URL을 TXT에서 직접 제공하지 않으므로, College URL을 임시 사용하거나,
                    # notice_link_txt의 도메인만 사용.
                    # 더 좋은 방법은 TXT에 학과 URL도 명시하는 것.
                    # 여기서는 notice_link_txt의 scheme + netloc + 첫번째 경로까지만 dept URL로 가정
                    parsed_notice_link = urlparse(notice_link_txt)
                    dept_url_path = "/" + parsed_notice_link.path.strip('/').split('/')[
                        0] if parsed_notice_link.path.strip('/') else "/"
                    dept_url_to_use = urlunparse(
                        (parsed_notice_link.scheme, parsed_notice_link.netloc, dept_url_path, '', '', ''))

                    dept_code_to_use = _generate_department_code_from_txt(college_obj.code, dept_name_to_use,
                                                                          notice_link_txt)
                    dept_type_to_use = "txt_manual"  # TXT에서 온 학과임을 표시

                # Department 객체 조회 또는 생성
                department_obj = sess.query(Department).filter_by(
                    college_id=college_obj.id,
                    code=dept_code_to_use  # 생성된 코드로 조회
                ).one_or_none()

                url_template = _create_url_template_from_link(notice_link_txt)

                # 어떤 종류의 공지사항 URL 템플릿에 저장할지 결정
                # 예: 학과명에 "대학원" 포함 시 grad_notice_url_template, 아니면 undergrad_notice_url_template
                #     "학사" 키워드가 URL이나 학과명에 있으면 academic_notice_url_template
                # TXT 파일에 게시판 유형도 명시하면 더 정확해짐 (예: 대학명,학과명,게시판유형,링크)
                # 현재는 휴리스틱 기반으로 undergrad 또는 grad에 저장
                target_template_field_name = "undergrad_notice_url_template"
                if "대학원" in dept_name_to_use or \
                        (dept_obj and dept_obj.dept_type in ["grad_school_dept", "plus_special_grad_dept",
                                                             "plus_general_grad_dept"]):
                    target_template_field_name = "grad_notice_url_template"

                # "학사" 키워드 우선 (URL 또는 학과명에 포함 시)
                if "학사" in dept_name_to_use or "academic" in notice_link_txt.lower() or "haksa" in notice_link_txt.lower():
                    target_template_field_name = "academic_notice_url_template"

                if department_obj:  # 기존 Department 업데이트
                    changed = False
                    if getattr(department_obj, target_template_field_name) != url_template:
                        setattr(department_obj, target_template_field_name, url_template)
                        changed = True
                    # 필요시 이름, URL, dept_type 등도 TXT 정보로 업데이트
                    if department_obj.name != dept_name_to_use: department_obj.name = dept_name_to_use; changed = True
                    if department_obj.url != dept_url_to_use: department_obj.url = dept_url_to_use; changed = True  # 학과 대표 URL 업데이트
                    if department_obj.dept_type != dept_type_to_use: department_obj.dept_type = dept_type_to_use; changed = True

                    if changed:
                        logger.info(
                            f"Department '{dept_name_to_use}' ({college_obj.name}) 정보 업데이트. '{target_template_field_name}' 설정: {url_template}")
                        updated_count += 1
                else:  # 새 Department 생성
                    new_dept_data = {
                        "college_id": college_obj.id,
                        "code": dept_code_to_use,
                        "name": dept_name_to_use,
                        "url": dept_url_to_use,  # 학과 대표 URL
                        "dept_type": dept_type_to_use,
                        target_template_field_name: url_template
                    }
                    department_obj = Department(**new_dept_data)
                    sess.add(department_obj)
                    logger.info(
                        f"새 Department '{dept_name_to_use}' ({college_obj.name}) 생성. '{target_template_field_name}' 설정: {url_template}")
                    created_count += 1

        if created_count > 0 or updated_count > 0:
            try:
                sess.commit()
                logger.success(
                    f"TXT 파일 기반 Department 정보 처리 완료: {created_count}개 생성, {updated_count}개 업데이트. College 매칭 실패: {skipped_college_not_found}건.")
            except Exception as e:
                logger.error(f"TXT 파일 기반 Department 정보 DB 커밋 중 오류: {e}")
                sess.rollback()
        else:
            logger.info(
                f"TXT 파일 기반 Department 정보 처리 완료: 생성/업데이트된 Department 없음. College 매칭 실패: {skipped_college_not_found}건.")