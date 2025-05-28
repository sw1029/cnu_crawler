# src/cnu_crawler/spiders/manual_linker.py (또는 txt_processor.py)
import re
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from loguru import logger

from cnu_crawler.storage import College, Department, get_session
from cnu_crawler.config import MANUAL_NOTICE_LINKS_FILE
from cnu_crawler.utils import clean_text


# ... (_parse_txt_line, _create_url_template_from_link 함수는 이전과 동일) ...

def _parse_txt_line(line: str) -> Optional[Tuple[str, Optional[str], str]]:
    """TXT 파일의 한 줄을 파싱하여 (대학명, 학과/대학원명, 공지사항링크) 튜플로 반환합니다."""
    parts = [part.strip() for part in line.split(',')]
    if len(parts) == 3:
        college_name = clean_text(parts[0])
        dept_or_grad_name = clean_text(parts[1]) if parts[1] != '-' else None
        url = parts[2].strip()
        if college_name and url:
            return college_name, dept_or_grad_name, url
    logger.warning(f"잘못된 형식의 TXT 라인 건너뜀: {line}")
    return None


def _create_url_template_from_link(link_url: str) -> str:
    """주어진 공지사항 목록 링크 URL을 페이지 파라미터 플레이스홀더를 포함하는 템플릿으로 변환합니다."""
    parsed_url = urlparse(link_url)
    query_params = parse_qs(parsed_url.query)
    common_page_params = ['page', 'p', 'pageNo', 'pageNum', 'pg', 'start', 'currentPage']
    for p_key in common_page_params:
        query_params.pop(p_key, None)
    page_param_for_template = "page"
    new_query_string = urlencode(query_params, doseq=True)
    base_url_for_template = urlunparse((
        parsed_url.scheme, parsed_url.netloc, parsed_url.path,
        parsed_url.params, new_query_string, ''
    ))
    if "?" in base_url_for_template and not base_url_for_template.endswith("?"):
        return base_url_for_template + f"&{page_param_for_template}={{page}}"
    elif "?" not in base_url_for_template:
        return base_url_for_template + f"?{page_param_for_template}={{page}}"
    else:
        return base_url_for_template + f"{page_param_for_template}={{page}}"


def _generate_department_code_from_txt(college_code: str, dept_name_or_placeholder: str, notice_url: str) -> str:
    """TXT 정보 기반으로 Department 코드를 생성합니다."""
    name_part = re.sub(r'\s+', '', dept_name_or_placeholder.lower())
    name_alnum = re.sub(r'[^a-z0-9]', '', name_part)[:15]

    parsed_url = urlparse(notice_url)
    path_end_part = parsed_url.path.split('/')[-1][:10] if parsed_url.path.split('/')[-1] else "na"

    base_code = f"{college_code[:10]}_{name_alnum}_{path_end_part}"

    # hash() 결과를 문자열로 변환 후 슬라이싱
    hash_str_part = str(hash(notice_url + dept_name_or_placeholder)).replace('-', '')[:6]

    return f"txt_dept_{base_code}_{hash_str_part}"[:50]  # 수정된 부분


async def process_manual_links_file():
    """
    MANUAL_NOTICE_LINKS_FILE을 읽어 Department 정보를 생성/업데이트하고,
    공지사항 URL 템플릿을 설정합니다.
    """
    if not MANUAL_NOTICE_LINKS_FILE.exists():
        logger.warning(f"수동 공지사항 링크 파일({MANUAL_NOTICE_LINKS_FILE})을 찾을 수 없습니다. Department 생성을 건너뜁니다.")
        return

    logger.info(f"'{MANUAL_NOTICE_LINKS_FILE}' 파일에서 Department 정보 생성/업데이트 시작...")
    created_count = 0
    updated_count = 0
    skipped_college_not_found = 0

    with get_session() as sess:
        with open(MANUAL_NOTICE_LINKS_FILE, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
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

                dept_name_to_use = ""
                dept_url_to_use = ""
                dept_type_to_use = "txt_manual"  # 기본 타입

                if not dept_name_txt_opt:  # 학과명이 '-' 또는 None인 경우
                    dept_name_to_use = college_obj.name + " (대표공지)"  # College 이름을 기반으로 학과명 생성
                    dept_url_to_use = college_obj.url  # College URL을 Department URL로 사용
                    # 이 경우 dept_type을 다르게 설정하여 구분 가능
                    dept_type_to_use = college_obj.college_type + "_main_notice_dept"
                else:
                    dept_name_to_use = dept_name_txt_opt
                    # Department URL: TXT의 notice_link_txt에서 scheme+netloc+첫번째경로 사용
                    parsed_notice_link = urlparse(notice_link_txt)
                    dept_url_path_segment = "/" + parsed_notice_link.path.strip('/').split('/')[
                        0] if parsed_notice_link.path.strip('/') else "/"
                    dept_url_to_use = urlunparse(
                        (parsed_notice_link.scheme, parsed_notice_link.netloc, dept_url_path_segment, '', '', ''))

                # college_obj.code는 InstrumentedAttribute가 아닌 실제 문자열 값을 사용해야 함
                college_code_str = str(college_obj.code)
                dept_code_to_use = _generate_department_code_from_txt(college_code_str, dept_name_to_use,
                                                                      notice_link_txt)  # college_obj.code 전달

                department_obj = sess.query(Department).filter_by(
                    college_id=college_obj.id,
                    code=dept_code_to_use
                ).one_or_none()

                url_template = _create_url_template_from_link(notice_link_txt)

                target_template_field_name = "undergrad_notice_url_template"
                if "대학원" in dept_name_to_use or \
                        (department_obj and department_obj.dept_type in ["grad_school_dept", "plus_special_grad_dept",
                                                                         "plus_general_grad_dept"]):
                    target_template_field_name = "grad_notice_url_template"
                if "학사" in dept_name_to_use or "academic" in notice_link_txt.lower() or "haksa" in notice_link_txt.lower():
                    target_template_field_name = "academic_notice_url_template"

                if department_obj:
                    changed = False
                    if getattr(department_obj, target_template_field_name, None) != url_template:
                        setattr(department_obj, target_template_field_name, url_template)
                        changed = True
                    if department_obj.name != dept_name_to_use: department_obj.name = dept_name_to_use; changed = True
                    if department_obj.url != dept_url_to_use: department_obj.url = dept_url_to_use; changed = True
                    if department_obj.dept_type != dept_type_to_use: department_obj.dept_type = dept_type_to_use; changed = True
                    if changed:
                        logger.info(
                            f"Department '{dept_name_to_use}' ({college_obj.name}) 정보 업데이트. '{target_template_field_name}' 설정: {url_template}")
                        updated_count += 1
                else:
                    new_dept_data = {
                        "college_id": college_obj.id, "code": dept_code_to_use,
                        "name": dept_name_to_use, "url": dept_url_to_use,
                        "dept_type": dept_type_to_use,
                        target_template_field_name: url_template
                    }
                    # 다른 템플릿 필드도 초기화 (None)
                    for t_field in ["academic_notice_url_template", "undergrad_notice_url_template",
                                    "grad_notice_url_template", "specific_grad_keyword_notice_url"]:
                        if t_field not in new_dept_data:
                            new_dept_data[t_field] = None

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