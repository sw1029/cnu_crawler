# src/cnu_crawler/utils.py
import re
from datetime import datetime, timezone, timedelta
from typing import Optional  # Optional 타입을 위해 추가
from loguru import logger  # 로깅을 위해 추가

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def now_kst() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))


def iso_now() -> str:
    return now_kst().strftime(ISO_FORMAT)


def clean_text(txt: str) -> str:
    txt_cleaned = re.sub(r"\s+", " ", txt).strip()
    # 추가적으로 HTML 엔티티 코드 변환 등이 필요하면 여기에 추가
    # 예: import html; txt_cleaned = html.unescape(txt_cleaned)
    return txt_cleaned


def parse_date_flexible(date_str: str) -> Optional[datetime]:
    """다양한 일반적인 날짜 문자열 형식을 파싱하여 datetime 객체로 변환합니다."""
    if not date_str or not isinstance(date_str, str):
        return None

    cleaned_date_str = date_str.strip()

    # 시도할 날짜 형식 목록 (우선순위 순서대로)
    # datetime.fromisoformat이 처리할 수 있는 대부분의 ISO 8601 형식을 먼저 시도
    if "T" in cleaned_date_str:
        try:
            # 'Z'를 UTC 오프셋으로 명시적 변환 (fromisoformat은 Python 3.11+ 부터 'Z' 직접 지원)
            # Python 3.7-3.10 호환성을 위해 +00:00으로 대체
            if cleaned_date_str.endswith("Z"):
                cleaned_date_str_iso = cleaned_date_str[:-1] + "+00:00"
            else:
                cleaned_date_str_iso = cleaned_date_str
            return datetime.fromisoformat(cleaned_date_str_iso)
        except ValueError:
            pass  # 다른 형식 시도

    # datetime.strptime으로 시도할 형식들
    formats_to_try = [
        "%Y-%m-%d %H:%M:%S",
        "%Y.%m.%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",  # 시간까지만 있는 경우
        "%Y.%m.%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y/%m/%d",
        "%y-%m-%d %H:%M:%S",  # 두 자리 연도
        "%y.%m.%d %H:%M:%S",
        "%y/%m/%d %H:%M:%S",
        "%y-%m-%d",
        "%y.%m.%d",
        "%y/%m/%d",
        # 필요시 다른 일반적인 형식 추가
    ]

    for fmt in formats_to_try:
        try:
            return datetime.strptime(cleaned_date_str, fmt)
        except ValueError:
            continue

    # 'T'가 없는 ISO 형식 문자열도 fromisoformat으로 시도 (예: '2023-10-25')
    # 이미 위 strptime에서 '%Y-%m-%d' 등으로 처리되지만, 더 넓은 범위의 ISO 변형 포괄 가능
    try:
        return datetime.fromisoformat(cleaned_date_str)
    except ValueError:
        pass

    logger.warning(f"날짜 문자열 파싱 실패 (지원하지 않는 형식 또는 잘못된 값): '{date_str}'")
    return None