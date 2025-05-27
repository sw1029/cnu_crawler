"""
spiders 서브패키지

단계별 크롤러(spider) 모듈을 바로 import 할 수 있도록
얇은 래퍼만 제공합니다.
"""

from .colleges import discover_all_colleges_entrypoint
from .departments import crawl_departments
from .notices import crawl_department_notices

__all__ = [
    "discover_all_colleges_entrypoint",
    "crawl_departments",
    "crawl_department_notices",
]
