# src/cnu_crawler/spiders/__init__.py
from .colleges import discover_all_colleges_entrypoint
#from .departments import crawl_departments
from .notices import crawl_department_notices
from .manual_linker import process_manual_links_file  # 추가

__all__ = [
    "discover_all_colleges_entrypoint",
    #"crawl_departments",
    "crawl_department_notices",
    "process_manual_links_file",
]