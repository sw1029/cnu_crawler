"""Backward compatibility wrapper for cnu_crawler package"""

from cnu_crawler.search.index_links import update_index
from cnu_crawler.search.query_links import search_links

__all__ = ["update_index", "search_links"]
