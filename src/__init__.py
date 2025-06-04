"""CNU Notice Crawler package"""

from .search.index_links import update_index
from .search.query_links import search_links

__all__ = ["update_index", "search_links"]
