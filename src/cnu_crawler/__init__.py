# ✅ 수정된 __init__.py
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

__all__ = [
    "__version__",
    # "config", "core", "spiders", "storage"  ← 제거
]
