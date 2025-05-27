"""
cnu_crawler 패키지 루트 초기화.

외부에서 `import cnu_crawler as cnu` 로 불러올 때
config, core, spiders, storage 네 가지 서브패키지를
바로 쓸 수 있도록 재-export 합니다.
"""
from importlib.metadata import PackageNotFoundError, version

# ──────────────────────────────
# 버전 정보
# ──────────────────────────────
try:
    __version__: str = version(__name__)
except PackageNotFoundError:           # 로컬 개발·배포 전용
    __version__ = "0.0.0-dev"

# ──────────────────────────────
# 서브패키지 re-export
# ──────────────────────────────
from . import config, core, spiders, storage     # noqa: E402  (순환 import X)

__all__ = [
    "__version__",
    "config",
    "core",
    "spiders",
    "storage",
]
