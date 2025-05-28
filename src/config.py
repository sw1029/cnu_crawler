from pathlib import Path

# ── 파일·폴더 경로 ────────────────────────────────────────────
ROOT_DIR   = Path(__file__).resolve().parent.parent
DATA_DIR   = ROOT_DIR / "data"
CSV_DIR    = DATA_DIR / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)
LINKS_FILE = DATA_DIR / "links.txt"

# ── 크롤링 공통 설정 ──────────────────────────────────────────
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; CNUNoticeBot/0.1; +https://github.com/your-handle)"
    )
}
REQUEST_TIMEOUT = 10  # sec
ENCODING_FALLBACKS = ["utf-8", "euc-kr", "cp949"]
