"""
질문 텍스트를 받아 FAISS로 가장 가까운 공지 1개를 찾고,
해당 URL을 Scraper로 재크롤링해 최신 내용을 반환
"""
import importlib
import pickle
from typing import Tuple

import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer

from ..config import CSV_DIR
from ..scraper.base import ScraperBase
from ..utils import load_links

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
INDEX_FILE = CSV_DIR / "faiss.index"
META_FILE  = CSV_DIR / "metadata.pkl"


def _load_faiss() -> Tuple[faiss.Index, pd.DataFrame]:
    if not INDEX_FILE.exists() or not META_FILE.exists():
        raise RuntimeError("Index not found. 먼저 indexer.py를 실행하세요.")
    index = faiss.read_index(str(INDEX_FILE))
    meta: pd.DataFrame = pickle.loads(META_FILE.read_bytes())
    return index, meta


def _get_scraper_for_url(url: str) -> ScraperBase:
    from urllib.parse import urlparse

    # links.txt에서 정의된 매핑을 재활용
    for college, dept, link_url in load_links(CSV_DIR.parent / "links.txt"):
        if url.startswith(link_url.split("/")[2]):  # 도메인 비교
            from ..pipeline import _get_scraper_class

            scraper_cls = _get_scraper_class(link_url)
            return scraper_cls(college, dept, link_url)
    raise RuntimeError(f"No scraper mapping found for url: {url}")


def query(text: str, k: int = 1):
    model = SentenceTransformer(MODEL_NAME)
    index, meta = _load_faiss()

    emb = model.encode([text])
    D, I = index.search(emb.astype("float32"), k)
    for idx in I[0]:
        row = meta.iloc[idx]
        url = row["url"]
        print(f"[FAISS] {row['college']} / {row['dept']} → {row['title']}")
        scraper = _get_scraper_for_url(url)
        latest_df = scraper.scrape()
        print(latest_df.head(5)[["title", "posted_at", "url"]])


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.search.query \"장학금 신청\"")
        sys.exit(0)
    query(" ".join(sys.argv[1:]))
