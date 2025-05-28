# ──────────────────────────────────────────────────────────────
#  src/search/query_links.py
#  자연어 질의 → links.txt 인덱스(FAISS) → 최적 링크 → GenericScraper
#  실행:  python -m src.search.query_links "화학과 공지 알려줘"
# ──────────────────────────────────────────────────────────────
import os, sys, re, difflib, pickle
from pathlib import Path
from typing import List, Dict

# ❌ TensorFlow/Keras 불필요하므로 로딩 차단
os.environ["TRANSFORMERS_NO_TF"] = "1"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer

from ..config import DATA_DIR
from ..scraper.generic import GenericScraper

# ── 설정 ──────────────────────────────────────────────────────
MODEL_NAME = "jhgan/ko-sroberta-multitask"        # 한국어 SBERT
INDEX_FILE = DATA_DIR / "link_index.faiss"
META_FILE  = DATA_DIR / "link_meta.pkl"
TOP_K_FAISS = 5       # 1차 후보 수
SHOW_ROWS   = 10      # 출력할 공지행 개수

# 동의어(질의 토큰 ↔ 학과 키워드) 사전
SYNONYMS = {
    "ai": "인공지능",
    "응용화학": "화학",
    "화공": "화학",
    "컴퓨터": "컴퓨터공학",
}

# ── 유틸 ───────────────────────────────────────────────────────
def load_index():
    if not INDEX_FILE.exists():
        raise RuntimeError("FAISS 인덱스가 없습니다. 먼저 index_links.py 실행하세요.")
    index = faiss.read_index(str(INDEX_FILE))
    meta  = pickle.loads(META_FILE.read_bytes())
    return index, meta

def normalize(text: str) -> str:
    """한글/숫자/영문만 남기고 소문자화, 공백 제거"""
    text = text.lower()
    text = re.sub(r"[^0-9a-z가-힣]", "", text)
    return text

def token_set(text: str) -> set:
    """학과·단과 명을 음절 단위 토큰 세트로"""
    return set(normalize(text))

def score(row: pd.Series, query: str) -> float:
    """후보 랭킹 점수: exact 포함(가중치 2) + 레벤슈타인 유사도"""
    q_clean = normalize(SYNONYMS.get(query, query).replace("학과", ""))
    dept_tok = token_set(row.dept)
    coll_tok = token_set(row.college)

    exact = 1 if q_clean in "".join(dept_tok) or q_clean in "".join(coll_tok) else 0
    lev   = difflib.SequenceMatcher(None, q_clean, normalize(row.dept)).ratio()
    return exact * 2 + lev

def re_rank(candidates: List[pd.Series], query: str) -> pd.Series:
    return max(candidates, key=lambda r: score(r, query))

def guess_list_url(url: str) -> str:
    """
    상세 URL → 목록 URL 추정:
      1) ?mode=view ⇒ mode=list
      2) mode 파라미터가 없으면 ?mode=list 추가
    """
    if "mode=view" in url:
        return url.replace("mode=view", "mode=list")
    if "mode=list" in url:
        return url
    return url + ("&" if "?" in url else "?") + "mode=list"

# ── 메인 로직 ──────────────────────────────────────────────────
def main(query: str):
    index, meta = load_index()
    model = SentenceTransformer(MODEL_NAME)

    q_emb = model.encode([query]).astype("float32")
    _, I = index.search(q_emb, TOP_K_FAISS)

    candidates = [meta.iloc[i] for i in I[0]]
    best = re_rank(candidates, query)

    list_url = guess_list_url(best.url)
    print(f"[MATCH] {best.college}/{best.dept}  →  {list_url}")

    scraper = GenericScraper(best.college, best.dept, list_url)
    df = scraper.scrape().head(SHOW_ROWS)[["title", "posted_at", "url"]]
    print(df.to_string(index=False))

# ── CLI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python -m src.search.query_links \"<검색어>\"")
    main(" ".join(sys.argv[1:]))
