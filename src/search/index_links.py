"""
links.txt → ko-sroberta 임베딩 → FAISS 인덱스(link_index.faiss) + 메타(link_meta.pkl)
실행:  python -m src.search.index_links
"""
import os, csv, pickle, json
from pathlib import Path
from typing import List, Dict

os.environ["TRANSFORMERS_NO_TF"] = "1"      # TF 차단

import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from ..config import DATA_DIR, LINKS_FILE

MODEL_NAME = "jhgan/ko-sroberta-multitask"   # ★ query_links.py 와 동일해야 함
INDEX_FILE = DATA_DIR / "link_index.faiss"
META_FILE  = DATA_DIR / "link_meta.pkl"
INFO_FILE  = DATA_DIR / "link_info.json"

# ── links.txt 로드 ────────────────────────────────────────────
def load_links() -> List[Dict]:
    rows = []
    with LINKS_FILE.open(encoding="utf-8") as f:
        reader = csv.reader(f)
        for college, dept, url in reader:
            college, dept = college.strip(), dept.strip()
            if college == "-": college = dept
            if dept == "-":   dept = college
            rows.append({"college": college, "dept": dept, "url": url.strip()})
    return rows

# ── 메인 ───────────────────────────────────────────────────────
def main():
    links = load_links()
    model = SentenceTransformer(MODEL_NAME)

    corpus = [f"{r['college']} {r['dept']}" for r in links]
    emb    = model.encode(corpus, show_progress_bar=True, batch_size=32).astype("float32")

    index = faiss.IndexFlatL2(emb.shape[1])
    index.add(emb)

    # 저장
    faiss.write_index(index, str(INDEX_FILE))
    META_FILE.write_bytes(pickle.dumps(pd.DataFrame(links)))
    INFO_FILE.write_text(json.dumps({"model": MODEL_NAME, "dim": int(emb.shape[1])}))

    print(f"[+] indexed {len(links)} links ({emb.shape[1]}-d) → {INDEX_FILE.name}")

if __name__ == "__main__":
    main()
