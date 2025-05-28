"""
모든 CSV를 읽어 FAISS 인덱스 + 메타 정보(pandas DataFrame)를 저장
"""
import pickle
from pathlib import Path
from typing import List

import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from ..config import CSV_DIR

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
INDEX_FILE = CSV_DIR / "faiss.index"
META_FILE  = CSV_DIR / "metadata.pkl"


def build_index():
    model = SentenceTransformer(MODEL_NAME)
    dfs: List[pd.DataFrame] = []

    for csv_path in CSV_DIR.glob("*.csv"):
        dfs.append(pd.read_csv(csv_path))

    meta = pd.concat(dfs, ignore_index=True).reset_index(drop=True)
    texts = (meta["title"] + " " + meta["college"] + " " + meta["dept"]).tolist()

    embeddings = model.encode(texts, show_progress_bar=True, batch_size=64)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings.astype("float32"))

    faiss.write_index(index, str(INDEX_FILE))
    META_FILE.write_bytes(pickle.dumps(meta))
    print(f"[+] Indexed {len(meta)} notices")


if __name__ == "__main__":
    build_index()
