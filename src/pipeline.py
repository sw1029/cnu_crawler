from .scraper.generic import GenericScraper
import importlib
from typing import Dict, Type

from tqdm import tqdm

from .config import LINKS_FILE
from .utils import load_links, save_dataframe

def main():
    for college, dept, url in tqdm(load_links(LINKS_FILE), desc="Crawling"):
        try:
            scraper = GenericScraper(college, dept, url)  # ← 항상 Generic 사용
            df = scraper.scrape()
            path = save_dataframe(df, college, dept)
            print(f"[√] {college}/{dept} → {len(df)} rows  ➜  {path.name}")
        except Exception as e:
            print(f"[×] {college}/{dept} 실패: {e}")
if __name__ == "__main__":
    main()