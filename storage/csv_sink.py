# cnu_crawler/storage/csv_sink.py
from pathlib import Path
import pandas as pd
from datetime import datetime

from .models import ENGINE
from ..config import DATA_DIR

def dump_daily_csv():
    ts = datetime.utcnow().strftime("%Y%m%d")
    out_dir = Path(DATA_DIR) / "csv"
    out_dir.mkdir(exist_ok=True)
    df = pd.read_sql_table("notices", ENGINE.connect())
    df.to_csv(out_dir / f"notices_{ts}.csv", index=False)
