from __future__ import annotations
from typing import Dict, List, Tuple
import pandas as pd
import re
from datetime import datetime, timedelta

# 日付から曜日部分を除去するための正規表現
MD_EXTRACT_RE = re.compile(r"^\s*(\d{1,2}/\d{1,2})")

def load_code_map(csv_path: str) -> Dict[str, Tuple[str, str]]:
    df = pd.read_csv(csv_path)
    m: Dict[str, Tuple[str, str]] = {}
    for _, r in df.iterrows():
        code = str(r["code"]).strip()
        start = str(r["start"]) if not pd.isna(r["start"]) else ""
        end   = str(r["end"]) if not pd.isna(r["end"]) else ""
        m[code] = (start, end)
    return m

def _extract_md(md_str: str) -> str:
    """日付文字列から M/D 部分のみを抽出（曜日があれば除去）"""
    match = MD_EXTRACT_RE.match(str(md_str).strip())
    if match:
        return match.group(1)
    return md_str  # マッチしない場合はそのまま返す

def to_events(target_row: pd.DataFrame, date_cols: list[str], code_map: Dict[str, Tuple[str, str]], year: int) -> tuple[List[dict], List[str]]:
    # 横持ち → 縦持ち
    id_vars = [c for c in target_row.columns if c not in date_cols]
    long_df = target_row.melt(id_vars=id_vars, value_vars=date_cols, var_name="日付", value_name="コード")
    long_df["コード"] = long_df["コード"].astype(str).str.strip()

    unknown: set[str] = set()
    events: List[dict] = []

    def parse_dt(md: str, hm: str) -> datetime:
        md_clean = _extract_md(md)  # 曜日部分を除去
        base = datetime.strptime(f"{year}/{md_clean}", "%Y/%m/%d")
        if hm.endswith("+1"):
            t = datetime.strptime(hm[:-2], "%H:%M").time()
            return datetime.combine(base + timedelta(days=1), t)
        else:
            t = datetime.strptime(hm, "%H:%M").time()
            return datetime.combine(base, t)

    for _, r in long_df.iterrows():
        code = r["コード"]
        md = r["日付"]
        if code in ("", "nan", "None"):
            continue
        if code not in code_map:
            unknown.add(code)
            continue
        start, end = code_map[code]
        # 休日など start/end 空はスキップ
        if not start or not end:
            continue

        start_dt = parse_dt(md, start)
        end_dt   = parse_dt(md, end)

        events.append({
            "date": start_dt.strftime("%Y-%m-%d"),
            "start": start_dt.strftime("%H:%M"),
            "end": end_dt.strftime("%H:%M"),
            "end_plus1": (end_dt.date() != start_dt.date()),
            "title": code,
            "code": code
        })

    # 日付・時間でソート
    events.sort(key=lambda e: (e["date"], e["start"]))
    return events, sorted(list(unknown))