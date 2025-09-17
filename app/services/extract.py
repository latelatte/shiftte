from __future__ import annotations
import tempfile
from typing import List, Optional
import re
import tabula
import pandas as pd

MD_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})\s*$")

def _looks_like_md(s: str) -> bool:
    return bool(MD_RE.match(str(s)))

def read_pdf_table(pdf_bytes: bytes) -> pd.DataFrame:
    """PDFの表をTabulaで読み込み、最も列数が多いテーブルを採用。"""
    with tempfile.NamedTemporaryFile(suffix=".pdf") as fp:
        fp.write(pdf_bytes)
        fp.flush()
        # lattice & stream を両方試す→列数最大のものを採用
        dfs: List[pd.DataFrame] = []
        for mode in ("lattice", "stream"):
            try:
                tables = tabula.read_pdf(
                    fp.name, pages="all", multiple_tables=True,
                    lattice=(mode=="lattice"), stream=(mode=="stream")
                )
                if tables:
                    dfs.extend(tables)
            except Exception:
                pass
        if not dfs:
            raise ValueError("表が検出できませんでした。PDFのフォーマットを確認してください。")

        df = max(dfs, key=lambda d: d.shape[1])  # 列数が多い=表らしい
        df = df.reset_index(drop=True)

        # 列名の空欄対策：文字列化
        df.columns = [str(c).strip() if str(c).strip() else f"col_{i}" for i, c in enumerate(df.columns)]
        return df

def normalize_table(df: pd.DataFrame) -> pd.DataFrame:
    """曜日行の除去／名前列の補正／日付列の正規化"""
    # 1) 日付ヘッダを特定（M/D）
    date_cols = [c for c in df.columns if _looks_like_md(c)]
    if not date_cols:
        # 先頭行に日付が並んでいて列名が NaN/Unnamed の場合、先頭行をヘッダに差し替え
        first_row = df.iloc[0].astype(str).tolist()
        if any(_looks_like_md(v) for v in first_row):
            df.columns = [v.strip() for v in first_row]
            df = df.iloc[1:].reset_index(drop=True)
            date_cols = [c for c in df.columns if _looks_like_md(c)]

    if not date_cols:
        raise ValueError("ヘッダに日付（M/D）が見つかりません。")

    # 2) 曜日行の除外（ヘッダ日付列が 1文字の曜日 [月火水木金土日] になってる行を落とす）
    weekday_set = set("月火水木金土日")
    def is_week_row(row) -> bool:
        vals = [str(row.get(c, "")).strip() for c in date_cols]
        return all(len(v) == 1 and v in weekday_set for v in vals if v != "")

    df = df[~df.apply(is_week_row, axis=1)].reset_index(drop=True)

    # 3) 名前列の推定：非日付列のうち、文字が多く重複が少ない列を候補に
    nondates = [c for c in df.columns if c not in date_cols]
    # Tabulaの癖で先頭が空列、次が名前列なことが多いので優先順候補
    candidates = []
    if len(nondates) >= 2: candidates = [nondates[1], nondates[0]] + nondates[2:]
    else: candidates = nondates

    # ヘッダ名が「名前」「氏名」などなら最優先
    for c in nondates:
        if any(x in str(c) for x in ("名前", "氏名", "スタッフ", "従業員")):
            candidates = [c] + [x for x in candidates if x != c]
            break

    name_col = candidates[0] if candidates else nondates[0]
    # 明確な空列は落とし、必要なら後工程で参照
    df = df.rename(columns={name_col: "スタッフ名"})
    return df, date_cols

def extract_person_row(df: pd.DataFrame, date_cols: list[str], person: str) -> pd.DataFrame:
    def norm(s: str) -> str:
        return str(s).replace(" ", "").replace("　", "").strip()

    if "スタッフ名" not in df.columns:
        raise ValueError("スタッフ名列を特定できませんでした。")

    df["__name_norm"] = df["スタッフ名"].astype(str).map(norm)
    target = df[df["__name_norm"] == norm(person)].copy()
    if target.empty:
        # 部分一致の保険
        target = df[df["__name_norm"].str.contains(norm(person), na=False)].copy()

    if target.empty:
        raise ValueError(f"指定の氏名が見つかりませんでした: {person}")

    # 1人だけの前提。複数行ある場合は先頭を採用
    return target.iloc[[0]], date_cols