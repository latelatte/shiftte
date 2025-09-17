from __future__ import annotations
import tempfile
from typing import List, Optional
import re
import tabula
import pandas as pd

MD_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})\s*$")

def _looks_like_md(s: str) -> bool:
    return bool(MD_RE.match(str(s)))

def _try_pdfplumber_fallback(pdf_bytes: bytes) -> Optional[pd.DataFrame]:
    """pdfplumberを使ったフォールバック処理"""
    try:
        import pdfplumber
        print("Trying pdfplumber as fallback...")
        
        with tempfile.NamedTemporaryFile(suffix=".pdf") as fp:
            fp.write(pdf_bytes)
            fp.flush()
            
            tables_data = []
            
            with pdfplumber.open(fp.name) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    print(f"Processing page {page_num + 1} with pdfplumber")
                    # 表を抽出
                    tables = page.extract_tables()
                    if tables:
                        print(f"Found {len(tables)} tables on page {page_num + 1}")
                        for table in tables:
                            if not table or len(table) < 2:  # ヘッダー+データ行が最低限必要
                                continue
                            
                            # 表をDataFrameに変換
                            try:
                                df = pd.DataFrame(table[1:], columns=table[0])  # 最初の行をヘッダーとする
                                if df.shape[1] > 2:  # 最低限の列数が必要
                                    tables_data.append(df)
                            except Exception:
                                # ヘッダーがない場合の対処
                                try:
                                    df = pd.DataFrame(table)
                                    if df.shape[1] > 2:
                                        tables_data.append(df)
                                except Exception:
                                    continue
            
            if tables_data:
                # 最も列数が多いテーブルを採用
                df = max(tables_data, key=lambda d: d.shape[1])
                print(f"pdfplumber found table with shape: {df.shape}")
                return df
                    
    except ImportError:
        print("pdfplumber not available")
    except Exception as e:
        print(f"pdfplumber fallback failed: {str(e)}")
    
    return None

def read_pdf_table(pdf_bytes: bytes) -> pd.DataFrame:
    """PDFの表をTabulaで読み込み、最も列数が多いテーブルを採用。"""
    with tempfile.NamedTemporaryFile(suffix=".pdf") as fp:
        fp.write(pdf_bytes)
        fp.flush()
        
        print(f"PDF file size: {len(pdf_bytes)} bytes")
        
        # より多くの抽出方法を試行
        dfs: List[pd.DataFrame] = []
        
        # 1. 従来の方法（lattice & stream）
        for mode in ("lattice", "stream"):
            try:
                print(f"Trying tabula with mode: {mode}")
                tables = tabula.read_pdf(
                    fp.name, pages="all", multiple_tables=True,
                    lattice=(mode=="lattice"), stream=(mode=="stream")
                )
                if tables:
                    print(f"Found {len(tables)} tables with {mode} mode")
                    dfs.extend(tables)
                else:
                    print(f"No tables found with {mode} mode")
            except Exception as e:
                print(f"Error with {mode} mode: {str(e)}")
        
        # 2. より寛容な設定で再試行
        if not dfs:
            print("Trying with more permissive settings...")
            try:
                # guess=Falseで境界検出を無効化
                tables = tabula.read_pdf(
                    fp.name, pages="all", multiple_tables=True,
                    guess=False, pandas_options={'header': None}
                )
                if tables:
                    print(f"Found {len(tables)} tables with permissive settings")
                    dfs.extend(tables)
            except Exception as e:
                print(f"Error with permissive settings: {str(e)}")
        
        # 3. エリア指定なしで全体を対象
        if not dfs:
            print("Trying to read entire page as table...")
            try:
                tables = tabula.read_pdf(
                    fp.name, pages="all", 
                    lattice=True, stream=False,
                    multiple_tables=False,
                    pandas_options={'header': None}
                )
                if tables:
                    print(f"Found {len(tables)} tables reading entire page")
                    dfs.extend(tables)
            except Exception as e:
                print(f"Error reading entire page: {str(e)}")
        
        if not dfs:
            print("No tables detected with tabula, trying pdfplumber fallback...")
            fallback_df = _try_pdfplumber_fallback(pdf_bytes)
            if fallback_df is not None:
                dfs.append(fallback_df)
        
        if not dfs:
            print("No tables detected with any method")
            raise ValueError("表が検出できませんでした。PDFのフォーマットを確認してください。")

        # 最も列数が多いテーブルを採用
        df = max(dfs, key=lambda d: d.shape[1])
        print(f"Selected table with shape: {df.shape}")
        df = df.reset_index(drop=True)

        # 列名の空欄対策：文字列化
        df.columns = [str(c).strip() if str(c).strip() else f"col_{i}" for i, c in enumerate(df.columns)]
        print(f"Table columns: {list(df.columns)}")
        return df

def normalize_table(df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
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

def extract_person_row(df: pd.DataFrame, date_cols: list[str], person: str) -> tuple[pd.DataFrame, List[str]]:
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