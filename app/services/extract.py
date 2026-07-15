from __future__ import annotations
import tempfile
from typing import List, Optional
import re
import gc
import os
import pandas as pd

# tabula, pdfplumberは遅延インポート（メモリ節約）
# JVMヒープサイズを制限（Herokuメモリ制限対応）
os.environ.setdefault('JAVA_TOOL_OPTIONS', '-Xmx256m -Xms64m')

# 日付のみ: 12/30
MD_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})\s*$")
# 日付+曜日: 12/30(火) or 12/30（火） or 12/30 (火) など
MD_WEEKDAY_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})\s*[（(]\s*[月火水木金土日]\s*[）)]\s*$")

def _looks_like_md(s: str) -> bool:
    """日付形式かどうかを判定（日付のみ、または日付+曜日の両方に対応）"""
    s_str = str(s)
    return bool(MD_RE.match(s_str) or MD_WEEKDAY_RE.match(s_str))

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
    finally:
        # メモリ解放
        gc.collect()
    
    return None

def read_pdf_table(pdf_bytes: bytes) -> pd.DataFrame:
    """PDFの表をTabulaで読み込み、最も列数が多いテーブルを採用。"""
    # tabulaを遅延インポート（JVMの起動を遅らせる）
    import tabula
    
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
        
        # メモリ解放（JVMヒープを含む）
        gc.collect()
        
        return df

def normalize_table(df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
    """曜日行の除去／名前列の補正／日付列の正規化"""
    # 1) 日付ヘッダを特定（M/D）
    date_cols = [c for c in df.columns if _looks_like_md(c)]
    if not date_cols:
        # 日付行がヘッダにない場合（tabula が注記行をヘッダに拾う等）、
        # 全行を走査して M/D が最も多く並ぶ行をヘッダに昇格し、それ以降をデータとする。
        best_idx, best_count = None, 0
        for idx in range(len(df)):
            row_vals = df.iloc[idx].astype(str).tolist()
            cnt = sum(_looks_like_md(v) for v in row_vals)
            if cnt > best_count:
                best_idx, best_count = idx, cnt
        # 週の日付が横に並ぶので数本以上を要求（誤検出防止）
        if best_idx is not None and best_count >= 3:
            df.columns = [str(v).strip() for v in df.iloc[best_idx].tolist()]
            df = df.iloc[best_idx + 1:].reset_index(drop=True)
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