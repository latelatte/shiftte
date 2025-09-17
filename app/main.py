from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from uuid import uuid4
import os
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from app.services.extract import read_pdf_table, normalize_table, extract_person_row
from app.services.transform import load_code_map, to_events
import json
import base64

from dotenv import load_dotenv
load_dotenv()

# 本番環境（Heroku）では HTTPS を要求
# 開発環境でのみ HTTP を許可
if os.getenv("ENVIRONMENT") == "development":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

app = FastAPI()
# セッションの設定を最適化
app.add_middleware(
    SessionMiddleware, 
    secret_key=os.getenv("SESSION_SECRET", "dev-secret"),
    max_age=7200,  # 2時間
    same_site="lax",
    https_only=os.getenv("ENVIRONMENT") != "development",
    session_cookie="session_id"  # カスタムセッションクッキー名
)

# 静的ファイル（CSS, JS, 画像等）を配信
app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")

CODES_CSV = os.getenv("CODES_CSV_PATH", "data/codes.default.csv")
DEFAULT_YEAR = int(os.getenv("DEFAULT_YEAR", "2025"))
SCOPES = [
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/calendar.readonly"
]
CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
# 本番環境とローカル環境でリダイレクトURIを切り替え
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
REDIRECT_URI = f"{BASE_URL}/auth/callback"


@app.get("/api/calendars")
async def list_calendars(request: Request):
    service = _get_calendar_service(request)
    items = []
    page_token = None
    while True:
        resp = service.calendarList().list(pageToken=page_token).execute()
        for cal in resp.get("items", []):
            items.append({
                "id": cal["id"],
                "summary": cal.get("summary", ""),
                "primary": cal.get("primary", False),
                "accessRole": cal.get("accessRole", ""),
            })
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return {"calendars": items}


def _build_flow(state: str | None = None) -> Flow:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="Google OAuth credentials not configured")
    
    return Flow.from_client_config(
        {
            "web": {
                "client_id": CLIENT_ID,
                "project_id": "shift-rpa",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "client_secret": CLIENT_SECRET,
                "redirect_uris": [REDIRECT_URI],
            }
        },
        scopes=SCOPES,
        state=state,
        redirect_uri=REDIRECT_URI,
    )

def _save_job_to_session(request: Request, job_id: str, job_data: dict) -> None:
    """jobデータをセッションに保存（軽量化版）"""
    if "jobs" not in request.session:
        request.session["jobs"] = {}
    
    # セッションサイズを制限するため、重要な情報のみ保存
    lightweight_job = {
        "uploader_name": job_data["uploader_name"],
        "events": job_data["events"][:50],  # 最大50件まで（セッションサイズ制限対策）
        "year": job_data["year"],
        "created": job_data.get("created", 0),
        "updated": job_data.get("updated", 0),
        "skipped": job_data.get("skipped", 0),
        "deleted": job_data.get("deleted", 0),
    }
    request.session["jobs"][job_id] = lightweight_job
    print(f"[DEBUG] Saved lightweight job {job_id} to session. Total jobs: {len(request.session['jobs'])}")
    print(f"[DEBUG] Job has {len(lightweight_job['events'])} events (truncated if >50)")

def _get_job_from_session(request: Request, job_id: str) -> dict | None:
    """セッションからjobデータを取得"""
    jobs = request.session.get("jobs", {})
    print(f"[DEBUG] Looking for job {job_id}. Available jobs in session: {list(jobs.keys())}")
    job = jobs.get(job_id)
    if job:
        print(f"[DEBUG] Found job {job_id} with {len(job.get('events', []))} events")
    else:
        print(f"[DEBUG] Job {job_id} not found in session")
    return job

def _encode_job_data(job_data: dict) -> str:
    """jobデータをbase64エンコードして文字列にする"""
    json_str = json.dumps(job_data, ensure_ascii=False)
    encoded = base64.b64encode(json_str.encode('utf-8')).decode('ascii')
    return encoded

def _decode_job_data(encoded_data: str) -> dict | None:
    """base64エンコードされた文字列からjobデータを復元する"""
    try:
        json_str = base64.b64decode(encoded_data.encode('ascii')).decode('utf-8')
        return json.loads(json_str)
    except Exception as e:
        print(f"[DEBUG] Failed to decode job data: {e}")
        return None

def _tz_dt(date_str: str, time_str: str, plus_one: bool = False) -> str:
    # date_str: "YYYY-MM-DD", time_str: "HH:MM"
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    if plus_one:
        dt += timedelta(days=1)
    # Asia/Tokyo(+09:00) のオフセット付きISO
    jst = timezone(timedelta(hours=9))
    return dt.replace(tzinfo=jst).isoformat()

@app.get("/debug/java")
async def debug_java():
    """Java環境のデバッグ情報を取得"""
    import subprocess
    import os
    
    debug_info = {}
    
    # JAVA_HOME環境変数
    debug_info["JAVA_HOME"] = os.environ.get("JAVA_HOME", "Not set")
    
    # PATH環境変数
    debug_info["PATH"] = os.environ.get("PATH", "Not set")
    
    # Javaバージョン確認
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=10)
        debug_info["java_version"] = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }
    except Exception as e:
        debug_info["java_version"] = f"Error: {str(e)}"
    
    # jpype1の状態確認
    try:
        import jpype
        debug_info["jpype_available"] = True
        debug_info["jpype_version"] = jpype.__version__
        
        # JVMの起動テスト
        try:
            if not jpype.isJVMStarted():
                jpype.startJVM()
            debug_info["jvm_started"] = jpype.isJVMStarted()
            debug_info["jvm_info"] = {
                "version": jpype.java.lang.System.getProperty("java.version"),
                "vendor": jpype.java.lang.System.getProperty("java.vendor"),
                "home": jpype.java.lang.System.getProperty("java.home")
            }
        except Exception as e:
            debug_info["jvm_error"] = str(e)
            
    except ImportError:
        debug_info["jpype_available"] = False
    except Exception as e:
        debug_info["jpype_error"] = str(e)
    
    # tabula-pyのテスト
    try:
        import tabula
        debug_info["tabula_available"] = True
        debug_info["tabula_version"] = tabula.__version__
        
        # tabula-javaのjarファイルの場所を確認
        try:
            jar_path = tabula.io._jar_path()
            debug_info["tabula_jar_path"] = jar_path
            debug_info["tabula_jar_exists"] = os.path.exists(jar_path)
        except Exception as e:
            debug_info["tabula_jar_error"] = str(e)
            
    except ImportError:
        debug_info["tabula_available"] = False
    except Exception as e:
        debug_info["tabula_error"] = str(e)
    
    return {"debug_info": debug_info}

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    authed = bool(request.session.get("credentials"))
    return templates.TemplateResponse("index.html", {"request": request, "authed": authed})

@app.post("/api/upload")
async def api_upload(
    request: Request,
    file: UploadFile = File(...),
    name: str = Form(...),
    year: int = Form(DEFAULT_YEAR),
):
    pdf_bytes = await file.read()
    try:
        print(f"Processing PDF for user: {name.strip()}")
        df = read_pdf_table(pdf_bytes)
        print(f"PDF table read successfully, shape: {df.shape}")
        
        df, date_cols = normalize_table(df)
        print(f"Table normalized, date columns: {date_cols}")
        
        person_row, date_cols = extract_person_row(df, date_cols, name.strip())
        print(f"Person row extracted for: {name.strip()}")
        
        code_map = load_code_map(CODES_CSV)
        print(f"Code map loaded with {len(code_map)} entries")
        
        events, unknown = to_events(person_row, date_cols, code_map, year=year)
        print(f"Events generated: {len(events)} events, {len(unknown)} unknown codes")
        
    except Exception as e:
        print(f"Error in API upload: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        msg = str(e)
        if "No columns to parse from file" in msg:
            msg = "変換テーブルCSV（data/codes.default.csv）が空か見つかりません。"
        return JSONResponse({"error": msg}, status_code=400)

    job_id = uuid4().hex
    job_data = {
        "uploader_name": name.strip(),
        "events": events,            # [{date,start,end,end_plus1,title,code}]
        "unknown_codes": unknown,    # 未知コード（今回スキップ）
        "created": 0, "updated": 0, "skipped": 0, "deleted": 0,
        "year": year,
    }
    print(f"[DEBUG] Generated job_id: {job_id}")
    print(f"[DEBUG] Job data has {len(events)} events")
    
    # セッションに保存
    _save_job_to_session(request, job_id, job_data)
    
    print(f"[DEBUG] Redirecting to /preview?job_id={job_id}")
    return RedirectResponse(url=f"/preview?job_id={job_id}", status_code=303)

@app.get("/preview", response_class=HTMLResponse)
async def preview(request: Request, job_id: str):
    print(f"[DEBUG] Preview endpoint called with job_id: {job_id}")
    print(f"[DEBUG] Session data keys: {list(request.session.keys())}")
    
    job = _get_job_from_session(request, job_id)
    
    if not job:
        print(f"[DEBUG] Job {job_id} not found, raising 404")
        raise HTTPException(status_code=404, detail="job not found")
    authed = bool(request.session.get("credentials"))
    
    # カレンダーリストを取得（認証済みの場合のみ）
    calendars = []
    if authed:
        try:
            service = _get_calendar_service(request)
            calendar_list = service.calendarList().list().execute()
            calendars = [
                {
                    "id": cal["id"],
                    "summary": cal.get("summary", ""),
                    "primary": cal.get("primary", False),
                    "accessRole": cal.get("accessRole", ""),
                }
                for cal in calendar_list.get("items", [])
                if cal.get("accessRole") in ["owner", "writer"]  # 書き込み権限があるもののみ
            ]
        except Exception as e:
            print(f"カレンダーリスト取得エラー: {e}")
    
    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request, 
            "job_id": job_id, 
            "job": job, 
            "events": job["events"], 
            "authed": authed,
            "calendars": calendars
        }
    )

# ===== OAuth =====
@app.get("/auth/logout")
async def auth_logout(request: Request):
    """認証情報をクリア（開発用）"""
    request.session.pop("credentials", None)
    request.session.pop("state", None)
    return RedirectResponse("/")

@app.get("/auth/google")
async def auth_google(request: Request):
    flow = _build_flow()
    auth_url, state = flow.authorization_url(
        access_type="offline",            # リフレッシュトークンをもらう
        include_granted_scopes="true",
        prompt="consent"                  # 毎回でも確実にrefreshを得る
    )
    request.session["state"] = state
    return RedirectResponse(auth_url)

@app.get("/auth/callback")
async def auth_callback(request: Request):
    state = request.session.get("state")
    if not state:
        raise HTTPException(status_code=400, detail="State parameter missing")
    
    flow = _build_flow(state=state)
    try:
        flow.fetch_token(authorization_response=str(request.url))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OAuth callback failed: {str(e)}")
    
    creds = flow.credentials
    # セッションに保存（PoCなのでメモリ。実運用はDBに暗号化保存）
    request.session["credentials"] = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    return RedirectResponse("/")

def _get_calendar_service(request: Request):
    c = request.session.get("credentials")
    if not c:
        raise HTTPException(status_code=401, detail="Google未認可です。先に「Googleに接続」を押してください。")
    creds = Credentials(
        token=c["token"],
        refresh_token=c.get("refresh_token"),
        token_uri=c["token_uri"],
        client_id=c["client_id"],
        client_secret=c["client_secret"],
        scopes=c["scopes"],
    )
    return build("calendar", "v3", credentials=creds)

@app.post("/api/commit")
async def api_commit(request: Request, job_id: str = Form(...), calendar_id: str = Form("primary")):
    job = _get_job_from_session(request, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    service = _get_calendar_service(request)

    created = 0
    for e in job["events"]:
        # 終了側の日付は end_plus1 で翌日補正
        start_iso = _tz_dt(e["date"], e["start"], plus_one=False)
        end_iso = _tz_dt(e["date"], e["end"], plus_one=bool(e.get("end_plus1")))

        body = {
            "summary": e["title"],
            "start": {"dateTime": start_iso, "timeZone": "Asia/Tokyo"},
            "end":   {"dateTime": end_iso,   "timeZone": "Asia/Tokyo"},
            # PoCは“新規作成のみ”。重複は後で冪等性対応する
            "description": f"元コード: {e['code']}",
        }
        service.events().insert(calendarId=calendar_id, body=body).execute()
        created += 1

    job["created"] = created
    # セッションのjobデータも更新
    _save_job_to_session(request, job_id, job)
    return RedirectResponse(url=f"/result?job_id={job_id}", status_code=303)

@app.get("/result", response_class=HTMLResponse)
async def result(request: Request, job_id: str):
    job = _get_job_from_session(request, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return templates.TemplateResponse("result.html", {"request": request, "job": job})