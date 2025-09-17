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

from dotenv import load_dotenv
load_dotenv()

# 本番環境（Heroku）では HTTPS を要求
# 開発環境でのみ HTTP を許可
if os.getenv("ENVIRONMENT") == "development":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key=os.getenv("SESSION_SECRET", "dev-secret"))

# 静的ファイル（CSS, JS, 画像等）を配信
app.mount("/static", StaticFiles(directory="app/static"), name="static")

templates = Jinja2Templates(directory="app/templates")

JOBS: dict[str, dict] = {}

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

def _tz_dt(date_str: str, time_str: str, plus_one: bool = False) -> str:
    # date_str: "YYYY-MM-DD", time_str: "HH:MM"
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    if plus_one:
        dt += timedelta(days=1)
    # Asia/Tokyo(+09:00) のオフセット付きISO
    jst = timezone(timedelta(hours=9))
    return dt.replace(tzinfo=jst).isoformat()

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
        df = read_pdf_table(pdf_bytes)
        df, date_cols = normalize_table(df)
        person_row, date_cols = extract_person_row(df, date_cols, name.strip())
        code_map = load_code_map(CODES_CSV)
        events, unknown = to_events(person_row, date_cols, code_map, year=year)
    except Exception as e:
        msg = str(e)
        # よくあるエラーをユーザーフレンドリーに変換
        if "No columns to parse from file" in msg:
            msg = "変換テーブルCSV（data/codes.default.csv）が空か見つかりません。"
        elif "表が検出できませんでした" in msg:
            msg = "PDFから表を読み取れませんでした。ファイル形式や表の構造を確認してください。"
        elif "日付（M/D）が見つかりません" in msg:
            msg = "ヘッダー行に日付（例：1/1, 12/31）の形式が見つかりません。PDFの表構造を確認してください。"
        elif "指定の氏名が見つかりませんでした" in msg:
            msg = f"氏名「{name.strip()}」がPDFの表内で見つかりませんでした。正確な表記で入力してください。"
        elif "jpype" in msg.lower():
            msg = "PDF処理中にエラーが発生しました。システム管理者にお問い合わせください。"
        return JSONResponse({"error": msg}, status_code=400)

    job_id = uuid4().hex
    JOBS[job_id] = {
        "uploader_name": name.strip(),
        "events": events,            # [{date,start,end,end_plus1,title,code}]
        "unknown_codes": unknown,    # 未知コード（今回スキップ）
        "created": 0, "updated": 0, "skipped": 0, "deleted": 0,
        "year": year,
    }
    return RedirectResponse(url=f"/preview?job_id={job_id}", status_code=303)

@app.get("/preview", response_class=HTMLResponse)
async def preview(request: Request, job_id: str):
    job = JOBS.get(job_id)
    if not job:
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
    job = JOBS.get(job_id)
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
    return RedirectResponse(url=f"/result?job_id={job_id}", status_code=303)

@app.get("/result", response_class=HTMLResponse)
async def result(request: Request, job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return templates.TemplateResponse("result.html", {"request": request, "job": job})