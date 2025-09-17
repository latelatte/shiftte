from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

# テンプレート設定
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    トップページ: PDFアップロード + 氏名入力 + OAuthボタン
    """
    return templates.TemplateResponse("index.html", {"request": request})