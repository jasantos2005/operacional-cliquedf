from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

from app.routes import dashboard, ranking, os_tipos, metas, tecnicos, telegram, usuarios, permissoes

app = FastAPI(title="HubProdutividade", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard.router,   prefix="/api/dashboard")
app.include_router(ranking.router,     prefix="/api/ranking")
app.include_router(os_tipos.router,    prefix="/api/os-tipos")
app.include_router(metas.router,       prefix="/api/metas")
app.include_router(tecnicos.router,    prefix="/api/tecnicos")
app.include_router(telegram.router,    prefix="/api/telegram")
app.include_router(usuarios.router,    prefix="/api/admin/usuarios")
app.include_router(permissoes.router,  prefix="/api/admin/permissoes")

app.mount("/static", StaticFiles(directory="/opt/automacoes/cliquedf/operacional/static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("/opt/automacoes/cliquedf/operacional/static/index.html") as f:
        return f.read()

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "operacao": os.getenv("OPERACAO"),
        "versao": "1.0.0"
    }
