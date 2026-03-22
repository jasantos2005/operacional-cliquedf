from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

from app.routes import dashboard, ranking, os_tipos, metas, tecnicos, telegram, usuarios, permissoes
from app.routes import dashboard_v2

# SAIS — novos módulos
from app.routes.sais import visao_geral, central, agenda, produtividade, auditoria, tv, pontuacao
from app.core.websocket import ws_router

app = FastAPI(title="SAIS — HubProdutividade", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Rotas legadas ──────────────────────────────────────
app.include_router(dashboard.router,   prefix="/api/dashboard")
app.include_router(ranking.router,     prefix="/api/ranking")
app.include_router(os_tipos.router,    prefix="/api/os-tipos")
app.include_router(metas.router,       prefix="/api/metas")
app.include_router(tecnicos.router,    prefix="/api/tecnicos")
app.include_router(telegram.router,    prefix="/api/telegram")
app.include_router(usuarios.router,    prefix="/api/admin/usuarios")
app.include_router(permissoes.router,  prefix="/api/admin/permissoes")
app.include_router(dashboard_v2.router,prefix="/api/dashboard/v2", tags=["Dashboard V2"])

# ── SAIS — novos endpoints ─────────────────────────────
app.include_router(visao_geral.router, prefix="/api/sais/visao-geral",    tags=["SAIS Visão Geral"])
app.include_router(central.router,    prefix="/api/sais/central",        tags=["SAIS Central"])
app.include_router(agenda.router,     prefix="/api/sais/agenda",         tags=["SAIS Agenda"])
app.include_router(produtividade.router, prefix="/api/sais/produtividade", tags=["SAIS Produtividade"])
app.include_router(auditoria.router,  prefix="/api/sais/auditoria",      tags=["SAIS Auditoria"])
app.include_router(tv.router,         prefix="/api/sais/tv",             tags=["SAIS TV"])
app.include_router(pontuacao.router,  prefix="/api/sais/pontuacao",       tags=["SAIS Pontuação"])

# ── WebSocket ──────────────────────────────────────────
app.include_router(ws_router)

# ── Static files ───────────────────────────────────────
app.mount("/static", StaticFiles(directory="/opt/automacoes/cliquedf/operacional/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("/opt/automacoes/cliquedf/operacional/static/index.html") as f:
        return f.read()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "sistema": "SAIS",
        "operacao": os.getenv("OPERACAO"),
        "versao": "2.0.0"
    }
