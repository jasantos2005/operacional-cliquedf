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
from app.routes.sais import visao_geral, central, agenda, produtividade, auditoria, tv, pontuacao, auth as sais_auth
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
app.include_router(sais_auth.router,  prefix="/api/sais/auth",            tags=["SAIS Auth"])

# ── WebSocket ──────────────────────────────────────────
app.include_router(ws_router)

# ── Static files ───────────────────────────────────────
app.mount("/static", StaticFiles(directory="/opt/automacoes/cliquedf/operacional/static"), name="static")



@app.get("/login", response_class=HTMLResponse)
async def login_page():
    with open("/opt/automacoes/cliquedf/operacional/static/login.html") as f:
        return f.read()

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("/opt/automacoes/cliquedf/operacional/static/index.html") as f:
        return f.read()



@app.on_event("startup")
async def startup_event():
    """Inicializa schemas e configs na startup."""
    try:
        from app.routes.sais.auth import init_schema
        init_schema()
        print("[SAIS] Schema de autenticação verificado ✓")
    except Exception as e:
        print(f"[SAIS] Aviso na init do auth: {e}")

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "sistema": "SAIS",
        "operacao": os.getenv("OPERACAO"),
        "versao": "2.0.0"
    }

@app.get("/api/logs")
async def get_logs():
    """Lê os logs dos crons e retorna as últimas linhas."""
    import os
    LOGS = {
        "sync":      "/var/log/sais_sync.log",
        "sync_full": "/var/log/sais_sync_full.log",
        "estoque":   "/var/log/sais_estoque.log",
        "auditoria": "/var/log/sais_auditoria.log",
        "destaques": "/var/log/sais_destaques.log",
    }
    resultado = {}
    for nome, path in LOGS.items():
        try:
            if os.path.exists(path):
                with open(path, "r") as f:
                    linhas = f.readlines()
                # Últimas 30 linhas
                ultimas = [l.rstrip() for l in linhas[-30:] if l.strip()]
                # Info do arquivo
                stat = os.stat(path)
                from datetime import datetime
                from datetime import timezone, timedelta
                brt = timezone(timedelta(hours=-3))
                modificado = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).astimezone(brt).strftime("%Y-%m-%d %H:%M:%S")
                resultado[nome] = {
                    "linhas": ultimas,
                    "total_linhas": len(linhas),
                    "modificado": modificado,
                    "tamanho": stat.st_size,
                    "ok": True,
                }
            else:
                resultado[nome] = {"ok": False, "linhas": [], "modificado": "—"}
        except Exception as e:
            resultado[nome] = {"ok": False, "erro": str(e), "linhas": []}
    return resultado
