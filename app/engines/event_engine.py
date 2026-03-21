"""
SAIS — Motor de Eventos + Hub WebSocket
Detecta eventos relevantes e faz broadcast para:
- Frontend (WebSocket)
- TV/NOC (sais_eventos_tv)
- Alertas (sais_alertas)
- Telegram (se configurado)

Uso no FastAPI:
    from app.engines.event_engine import hub, detectar_eventos

WebSocket endpoint:
    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket):
        await hub.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except:
            hub.disconnect(websocket)
"""
import sqlite3
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import WebSocket

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"
log = logging.getLogger("EVENTS")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [EVENTS] %(message)s",
                    datefmt="%H:%M:%S")


# ═══════════════════════════════════════════════════════
# HUB WEBSOCKET
# ═══════════════════════════════════════════════════════

class WSHub:
    def __init__(self):
        self.connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self.connections.append(ws)
        log.info(f"WS conectado. Total: {len(self.connections)}")

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)
        log.info(f"WS desconectado. Total: {len(self.connections)}")

    async def broadcast(self, message: dict):
        """Envia mensagem para todos os clientes conectados."""
        if not self.connections:
            return
        mortos = []
        for ws in self.connections:
            try:
                await ws.send_json(message)
            except Exception:
                mortos.append(ws)
        for ws in mortos:
            self.disconnect(ws)

    async def broadcast_evento(self, tipo: str, dados: dict):
        """Envia evento tipado."""
        await self.broadcast({"tipo": tipo, "dados": dados, "ts": datetime.now().isoformat()})


# Instância global do hub
hub = WSHub()


# ═══════════════════════════════════════════════════════
# CONFIGURAÇÃO DE EVENTOS
# ═══════════════════════════════════════════════════════

POPUP_CONFIG = {
    "os_finalizada":    {"duracao": 6000,  "cor": "green",  "icone": "✅", "criticidade": "info"},
    "sla_estourado":    {"duracao": 10000, "cor": "red",    "icone": "🚨", "criticidade": "critico"},
    "sla_em_risco":     {"duracao": 8000,  "cor": "amber",  "icone": "⚠️", "criticidade": "aviso"},
    "meta_atingida":    {"duracao": 8000,  "cor": "green",  "icone": "🏆", "criticidade": "info"},
    "meta_em_risco":    {"duracao": 7000,  "cor": "amber",  "icone": "📉", "criticidade": "aviso"},
    "tecnico_ocioso":   {"duracao": 7000,  "cor": "amber",  "icone": "💤", "criticidade": "aviso"},
    "auditoria_grave":  {"duracao": 10000, "cor": "red",    "icone": "🔍", "criticidade": "critico"},
    "os_atrasada":      {"duracao": 8000,  "cor": "red",    "icone": "⏰", "criticidade": "critico"},
    "kpi_update":       {"duracao": 0,     "cor": "cyan",   "icone": "📊", "criticidade": "info"},
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _registrar_alerta(db, tipo: str, titulo: str, mensagem: str,
                      criticidade: str, os_id=None, tecnico_id=None):
    """Registra alerta no banco."""
    cfg = POPUP_CONFIG.get(tipo, {})
    db.execute("""
        INSERT INTO sais_alertas (tipo, criticidade, titulo, mensagem, os_id, tecnico_id)
        VALUES (?,?,?,?,?,?)
    """, (tipo, criticidade or cfg.get("criticidade", "info"),
          titulo, mensagem, os_id, tecnico_id))


def _registrar_evento_tv(db, tipo: str, titulo: str, subtitulo: str,
                         criticidade: str, os_id=None, tecnico_id=None):
    """Registra evento para TV/NOC."""
    db.execute("""
        INSERT INTO sais_eventos_tv (tipo, titulo, subtitulo, criticidade, os_id, tecnico_id)
        VALUES (?,?,?,?,?,?)
    """, (tipo, titulo, subtitulo, criticidade, os_id, tecnico_id))


# ═══════════════════════════════════════════════════════
# DETECÇÃO DE EVENTOS
# ═══════════════════════════════════════════════════════

def detectar_os_atrasadas(db) -> list:
    """OS em execução/aberta há mais de 4 horas."""
    agora = datetime.now()
    limite = agora - timedelta(hours=4)

    rows = db.execute("""
        SELECT o.ixc_os_id, o.tecnico_id, o.data_abertura,
               t.nome as tecnico_nome,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) as assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status IN ('execucao', 'aberta')
          AND o.data_abertura < ?
    """, (limite.strftime("%Y-%m-%d %H:%M:%S"),)).fetchall()

    eventos = []
    for r in rows:
        from datetime import datetime as dt
        dt_ab = None
        try:
            dt_ab = dt.strptime(str(r["data_abertura"])[:19], "%Y-%m-%d %H:%M:%S")
        except:
            pass
        horas = round((agora - dt_ab).total_seconds() / 3600, 1) if dt_ab else "?"

        eventos.append({
            "tipo": "os_atrasada",
            "os_id": r["ixc_os_id"],
            "tecnico_id": r["tecnico_id"],
            "tecnico_nome": r["tecnico_nome"],
            "assunto": r["assunto"],
            "horas_aberta": horas,
            "titulo": f"OS #{r['ixc_os_id']} atrasada",
            "subtitulo": f"{r['tecnico_nome']} · {horas}h sem fechar",
        })
    return eventos


def detectar_tecnicos_ociosos(db) -> list:
    """Técnicos sem OS há mais de 1.5 horas."""
    config = db.execute(
        "SELECT valor FROM sais_config WHERE chave='tecnico_ocioso_horas'"
    ).fetchone()
    horas_limite = float(config["valor"]) if config else 1.5
    limite = datetime.now() - timedelta(hours=horas_limite)

    tecnicos = db.execute(
        "SELECT id, nome FROM prod_tecnicos WHERE ativo=1"
    ).fetchall()

    ociosos = []
    for t in tecnicos:
        ultima_os = db.execute("""
            SELECT MAX(COALESCE(data_fechamento, data_abertura)) as ultima
            FROM prod_os_cache
            WHERE tecnico_id = ?
              AND status IN ('finalizada', 'execucao', 'aberta')
              AND DATE(COALESCE(data_fechamento, data_abertura)) = DATE('now', '-3 hours')
        """, (t["id"],)).fetchone()

        ultima = ultima_os["ultima"] if ultima_os else None
        if ultima:
            try:
                dt_ultima = datetime.strptime(str(ultima)[:19], "%Y-%m-%d %H:%M:%S")
                if dt_ultima < limite:
                    horas = round((datetime.now() - dt_ultima).total_seconds() / 3600, 1)
                    ociosos.append({
                        "tipo": "tecnico_ocioso",
                        "tecnico_id": t["id"],
                        "tecnico_nome": t["nome"],
                        "horas_ocioso": horas,
                        "titulo": f"{t['nome']} ocioso",
                        "subtitulo": f"Sem OS há {horas}h",
                    })
            except:
                pass

    return ociosos


def detectar_meta(db) -> Optional[dict]:
    """Verifica status da meta do dia."""
    config = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia_pontos'"
    ).fetchone()
    meta = int(config["valor"]) if config else 80
    hoje = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")
    hoje = datetime.now().strftime("%Y-%m-%d")
    fins = db.execute("""
        SELECT COUNT(*) as total FROM prod_os_cache
        WHERE status='finalizada'
          AND DATE(data_fechamento, '+3 hours') = ?
    """, (hoje,)).fetchone()

    total_fins = fins["total"] if fins else 0
    pct = round(total_fins / meta * 100) if meta > 0 else 0

    if total_fins == meta:
        return {
            "tipo": "meta_atingida",
            "titulo": "🏆 Meta do dia atingida!",
            "subtitulo": f"{total_fins} OS finalizadas",
            "pct": pct,
        }
    elif pct < 50:
        return {
            "tipo": "meta_em_risco",
            "titulo": f"Meta em risco ({pct}%)",
            "subtitulo": f"{total_fins}/{meta} pts",
            "pct": pct,
        }
    return None


# ═══════════════════════════════════════════════════════
# EXECUÇÃO PRINCIPAL (chamada pelo cron ou endpoint)
# ═══════════════════════════════════════════════════════

def detectar_e_registrar() -> dict:
    """
    Detecta todos os eventos e registra no banco.
    Retorna resumo para broadcast via WebSocket.
    """
    db = get_db()
    novos_alertas  = 0
    novos_tv       = 0

    # OS atrasadas
    for ev in detectar_os_atrasadas(db):
        # Verificar se já existe alerta não lido
        existe = db.execute("""
            SELECT id FROM sais_alertas
            WHERE tipo='os_atrasada' AND os_id=? AND lido=0
        """, (ev["os_id"],)).fetchone()
        if not existe:
            _registrar_alerta(db, ev["tipo"], ev["titulo"], ev["subtitulo"],
                              "critico", ev["os_id"], ev["tecnico_id"])
            _registrar_evento_tv(db, ev["tipo"], ev["titulo"], ev["subtitulo"],
                                 "critico", ev["os_id"], ev["tecnico_id"])
            novos_alertas += 1
            novos_tv += 1

    # Técnicos ociosos
    for ev in detectar_tecnicos_ociosos(db):
        existe = db.execute("""
            SELECT id FROM sais_alertas
            WHERE tipo='tecnico_ocioso' AND tecnico_id=? AND lido=0
              AND criado_em > datetime('now', '-3 hours', '-2 hours')
        """, (ev["tecnico_id"],)).fetchone()
        if not existe:
            _registrar_alerta(db, ev["tipo"], ev["titulo"], ev["subtitulo"],
                              "aviso", None, ev["tecnico_id"])
            _registrar_evento_tv(db, ev["tipo"], ev["titulo"], ev["subtitulo"],
                                 "aviso", None, ev["tecnico_id"])
            novos_alertas += 1
            novos_tv += 1

    # Meta
    meta_ev = detectar_meta(db)
    if meta_ev:
        existe = db.execute("""
            SELECT id FROM sais_alertas
            WHERE tipo=? AND DATE(criado_em)=DATE('now','-3 hours') AND lido=0
        """, (meta_ev["tipo"],)).fetchone()
        if not existe:
            _registrar_alerta(db, meta_ev["tipo"], meta_ev["titulo"],
                              meta_ev["subtitulo"], "info")
            novos_alertas += 1

    db.commit()

    # Log
    db.execute("""
        INSERT INTO sais_automacoes_log (automacao, status, itens_afetados, detalhe)
        VALUES ('event_engine', 'ok', ?, ?)
    """, (novos_alertas, f"alertas:{novos_alertas} tv:{novos_tv}"))
    db.commit()
    db.close()

    log.info(f"Eventos: {novos_alertas} alertas, {novos_tv} eventos TV")
    return {"novos_alertas": novos_alertas, "novos_tv": novos_tv}


if __name__ == "__main__":
    detectar_e_registrar()
