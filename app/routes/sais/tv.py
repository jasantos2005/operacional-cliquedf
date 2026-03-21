"""SAIS — Modo TV / NOC"""
import sqlite3
from datetime import datetime, timedelta
from fastapi import APIRouter, Query

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")


@router.get("/estado")
async def get_estado_tv():
    """Estado completo para a TV — uma chamada traz tudo."""
    db = get_db()
    data = hoje_brt()

    # KPIs
    kpis = dict(db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (data,)).fetchone())

    meta_row = db.execute("SELECT valor FROM sais_config WHERE chave='meta_dia'").fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150
    fins = kpis["finalizadas"] or 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    # Top 5 técnicos
    from app.engines.score_engine import ranking_dia
    top5 = ranking_dia(data, limit=5)

    # Últimas 5 OS finalizadas
    recentes = db.execute("""
        SELECT o.ixc_os_id, t.nome AS tecnico, o.categoria,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
               o.data_fechamento
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status='finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = ?
        ORDER BY o.data_fechamento DESC
        LIMIT 5
    """, (data,)).fetchall()

    # Alertas críticos
    alertas = db.execute("""
        SELECT tipo, titulo, mensagem, criticidade
        FROM sais_alertas
        WHERE lido=0
          AND criticidade IN ('critico','aviso')
        ORDER BY CASE criticidade WHEN 'critico' THEN 1 ELSE 2 END,
                 criado_em DESC
        LIMIT 5
    """).fetchall()

    db.close()

    return {
        "ts": datetime.now().isoformat(),
        "data": data,
        "kpis": {**kpis, "meta": meta, "pct_meta": pct_meta},
        "top5": top5,
        "recentes": [dict(r) for r in recentes],
        "alertas": [dict(r) for r in alertas],
    }


@router.get("/popups-pendentes")
async def get_popups_pendentes():
    """Eventos não exibidos ainda na TV."""
    db = get_db()
    rows = db.execute("""
        SELECT e.*, t.nome AS tecnico_nome
        FROM sais_eventos_tv e
        LEFT JOIN prod_tecnicos t ON t.id = e.tecnico_id
        WHERE e.exibido = 0
        ORDER BY
            CASE e.criticidade WHEN 'critico' THEN 1 WHEN 'aviso' THEN 2 ELSE 3 END,
            e.criado_em DESC
        LIMIT 10
    """).fetchall()
    db.close()
    return {"popups": [dict(r) for r in rows]}


@router.post("/popups/{evento_id}/exibido")
async def marcar_exibido(evento_id: int):
    """Marca evento como exibido na TV."""
    db = get_db()
    db.execute("UPDATE sais_eventos_tv SET exibido=1 WHERE id=?", (evento_id,))
    db.commit()
    db.close()
    return {"ok": True}


@router.post("/popups/marcar-todos-exibidos")
async def marcar_todos_exibidos():
    """Marca todos os eventos como exibidos."""
    db = get_db()
    db.execute("UPDATE sais_eventos_tv SET exibido=1 WHERE exibido=0")
    db.commit()
    db.close()
    return {"ok": True}
