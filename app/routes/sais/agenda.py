"""SAIS — Agenda Inteligente"""
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
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


@router.get("/dia")
async def get_agenda_dia(data: Optional[str] = Query(None)):
    """Agenda completa do dia agrupada por técnico."""
    db = get_db()
    data = data or hoje_brt()

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_agenda, o.data_abertura,
            t.nome AS tecnico, t.id AS tecnico_id,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status = 'agendada'
          AND DATE(o.data_agenda, '+3 hours') = ?
        ORDER BY t.nome, o.data_agenda
    """, (data,)).fetchall()
    db.close()

    # Agrupar por técnico
    por_tecnico = {}
    for r in rows:
        nome = r["tecnico"] or "Sem técnico"
        if nome not in por_tecnico:
            por_tecnico[nome] = {"tecnico_id": r["tecnico_id"], "nome": nome, "os": []}
        por_tecnico[nome]["os"].append(dict(r))

    return {
        "data": data,
        "total": len(rows),
        "por_tecnico": list(por_tecnico.values()),
    }


@router.get("/futura")
async def get_agenda_futura(dias: int = Query(7)):
    """Agenda dos próximos dias."""
    db = get_db()
    hoje = hoje_brt()
    fim = (datetime.strptime(hoje, "%Y-%m-%d") + timedelta(days=dias)).strftime("%Y-%m-%d")

    rows = db.execute("""
        SELECT
            DATE(o.data_agenda, '+3 hours') AS data,
            COUNT(*) AS total,
            SUM(CASE WHEN o.categoria='servico' THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN o.categoria='suporte' THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN o.categoria='retirada' THEN 1 ELSE 0 END) AS retiradas,
            SUM(CASE WHEN o.categoria='infra' THEN 1 ELSE 0 END) AS infra
        FROM prod_os_cache o
        WHERE o.status = 'agendada'
          AND DATE(o.data_agenda, '+3 hours') BETWEEN ? AND ?
        GROUP BY DATE(o.data_agenda, '+3 hours')
        ORDER BY data
    """, (hoje, fim)).fetchall()
    db.close()

    return {"agenda_futura": [dict(r) for r in rows]}


@router.get("/reagendamentos")
async def get_reagendamentos(limit: int = Query(20)):
    """OS com status agendada que já deveriam ter sido executadas."""
    db = get_db()
    ontem = (datetime.now() + timedelta(hours=-3, days=-1)).strftime("%Y-%m-%d")

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.data_agenda, o.data_abertura,
            t.nome AS tecnico,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            ROUND((julianday('now') - julianday(o.data_agenda)) * 24, 1) AS horas_atraso
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status = 'agendada'
          AND DATE(o.data_agenda, '+3 hours') < ?
        ORDER BY horas_atraso DESC
        LIMIT ?
    """, (ontem, limit)).fetchall()
    db.close()

    return {"reagendamentos": [dict(r) for r in rows], "total": len(rows)}
