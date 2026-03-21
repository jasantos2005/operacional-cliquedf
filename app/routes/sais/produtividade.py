"""SAIS — Produtividade"""
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


@router.get("/ranking")
async def get_ranking(data: Optional[str] = Query(None), limit: int = Query(20)):
    """Ranking de produtividade do dia."""
    from app.engines.score_engine import ranking_dia
    data = data or hoje_brt()
    ranking = ranking_dia(data, limit)
    return {"data": data, "ranking": ranking}


@router.get("/tecnico/{tecnico_id}")
async def get_produtividade_tecnico(
    tecnico_id: int,
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
):
    """Produtividade detalhada de um técnico."""
    from app.engines.score_engine import historico_tecnico, calcular_score_tecnico
    db = get_db()
    data = hoje_brt()

    tecnico = db.execute(
        "SELECT id, nome, meta_dia FROM prod_tecnicos WHERE id=?", (tecnico_id,)
    ).fetchone()
    if not tecnico:
        db.close()
        return {"erro": "Técnico não encontrado"}

    # Score do dia
    score_hoje = calcular_score_tecnico(tecnico_id, data)

    # Histórico 30 dias
    historico = historico_tecnico(tecnico_id, dias=30)

    # Por categoria no período
    data_ini = data_inicio or (datetime.strptime(data, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    data_fim = data_fim or data

    cats = db.execute("""
        SELECT categoria, COUNT(*) as total,
               SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) as finalizadas
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND DATE(COALESCE(data_fechamento, data_abertura), '+3 hours') BETWEEN ? AND ?
        GROUP BY categoria
    """, (tecnico_id, data_ini, data_fim)).fetchall()

    # Tempo médio por categoria
    tempo_medio = db.execute("""
        SELECT categoria,
               ROUND(AVG((julianday(data_fechamento) - julianday(data_abertura)) * 24), 2) AS horas_media
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND status='finalizada'
          AND data_fechamento IS NOT NULL
          AND data_fechamento NOT LIKE '0000%'
          AND DATE(data_fechamento, '+3 hours') BETWEEN ? AND ?
        GROUP BY categoria
    """, (tecnico_id, data_ini, data_fim)).fetchall()

    db.close()

    return {
        "tecnico": dict(tecnico),
        "score_hoje": score_hoje,
        "historico_30d": historico,
        "por_categoria": [dict(r) for r in cats],
        "tempo_medio": [dict(r) for r in tempo_medio],
    }


@router.get("/por-assunto")
async def get_produtividade_assunto(data: Optional[str] = Query(None)):
    """Produtividade agrupada por tipo de OS."""
    db = get_db()
    data = data or hoje_brt()

    rows = db.execute("""
        SELECT
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            o.categoria,
            COUNT(*) AS total,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            ROUND(AVG(CASE WHEN o.status='finalizada' AND o.data_fechamento NOT LIKE '0000%'
                THEN (julianday(o.data_fechamento) - julianday(o.data_abertura)) * 60
                END), 0) AS minutos_medio
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
        GROUP BY o.ixc_assunto_id
        ORDER BY total DESC
    """, (data,)).fetchall()
    db.close()

    return {"data": data, "por_assunto": [dict(r) for r in rows]}
