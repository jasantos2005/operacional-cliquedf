"""SAIS — Auditoria Operacional"""
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


@router.get("/ocorrencias")
async def get_ocorrencias(
    tipo: Optional[str] = Query(None),
    criticidade: Optional[str] = Query(None),
    tecnico_id: Optional[int] = Query(None),
    resolvida: Optional[int] = Query(0),
    limit: int = Query(50),
):
    """Lista de ocorrências de auditoria com filtros."""
    db = get_db()
    conds = [f"resolvida={resolvida}"]
    if tipo:        conds.append(f"a.tipo='{tipo}'")
    if criticidade: conds.append(f"a.criticidade='{criticidade}'")
    if tecnico_id:  conds.append(f"a.tecnico_id={tecnico_id}")
    where = "WHERE " + " AND ".join(conds)

    rows = db.execute(f"""
        SELECT
            a.*,
            t.nome AS tecnico_nome,
            COALESCE(ass.assunto, '') AS assunto_nome
        FROM sais_auditorias a
        LEFT JOIN prod_tecnicos t ON t.id = a.tecnico_id
        LEFT JOIN prod_os_cache o ON o.ixc_os_id = a.os_id
        LEFT JOIN prod_assuntos ass ON ass.id = o.ixc_assunto_id
        {where}
        ORDER BY
            CASE criticidade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                             WHEN 'media' THEN 3 ELSE 4 END,
            criado_em DESC
        LIMIT ?
    """, (limit,)).fetchall()
    db.close()

    return {"ocorrencias": [dict(r) for r in rows], "total": len(rows)}


@router.get("/resumo")
async def get_resumo_auditoria():
    """Resumo de auditorias para o dashboard."""
    db = get_db()
    rows = db.execute("""
        SELECT tipo, criticidade, COUNT(*) as total
        FROM sais_auditorias
        WHERE resolvida=0
        GROUP BY tipo, criticidade
        ORDER BY CASE criticidade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                                  WHEN 'media' THEN 3 ELSE 4 END
    """).fetchall()

    totais = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN criticidade='critica' THEN 1 ELSE 0 END) AS criticas,
            SUM(CASE WHEN criticidade='alta'    THEN 1 ELSE 0 END) AS altas,
            SUM(CASE WHEN criticidade='media'   THEN 1 ELSE 0 END) AS medias,
            SUM(CASE WHEN resolvida=1           THEN 1 ELSE 0 END) AS resolvidas
        FROM sais_auditorias
    """).fetchone()

    db.close()
    return {"por_tipo": [dict(r) for r in rows], "totais": dict(totais)}


@router.post("/ocorrencias/{audit_id}/resolver")
async def resolver_ocorrencia(audit_id: int):
    """Marca ocorrência como resolvida."""
    db = get_db()
    db.execute("UPDATE sais_auditorias SET resolvida=1 WHERE id=?", (audit_id,))
    db.commit()
    db.close()
    return {"ok": True}


@router.get("/score-risco")
async def get_score_risco():
    """Score de risco por técnico baseado em auditorias."""
    db = get_db()
    rows = db.execute("""
        SELECT
            t.id, t.nome,
            COUNT(a.id) AS total_auditorias,
            SUM(CASE WHEN a.criticidade='critica' THEN 4
                     WHEN a.criticidade='alta'    THEN 2
                     WHEN a.criticidade='media'   THEN 1
                     ELSE 0 END) AS score_risco
        FROM prod_tecnicos t
        LEFT JOIN sais_auditorias a ON a.tecnico_id = t.id AND a.resolvida=0
        WHERE t.ativo=1
        GROUP BY t.id, t.nome
        ORDER BY score_risco DESC
    """).fetchall()
    db.close()
    return {"score_risco": [dict(r) for r in rows]}
