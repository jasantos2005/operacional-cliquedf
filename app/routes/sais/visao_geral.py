"""
SAIS — Visão Geral
Endpoints para o dashboard principal.
"""
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


@router.get("/resumo")
async def get_resumo(data: Optional[str] = Query(None)):
    """KPIs principais do dia para o dashboard."""
    db = get_db()
    data = data or hoje_brt()

    resumo = dict(db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas,
            SUM(CASE WHEN status='aguardando' THEN 1 ELSE 0 END) AS aguardando,
            SUM(CASE WHEN categoria='servico'  THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN categoria='suporte'  THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN categoria='infra'    THEN 1 ELSE 0 END) AS infra,
            SUM(CASE WHEN categoria='retirada' THEN 1 ELSE 0 END) AS retiradas
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (data,)).fetchone())

    meta_row = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia'"
    ).fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150

    fins  = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    efic  = round(fins / total * 100, 1) if total > 0 else 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    # Pontuação ponderada por assunto
    pontos_row = db.execute("""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS pontos
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p
            ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo = 1
        WHERE o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = ?
    """, (data,)).fetchone()

    total_pontos = pontos_row["pontos"] or 0 if pontos_row else 0

    # Técnicos ativos no dia
    tecs_ativos = db.execute("""
        SELECT COUNT(DISTINCT tecnico_id) as total
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (data,)).fetchone()

    # Alertas não lidos
    alertas = db.execute(
        "SELECT COUNT(*) as total FROM sais_alertas WHERE lido=0"
    ).fetchone()

    # Auditorias críticas não resolvidas
    audits = db.execute(
        "SELECT COUNT(*) as total FROM sais_auditorias WHERE criticidade IN ('critica','alta') AND resolvida=0"
    ).fetchone()

    db.close()

    return {
        "data": data,
        "resumo": resumo,
        "meta_dia": meta,
        "eficiencia": efic,
        "meta_percentual": pct_meta,
        "tecnicos_ativos": tecs_ativos["total"] if tecs_ativos else 0,
        "alertas_pendentes":   alertas["total"] if alertas else 0,
        "auditorias_criticas": audits["total"] if audits else 0,
        "total_pontos":        total_pontos,
    }


@router.get("/eventos-recentes")
async def get_eventos_recentes(limit: int = Query(20)):
    """Últimas OS finalizadas e eventos do dia."""
    db = get_db()
    data = hoje_brt()

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura, o.data_fechamento,
            t.nome AS tecnico,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
        ORDER BY COALESCE(o.data_fechamento, o.data_abertura) DESC
        LIMIT ?
    """, (data, limit)).fetchall()
    db.close()

    return {"eventos": [dict(r) for r in rows]}


@router.get("/alertas")
async def get_alertas(limit: int = Query(20), apenas_nao_lidos: bool = Query(False)):
    """Alertas do sistema."""
    db = get_db()
    where = "WHERE lido=0" if apenas_nao_lidos else ""
    rows = db.execute(f"""
        SELECT a.*, t.nome AS tecnico_nome
        FROM sais_alertas a
        LEFT JOIN prod_tecnicos t ON t.id = a.tecnico_id
        {where}
        ORDER BY
            CASE criticidade WHEN 'critico' THEN 1 WHEN 'aviso' THEN 2 ELSE 3 END,
            criado_em DESC
        LIMIT ?
    """, (limit,)).fetchall()
    db.close()
    return {"alertas": [dict(r) for r in rows]}


@router.post("/alertas/{alerta_id}/lido")
async def marcar_lido(alerta_id: int):
    """Marca alerta como lido."""
    db = get_db()
    db.execute("UPDATE sais_alertas SET lido=1 WHERE id=?", (alerta_id,))
    db.commit()
    db.close()
    return {"ok": True}


@router.get("/tecnico/{tecnico_id}")
async def get_modal_tecnico(tecnico_id: int, data: Optional[str] = Query(None)):
    """Dados completos para o modal do técnico."""
    db = get_db()
    data = data or hoje_brt()

    # Garantir que data é string
    data = str(data) if data else hoje_brt()

    tecnico = db.execute(
        "SELECT * FROM prod_tecnicos WHERE id=?", (tecnico_id,)
    ).fetchone()
    if not tecnico:
        db.close()
        return {"erro": "Técnico não encontrado"}

    # OS do dia
    os_hoje = dict(db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (tecnico_id, data)).fetchone())

    # Score do dia
    from app.engines.score_engine import calcular_pontos_tecnico as calcular_score_tecnico
    score = calcular_score_tecnico(tecnico_id, data)

    # OS recentes (últimas 10)
    os_recentes = db.execute("""
        SELECT o.ixc_os_id, o.status, o.categoria,
               o.data_abertura, o.data_fechamento, o.data_agenda,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.tecnico_id=?
        ORDER BY COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura) DESC
        LIMIT 10
    """, (tecnico_id,)).fetchall()

    # Agenda hoje e amanhã
    amanha = (datetime.strptime(data, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    agenda = db.execute("""
        SELECT o.ixc_os_id, o.data_agenda, o.status,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.tecnico_id=?
          AND o.status = 'agendada'
          AND DATE(o.data_agenda, '+3 hours') BETWEEN ? AND ?
        ORDER BY o.data_agenda
    """, (tecnico_id, data, amanha)).fetchall()

    # Auditorias do técnico
    auditorias = db.execute("""
        SELECT tipo, subtipo, criticidade, descricao, criado_em
        FROM sais_auditorias
        WHERE tecnico_id=? AND resolvida=0
        ORDER BY criado_em DESC LIMIT 10
    """, (tecnico_id,)).fetchall()

    # Histórico 7 dias
    from app.engines.score_engine import historico_tecnico
    historico = historico_tecnico(tecnico_id, dias=7)

    db.close()

    return {
        "tecnico": dict(tecnico),
        "hoje": os_hoje,
        "score": score,
        "os_recentes": [dict(r) for r in os_recentes],
        "agenda": [dict(r) for r in agenda],
        "auditorias": [dict(r) for r in auditorias],
        "historico_7d": historico,
    }


@router.get("/os/{os_id}")
async def get_modal_os(os_id: int):
    """Dados completos para o modal de uma OS."""
    db = get_db()

    os = db.execute("""
        SELECT
            o.*,
            t.nome AS tecnico_nome,
            t.meta_dia AS tecnico_meta,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS nome_assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.ixc_os_id = ?
    """, (os_id,)).fetchone()

    if not os:
        db.close()
        return {"erro": "OS não encontrada"}

    os_dict = dict(os)

    # SLA da OS
    from app.engines.sla_engine import calcular_sla
    sla = calcular_sla(os_dict)

    # Auditorias desta OS
    auditorias = db.execute("""
        SELECT tipo, subtipo, criticidade, descricao, valor_detectado, valor_esperado, criado_em
        FROM sais_auditorias
        WHERE os_id = ?
        ORDER BY criado_em DESC
    """, (os_id,)).fetchall()

    db.close()

    return {
        "os": os_dict,
        "sla": sla,
        "auditorias": [dict(r) for r in auditorias],
    }
