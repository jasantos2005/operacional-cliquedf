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




@router.get("/os/{os_id}")
async def get_os_agendada_detalhe(os_id: int):
    """Detalhe completo de uma OS agendada — busca dados do IXC em tempo real."""
    import os as _os
    import pymysql
    from datetime import datetime as dt

    db = get_db()

    # Busca dados locais
    local = db.execute("""
        SELECT o.*, t.nome AS tecnico_nome, t.ixc_funcionario_id,
               COALESCE(a.assunto, 'Assunto '||o.ixc_assunto_id) AS nome_assunto,
               s.horas_sla
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t     ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a     ON a.id = o.ixc_assunto_id
        LEFT JOIN sais_sla_config s   ON s.assunto_id = o.ixc_assunto_id
        WHERE o.ixc_os_id = ?
    """, (os_id,)).fetchone()
    db.close()

    if not local:
        return {"erro": "OS não encontrada"}

    loc = dict(local)
    agora_brt = datetime.now() + timedelta(hours=-3)

    # Tempo desde abertura
    tempo_abertura_h = None
    if loc.get("data_abertura"):
        try:
            ab = datetime.strptime(loc["data_abertura"][:19], "%Y-%m-%d %H:%M:%S")
            tempo_abertura_h = round((agora_brt - ab).total_seconds() / 3600, 1)
        except: pass

    # Tempo até execução (data_agenda)
    exec_em_h = None
    exec_label = None
    if loc.get("data_agenda"):
        try:
            ag_dt = datetime.strptime(loc["data_agenda"][:19], "%Y-%m-%d %H:%M:%S")
            diff_h = (ag_dt - agora_brt).total_seconds() / 3600
            exec_em_h = round(diff_h, 1)
            if diff_h >= 0:
                exec_label = f"+{round(diff_h, 1)}h"
            else:
                exec_label = f"{round(diff_h, 1)}h (atrasada)"
        except: pass

    # SLA
    horas_sla = loc.get("horas_sla") or 4.0
    pct_sla   = round(tempo_abertura_h / horas_sla * 100) if tempo_abertura_h else None
    status_sla = None
    if pct_sla is not None:
        status_sla = "no_prazo" if pct_sla <= 80 else "em_risco" if pct_sla <= 100 else "estourado"

    # Busca IXC em tempo real
    cliente_nome  = "—"
    id_cliente    = None
    hist_30       = 0
    hist_60       = 0
    padrao        = "Normal"
    alerta        = None

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=8,
        )
        with ixc.cursor() as cur:
            # Busca id_cliente e nome
            cur.execute("""
                SELECT o.id_cliente, c.razao AS cliente_nome
                FROM su_oss_chamado o
                LEFT JOIN cliente c ON c.id = o.id_cliente
                WHERE o.id = %s
            """, (os_id,))
            row = cur.fetchone()
            if row:
                cliente_nome = row.get("cliente_nome") or "—"
                id_cliente   = row.get("id_cliente")

            # Histórico do cliente (mesmo assunto, últimos 30/60 dias)
            if id_cliente:
                cur.execute("""
                    SELECT
                        SUM(CASE WHEN data_abertura >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS hist_30,
                        SUM(CASE WHEN data_abertura >= DATE_SUB(NOW(), INTERVAL 60 DAY) THEN 1 ELSE 0 END) AS hist_60
                    FROM su_oss_chamado
                    WHERE id_cliente = %s
                      AND id_assunto = (SELECT id_assunto FROM su_oss_chamado WHERE id = %s)
                      AND id != %s
                """, (id_cliente, os_id, os_id))
                hist = cur.fetchone()
                if hist:
                    hist_30 = int(hist.get("hist_30") or 0)
                    hist_60 = int(hist.get("hist_60") or 0)

        ixc.close()

        # Padrão e alerta
        if hist_30 >= 3:
            padrao = "Recorrente"
        elif hist_30 >= 1:
            padrao = "Ocasional"

        if status_sla == "estourado" and hist_30 >= 3:
            alerta = {"nivel": "critico", "msg": "Cliente com falha recorrente + SLA estourado → Prioridade máxima"}
        elif status_sla == "estourado":
            alerta = {"nivel": "aviso", "msg": "SLA estourado — priorizar atendimento"}
        elif hist_30 >= 3:
            alerta = {"nivel": "aviso", "msg": f"Cliente recorrente — {hist_30} ocorrências nos últimos 30 dias"}

    except Exception as e:
        alerta = {"nivel": "info", "msg": f"Histórico IXC indisponível: {str(e)[:60]}"}

    return {
        "os_id":          os_id,
        "nome_assunto":   loc["nome_assunto"],
        "tecnico_nome":   loc["tecnico_nome"] or "—",
        "status":         loc["status"],
        "cliente_nome":   cliente_nome,
        "data_abertura":  loc["data_abertura"],
        "data_agenda":    loc["data_agenda"],
        "tempo_abertura_h": tempo_abertura_h,
        "exec_em_h":      exec_em_h,
        "exec_label":     exec_label,
        "sla": {
            "horas_previstas": horas_sla,
            "pct":             pct_sla,
            "status":          status_sla,
        },
        "historico_cliente": {
            "ultimos_30": hist_30,
            "ultimos_60": hist_60,
            "padrao":     padrao,
        },
        "alerta": alerta,
    }


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
