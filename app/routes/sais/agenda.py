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


@router.get("/monitor")
async def get_monitor_agenda(
    data: Optional[str] = Query(None),
):
    """Monitor de agenda em tempo real — acompanhamento do cumprimento da agenda do dia."""
    import os as _os, pymysql
    from datetime import datetime as _dt, timedelta as _td
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    hoje = (_dt.now() + _td(hours=-3)).strftime("%Y-%m-%d")
    agora = (_dt.now() + _td(hours=-3)).strftime("%H:%M")
    data = data or hoje

    TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT",3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT
                    c.id,
                    f.funcionario AS tecnico,
                    cli.razao AS cliente,
                    a.assunto AS servico,
                    c.status,
                    c.data_prazo_limite,
                    c.data_hora_assumido,
                    c.data_hora_execucao,
                    c.data_fechamento,
                    c.data_reservada
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id = c.id_cliente
                LEFT JOIN funcionarios f ON f.id = c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id = c.id_assunto
                WHERE DATE(c.data_reservada) = %s
                  AND c.id_tecnico IN ({TECS_IDS})
                ORDER BY f.funcionario, c.data_prazo_limite
            """, (data,))
            rows = cur.fetchall()
        ixc.close()

        # Agrupa por técnico
        tecnicos = {}
        for r in rows:
            tec = r["tecnico"] or "Desconhecido"
            if tec not in tecnicos:
                tecnicos[tec] = {"os": [], "total": 0, "finalizadas": 0, "execucao": 0, "atrasadas": 0, "agendadas": 0}

            prazo = str(r["data_prazo_limite"])[11:16] if r["data_prazo_limite"] else "--"
            assumido = str(r["data_hora_assumido"])[11:16] if r["data_hora_assumido"] and str(r["data_hora_assumido"]) != "None" else None
            execucao = str(r["data_hora_execucao"])[11:16] if r["data_hora_execucao"] and str(r["data_hora_execucao"]) != "None" else None
            fechamento = str(r["data_fechamento"])[11:16] if r["data_fechamento"] and str(r["data_fechamento"])[:4] != "0000" else None

            # Determina status da OS
            status = r["status"]
            if status == "F":
                st_label = "finalizada"
            elif status in ("EX","E"):
                st_label = "execucao"
            elif status == "AS":
                st_label = "assumida"
            elif status == "AG" and execucao:
                st_label = "execucao"
            elif status == "AG" and assumido:
                st_label = "assumida"
            elif status == "AG" and prazo < agora and data == hoje and not assumido:
                st_label = "atrasada"
            else:
                st_label = "agendada"

            tecnicos[tec]["total"] += 1
            if st_label == "finalizada":   tecnicos[tec]["finalizadas"] += 1
            elif st_label == "execucao":   tecnicos[tec]["execucao"] += 1
            elif st_label == "atrasada":   tecnicos[tec]["atrasadas"] += 1
            else:                          tecnicos[tec]["agendadas"] += 1

            tecnicos[tec]["os"].append({
                "os_id":      r["id"],
                "servico":    r["servico"] or "—",
                "cliente":    r["cliente"] or "—",
                "prazo":      prazo,
                "assumido":   assumido or "--",
                "execucao":   execucao or "--",
                "fechamento": fechamento or "--",
                "status":     st_label,
            })

        # Monta resultado
        resultado = []
        for nome, info in tecnicos.items():
            total = info["total"] or 1
            pct = round(info["finalizadas"] / total * 100)
            # Previsão baseada no horário
            hora_atual = int(agora[:2]) if agora != "--" else 12
            progresso_esperado = min(max((hora_atual - 8) * 10, 0), 100)
            previsao = "NO PRAZO" if pct >= progresso_esperado else "ATRASADO"
            if pct == 100: previsao = "FINALIZADO"

            resultado.append({
                "tecnico":      nome,
                "total":        info["total"],
                "finalizadas":  info["finalizadas"],
                "execucao":     info["execucao"],
                "atrasadas":    info["atrasadas"],
                "agendadas":    info["agendadas"],
                "pct":          pct,
                "previsao":     previsao,
                "os":           info["os"],
            })

        resultado.sort(key=lambda x: -x["pct"])

        return {
            "data":          data,
            "agora":         agora,
            "total_tecnicos": len(resultado),
            "tecnicos":      resultado,
        }

    except Exception as e:
        print(f"ERRO monitor agenda: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "tecnicos": []}


@router.get("/reincidencias")
async def get_reincidencias_agenda(
    dias: Optional[int] = Query(45),
    data: Optional[str] = Query(None),
):
    """
    Reincidências na agenda:
    - Busca OS abertas hoje (ou na data) agendadas para os técnicos
    - Verifica se o mesmo cliente teve OS de sem acesso/internet lenta nos últimos X dias
    - Retorna lista com risco de cancelamento
    """
    import os as _os, pymysql
    from datetime import datetime as _dt, timedelta as _td
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    hoje = (_dt.now() + _td(hours=-3)).strftime("%Y-%m-%d")
    data = data or hoje
    data_limite = (_dt.strptime(data, "%Y-%m-%d") - _td(days=dias)).strftime("%Y-%m-%d")

    TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

    # IDs de assuntos que caracterizam reincidência
    ASSUNTOS_REIN = (20, 21, 5, 44, 47, 103, 148, 160, 226)

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=15
        )

        with ixc.cursor() as cur:
            # OS abertas hoje (agendadas OU em aberto)
            cur.execute(f"""
                SELECT
                    c.id AS os_id,
                    c.id_cliente,
                    cli.razao AS cliente,
                    f.funcionario AS tecnico,
                    a.assunto AS servico,
                    c.status,
                    c.status_sla,
                    c.protocolo,
                    c.data_reservada,
                    c.data_prazo_limite,
                    c.data_abertura,
                    (SELECT cc.id FROM cliente_contrato cc WHERE cc.id_cliente=c.id_cliente AND cc.status='A' LIMIT 1) AS id_contrato,
                    (SELECT COUNT(*) FROM su_oss_chamado_mensagem m WHERE m.id_chamado=c.id) AS interacoes
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id = c.id_cliente
                LEFT JOIN funcionarios f ON f.id = c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id = c.id_assunto
                WHERE (
                    DATE(c.data_reservada) = %s
                    OR (c.status NOT IN ('F','C') AND c.id_tecnico IN ({TECS_IDS}))
                )
                AND c.id_tecnico IN ({TECS_IDS})
                AND c.status NOT IN ('F','C')
                ORDER BY c.data_abertura ASC
            """, (data,))
            os_hoje = cur.fetchall()

            if not os_hoje:
                ixc.close()
                return {"data": data, "dias": dias, "reincidencias": [], "total": 0}

            # IDs de clientes únicos
            ids_clientes = list(set(r["id_cliente"] for r in os_hoje if r["id_cliente"]))
            if not ids_clientes:
                ixc.close()
                return {"data": data, "dias": dias, "reincidencias": [], "total": 0}

            ph = ",".join(["%s"] * len(ids_clientes))
            ph_ass = ",".join([str(i) for i in ASSUNTOS_REIN])

            # Histórico de OS de problema nos últimos X dias para esses clientes
            cur.execute(f"""
                SELECT
                    c.id AS os_id,
                    c.id_cliente,
                    cli.razao AS cliente,
                    f.funcionario AS tecnico,
                    a.assunto AS servico,
                    c.id_assunto,
                    c.status,
                    c.data_abertura,
                    c.data_fechamento,
                    c.mensagem_resposta AS solucao
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id = c.id_cliente
                LEFT JOIN funcionarios f ON f.id = c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id = c.id_assunto
                WHERE c.id_cliente IN ({ph})
                  AND c.id_assunto IN ({ph_ass})
                  AND c.data_abertura >= %s
                  AND c.data_abertura < %s
                ORDER BY c.id_cliente, c.data_abertura DESC
            """, ids_clientes + [data_limite, data])
            historico = cur.fetchall()

        ixc.close()

        # Monta dicionário de histórico por cliente
        hist_por_cliente = {}
        for h in historico:
            cid = h["id_cliente"]
            if cid not in hist_por_cliente:
                hist_por_cliente[cid] = []
            hist_por_cliente[cid].append({
                "os_id":     h["os_id"],
                "servico":   h["servico"] or "—",
                "tecnico":   h["tecnico"] or "—",
                "status":    h["status"],
                "abertura":  str(h["data_abertura"])[:10] if h["data_abertura"] else "—",
                "fechamento": str(h["data_fechamento"])[:10] if h["data_fechamento"] and str(h["data_fechamento"])[:4] != "0000" else "—",
                "solucao":   (h["solucao"] or "Sem descrição")[:200],
            })

        # Monta reincidências
        reincidencias = []
        for os in os_hoje:
            cid = os["id_cliente"]
            hist = hist_por_cliente.get(cid, [])
            if not hist:
                continue

            # Calcula risco
            qtd_hist = len(hist)
            if qtd_hist >= 3:
                risco = "ALTO"
                risco_cor = "var(--red)"
            elif qtd_hist == 2:
                risco = "MÉDIO"
                risco_cor = "var(--amber)"
            else:
                risco = "BAIXO"
                risco_cor = "var(--cyan)"

            prazo = str(os["data_prazo_limite"])[11:16] if os["data_prazo_limite"] else "--"

            # Calcula dias em aberto
            dias_aberto = 0
            if os.get("data_abertura"):
                from datetime import datetime as _dtx
                try:
                    dias_aberto = (_dtx.now() - _dtx.strptime(str(os["data_abertura"])[:10], "%Y-%m-%d")).days
                except:
                    dias_aberto = 0

            reincidencias.append({
                "os_id":       os["os_id"],
                "protocolo":   os["protocolo"] or "—",
                "id_contrato": os["id_contrato"] or "—",
                "cliente":     os["cliente"] or "—",
                "tecnico":     os["tecnico"] or "—",
                "servico":     os["servico"] or "—",
                "status":      os["status"],
                "status_sla":  os["status_sla"] or "",
                "prazo":       prazo,
                "abertura":    str(os["data_abertura"])[:10] if os["data_abertura"] else "—",
                "dias_aberto": dias_aberto,
                "interacoes":  int(os["interacoes"] or 0),
                "qtd_historico": qtd_hist,
                "risco":       risco,
                "risco_cor":   risco_cor,
                "historico":   hist,
            })

        # Ordena por risco
        ordem_risco = {"ALTO": 0, "MÉDIO": 1, "BAIXO": 2}
        reincidencias.sort(key=lambda x: ordem_risco.get(x["risco"], 3))

        return {
            "data":          data,
            "dias":          dias,
            "total":         len(reincidencias),
            "alto_risco":    sum(1 for r in reincidencias if r["risco"] == "ALTO"),
            "medio_risco":   sum(1 for r in reincidencias if r["risco"] == "MÉDIO"),
            "reincidencias": reincidencias,
        }

    except Exception as e:
        print(f"ERRO reincidencias: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "reincidencias": [], "total": 0}


@router.get("/os-detalhe/{os_id}")
async def get_os_detalhe(os_id: int):
    """Detalhes completos de uma OS: info + interações."""
    import os as _os, pymysql
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT",3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )
        with ixc.cursor() as cur:
            # Info da OS
            cur.execute("""
                SELECT c.id, c.id_cliente, c.status, c.status_sla, c.protocolo,
                       c.data_abertura, c.data_prazo_limite, c.data_fechamento,
                       c.mensagem_resposta AS solucao,
                       a.assunto, cli.razao AS cliente, f.funcionario AS tecnico,
                       (SELECT cc.id FROM cliente_contrato cc WHERE cc.id_cliente=c.id_cliente AND cc.status='A' LIMIT 1) AS id_contrato
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id=c.id_cliente
                LEFT JOIN funcionarios f ON f.id=c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id=c.id_assunto
                WHERE c.id=%s
            """, (os_id,))
            os_info = cur.fetchone()

            # Interações
            cur.execute("""
                SELECT m.id, m.data, m.status, m.mensagem, m.historico,
                       f.funcionario AS operador
                FROM su_oss_chamado_mensagem m
                LEFT JOIN funcionarios f ON f.id=m.id_operador
                WHERE m.id_chamado=%s
                ORDER BY m.data DESC
                LIMIT 20
            """, (os_id,))
            interacoes = cur.fetchall()

        ixc.close()

        STATUS_MAP = {
            'AB':'Aberta','AG':'Agendada','EN':'Encaminhada',
            'AS':'Assumida','EX':'Em Execução','F':'Finalizada',
            'C':'Cancelada','RAG':'Reagendada','RE':'Reaberta'
        }

        return {
            "os": {
                "id":          os_info["id"] if os_info else os_id,
                "protocolo":   os_info["protocolo"] if os_info else "—",
                "id_contrato": os_info["id_contrato"] if os_info else "—",
                "assunto":     os_info["assunto"] if os_info else "—",
                "cliente":     os_info["cliente"] if os_info else "—",
                "tecnico":     os_info["tecnico"] if os_info else "—",
                "status":      STATUS_MAP.get(os_info["status"],"—") if os_info else "—",
                "status_sla":  os_info["status_sla"] if os_info else "",
                "abertura":    str(os_info["data_abertura"])[:16] if os_info and os_info["data_abertura"] else "—",
                "prazo":       str(os_info["data_prazo_limite"])[:16] if os_info and os_info["data_prazo_limite"] else "—",
                "fechamento":  str(os_info["data_fechamento"])[:16] if os_info and os_info["data_fechamento"] and str(os_info["data_fechamento"])[:4]!="0000" else "—",
                "solucao":     (os_info["solucao"] or "Sem solução registrada")[:300] if os_info else "—",
            },
            "interacoes": [{
                "data":     str(r["data"])[:16] if r["data"] else "—",
                "status":   STATUS_MAP.get(r["status"], r["status"] or "—"),
                "operador": r["operador"] or "Sistema",
                "mensagem": (r["mensagem"] or r["historico"] or "—")[:200],
            } for r in interacoes],
        }
    except Exception as e:
        print(f"ERRO os-detalhe: {e}")
        return {"erro": str(e)}


@router.get("/qualidade-instalacao")
async def get_qualidade_instalacao(
    dias: Optional[int] = Query(60),
):
    """
    Qualidade pós-instalação:
    - Busca clientes instalados nos últimos X dias
    - Verifica se abriram OS de suporte após a instalação
    - Ranking de técnicos com mais retornos
    """
    import os as _os, pymysql
    from datetime import datetime as _dt, timedelta as _td
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    hoje = (_dt.now() + _td(hours=-3)).strftime("%Y-%m-%d")
    data_limite = (_dt.now() + _td(hours=-3) - _td(days=dias)).strftime("%Y-%m-%d")

    TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

    # IDs de assuntos de instalação
    ASSUNTOS_INST = (227, 2, 3, 15, 19, 49, 232, 75)

    # IDs de assuntos de suporte/problema
    ASSUNTOS_SUP = (20, 21, 5, 44, 47, 103, 148, 160, 226, 16)

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=15
        )

        ph_inst = ",".join(str(i) for i in ASSUNTOS_INST)
        ph_sup  = ",".join(str(i) for i in ASSUNTOS_SUP)

        with ixc.cursor() as cur:
            # Instalações finalizadas nos últimos X dias
            cur.execute(f"""
                SELECT
                    c.id AS os_id,
                    c.id_cliente,
                    cli.razao AS cliente,
                    f.funcionario AS tecnico_inst,
                    f.id AS tecnico_inst_id,
                    a.assunto AS servico_inst,
                    c.data_fechamento AS data_inst,
                    (SELECT cc.id FROM cliente_contrato cc WHERE cc.id_cliente=c.id_cliente AND cc.status='A' LIMIT 1) AS id_contrato
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id=c.id_cliente
                LEFT JOIN funcionarios f ON f.id=c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id=c.id_assunto
                WHERE c.id_assunto IN ({ph_inst})
                  AND c.status = 'F'
                  AND c.id_tecnico IN ({TECS_IDS})
                  AND DATE(c.data_fechamento) BETWEEN %s AND %s
                ORDER BY c.data_fechamento DESC
            """, (data_limite, hoje))
            instalacoes = cur.fetchall()

            if not instalacoes:
                ixc.close()
                return {"dias": dias, "retornos": [], "ranking_tecnicos": [], "total": 0}

            # IDs de clientes instalados
            ids_clientes = list(set(r["id_cliente"] for r in instalacoes if r["id_cliente"]))
            if not ids_clientes:
                ixc.close()
                return {"dias": dias, "retornos": [], "ranking_tecnicos": [], "total": 0}

            ph_cli = ",".join(["%s"] * len(ids_clientes))

            # OS de suporte APÓS a instalação
            cur.execute(f"""
                SELECT
                    c.id AS os_id,
                    c.id_cliente,
                    cli.razao AS cliente,
                    f.funcionario AS tecnico_sup,
                    a.assunto AS servico_sup,
                    c.id_assunto,
                    c.status,
                    c.status_sla,
                    c.protocolo,
                    c.data_abertura,
                    c.data_prazo_limite,
                    c.data_fechamento,
                    c.mensagem_resposta AS solucao,
                    (SELECT cc.id FROM cliente_contrato cc WHERE cc.id_cliente=c.id_cliente AND cc.status='A' LIMIT 1) AS id_contrato,
                    (SELECT COUNT(*) FROM su_oss_chamado_mensagem m WHERE m.id_chamado=c.id) AS interacoes
                FROM su_oss_chamado c
                LEFT JOIN cliente cli ON cli.id=c.id_cliente
                LEFT JOIN funcionarios f ON f.id=c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id=c.id_assunto
                WHERE c.id_cliente IN ({ph_cli})
                  AND c.id_assunto IN ({ph_sup})
                  AND c.data_abertura >= %s
                ORDER BY c.data_abertura DESC
            """, ids_clientes + [data_limite])
            os_suporte = cur.fetchall()

        ixc.close()

        # Monta mapa de instalação por cliente
        inst_map = {}
        for inst in instalacoes:
            cid = inst["id_cliente"]
            if cid not in inst_map:
                inst_map[cid] = inst

        # Cruza OS de suporte com instalação do cliente
        retornos = []
        ranking_tec = {}

        for os in os_suporte:
            cid = os["id_cliente"]
            inst = inst_map.get(cid)
            if not inst:
                continue

            # Verifica se OS de suporte é APÓS a instalação
            try:
                from datetime import datetime as _dtx
                dt_inst = _dtx.strptime(str(inst["data_inst"])[:10], "%Y-%m-%d")
                dt_sup  = _dtx.strptime(str(os["data_abertura"])[:10], "%Y-%m-%d")
                if dt_sup <= dt_inst:
                    continue
                dias_apos = (dt_sup - dt_inst).days
                dias_aberto = (_dtx.now() - dt_sup).days
            except:
                dias_apos = 0
                dias_aberto = 0

            # Ranking de técnicos de instalação
            tec = inst["tecnico_inst"] or "Desconhecido"
            if tec not in ranking_tec:
                ranking_tec[tec] = {"nome": tec, "total": 0, "clientes": set()}
            ranking_tec[tec]["total"] += 1
            ranking_tec[tec]["clientes"].add(cid)

            retornos.append({
                "os_id":        os["os_id"],
                "protocolo":    os["protocolo"] or "—",
                "id_contrato":  os["id_contrato"] or "—",
                "cliente":      os["cliente"] or "—",
                "tecnico_inst": inst["tecnico_inst"] or "—",
                "tecnico_sup":  os["tecnico_sup"] or "—",
                "servico_inst": inst["servico_inst"] or "—",
                "servico_sup":  os["servico_sup"] or "—",
                "data_inst":    str(inst["data_inst"])[:10],
                "abertura_sup": str(os["data_abertura"])[:10],
                "dias_apos":    dias_apos,
                "dias_aberto":  dias_aberto,
                "status":       os["status"],
                "status_sla":   os["status_sla"] or "",
                "interacoes":   int(os["interacoes"] or 0),
                "solucao":      (os["solucao"] or "Sem solução registrada")[:200],
            })

        # Conta instalações por técnico no período
        inst_por_tec = {}
        for inst in instalacoes:
            tec = inst["tecnico_inst"] or "Desconhecido"
            if tec not in inst_por_tec:
                inst_por_tec[tec] = 0
            inst_por_tec[tec] += 1

        # Finaliza ranking com total de instalações e taxa
        ranking_final = []
        for nome, v in ranking_tec.items():
            total_inst_tec = inst_por_tec.get(nome, 0)
            taxa = round(v["total"] / total_inst_tec * 100, 1) if total_inst_tec > 0 else 0
            ranking_final.append({
                "nome":        nome,
                "total":       v["total"],
                "clientes":    len(v["clientes"]),
                "instalacoes": total_inst_tec,
                "taxa":        taxa,
            })
        ranking_final.sort(key=lambda x: -x["total"])

        # Ordena retornos por dias_apos (problemas mais rápidos primeiro)
        retornos.sort(key=lambda x: x["dias_apos"])

        return {
            "dias":             dias,
            "total":            len(retornos),
            "total_instalacoes": len(instalacoes),
            "retornos":         retornos[:50],
            "ranking_tecnicos": ranking_final,
        }

    except Exception as e:
        print(f"ERRO qualidade-instalacao: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "retornos": [], "ranking_tecnicos": [], "total": 0}
