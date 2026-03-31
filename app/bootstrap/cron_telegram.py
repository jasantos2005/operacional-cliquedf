#!/usr/bin/env python3
"""
SAIS — cron_telegram.py
Envia relatórios diários às 18:30 BRT via Telegram.
Cada --tipo envia uma mensagem separada.

Crontabs (UTC — BRT+3):
30 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=producao
32 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=ranking
34 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=eficiencia
36 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=qualidade
38 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=alertas
40 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=auditoria
42 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=sla
44 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=destaques
46 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=reincidencias
48 21 * * 1-5  python -m app.bootstrap.cron_telegram --tipo=performance
"""

import sys, os, sqlite3, requests, logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [TG] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")

def agora_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%H:%M")

def send(msg: str):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    r = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }, timeout=15)
    if r.status_code != 200:
        log.error(f"Erro Telegram {r.status_code}: {r.text}")
    else:
        log.info("✅ Mensagem enviada")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ic(pct):
    if pct >= 100: return "🏆"
    if pct >= 80:  return "✅"
    if pct >= 50:  return "🟡"
    return "🔴"

def fmt_data(d):
    return d[8:10] + "/" + d[5:7] if d else "—"

# ══════════════════════════════════════════════════════
# 1. PRODUÇÃO DO DIA
# ══════════════════════════════════════════════════════
def msg_producao():
    db = get_db()
    data = hoje_brt()
    r = dict(db.execute("""
        SELECT COUNT(*) total,
               SUM(status='finalizada') fins,
               SUM(status='aberta') abertas,
               SUM(status='agendada') agendadas,
               SUM(categoria='servico') servicos,
               SUM(categoria='suporte') suportes,
               SUM(categoria='infra') infra,
               SUM(categoria='retirada') retiradas
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento,data_agenda,data_abertura),'+3 hours')=?
    """, (data,)).fetchone())

    meta = int(db.execute("SELECT valor FROM sais_config WHERE chave='meta_dia'").fetchone()["valor"] or 150)
    fins = r["fins"] or 0
    pct = round(fins/meta*100) if meta else 0

    tecs = db.execute(f"""
        SELECT t.nome,
               COUNT(o.id) total,
               SUM(o.status='finalizada') fins,
               COALESCE(SUM(p.pontos_final),0) pts
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id=t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours')=?
        LEFT JOIN sais_os_pontuacao p ON p.os_id=o.ixc_os_id
        WHERE t.ativo=1
        GROUP BY t.id ORDER BY fins DESC
    """, (data,)).fetchall()
    db.close()

    linhas = "\n".join(
        f"  {'🥇' if i==0 else '🥈' if i==1 else '🥉' if i==2 else '▫️'} "
        f"{t['nome'].split()[0]}: <b>{t['fins'] or 0}</b> OS · {t['pts'] or 0}pts"
        for i, t in enumerate(tecs) if (t['total'] or 0) > 0
    ) or "  Nenhuma OS executada hoje"

    return (
        f"📊 <b>Produção do Dia — Cliquedf</b>\n"
        f"📅 {fmt_data(data)} · {agora_brt()}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"🎯 Meta: <b>{fins}/{meta}</b> OS ({ic(pct)} {pct}%)\n"
        f"✅ Finalizadas: <b>{fins}</b>\n"
        f"📋 Total dia: <b>{r['total'] or 0}</b>\n"
        f"🔧 Serviço: {r['servicos'] or 0} · Suporte: {r['suportes'] or 0} · "
        f"Infra: {r['infra'] or 0} · Retirada: {r['retiradas'] or 0}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Por técnico:</b>\n{linhas}"
    )

# ══════════════════════════════════════════════════════
# 2. RANKING DO DIA
# ══════════════════════════════════════════════════════
def msg_ranking():
    db = get_db()
    data = hoje_brt()
    tecs = db.execute("""
        SELECT t.nome, t.meta_dia,
               COUNT(o.id) total,
               SUM(o.status='finalizada') fins,
               COALESCE(SUM(p.pontos_final),0) pts,
               ROUND(AVG(o.sla_tecnico_min),0) sla_med
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id=t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours')=?
        LEFT JOIN sais_os_pontuacao p ON p.os_id=o.ixc_os_id
        WHERE t.ativo=1
        GROUP BY t.id ORDER BY pts DESC, fins DESC
    """, (data,)).fetchall()
    db.close()

    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    linhas = []
    for i, t in enumerate(tecs):
        fins = t["fins"] or 0
        pts  = t["pts"] or 0
        meta = t["meta_dia"] or 80
        pct  = round(pts/meta*100) if meta else 0
        sla  = f"{int(t['sla_med'])}min" if t["sla_med"] else "—"
        linhas.append(
            f"  {medals[i]} {t['nome'].split()[0]}: "
            f"<b>{fins} OS</b> · {pts}pts · {ic(pct)}{pct}% · ⏱{sla}"
        )

    return (
        f"🏆 <b>Ranking do Dia — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        + "\n".join(linhas) +
        f"\n━━━━━━━━━━━━━━━━━\n"
        f"<i>OS · Pontos · %Meta · Tempo médio</i>"
    )

# ══════════════════════════════════════════════════════
# 3. EFICIÊNCIA OPERACIONAL
# ══════════════════════════════════════════════════════
def msg_eficiencia():
    db = get_db()
    data = hoje_brt()
    rows = db.execute("""
        SELECT t.nome,
               COUNT(o.id) total,
               SUM(o.status='finalizada') fins,
               ROUND(AVG(o.sla_tecnico_min),0) sla_tec,
               ROUND(AVG(o.sla_fila_min),0) sla_fila,
               ROUND(AVG(o.sla_exec_min),0) sla_exec,
               COALESCE(SUM(p.pontos_final),0) pts,
               COALESCE(SUM(p.pontos_base),0) pts_base
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id=t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours')=?
            AND o.status='finalizada'
        LEFT JOIN sais_os_pontuacao p ON p.os_id=o.ixc_os_id
        WHERE t.ativo=1
        GROUP BY t.id
        HAVING fins > 0
        ORDER BY pts DESC
    """, (data,)).fetchall()
    db.close()

    if not rows:
        return (
            f"⚡ <b>Eficiência Operacional — Cliquedf</b>\n"
            f"📅 {fmt_data(data)}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Nenhuma OS finalizada hoje ainda."
        )

    linhas = []
    for t in rows:
        aprov = round((t["pts"] or 0)/(t["pts_base"] or 1)*100) if t["pts_base"] else 0
        sla   = f"{int(t['sla_tec'])}min" if t["sla_tec"] else "—"
        fila  = f"{int(t['sla_fila'])}min" if t["sla_fila"] else "—"
        linhas.append(
            f"  👷 <b>{t['nome'].split()[0]}</b>\n"
            f"    OS: {t['fins']} · Tempo: {sla} · Aproveit: {aprov}%\n"
            f"    Fila: {fila} · Pontos: {t['pts']}"
        )

    return (
        f"⚡ <b>Eficiência Operacional — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        + "\n".join(linhas)
    )

# ══════════════════════════════════════════════════════
# 4. QUALIDADE DE INSTALAÇÕES
# ══════════════════════════════════════════════════════
def msg_qualidade():
    db = get_db()
    # Busca retornos dos últimos 30 dias
    try:
        import pymysql
        ixc = pymysql.connect(
            host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT",3306)),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"), cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=8
        )
        with ixc.cursor() as cur:
            # Instalações dos últimos 30 dias
            cur.execute("""
                SELECT COUNT(*) AS instalacoes
                FROM su_oss_chamado inst
                WHERE inst.id_assunto IN (2,14,15,49,227,239)
                  AND inst.status='F'
                  AND inst.data_fechamento >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  AND inst.id_tecnico IN (13,17,11,38,47,50,35,56,55,46,19)
            """)
            total_inst_row = cur.fetchone()

            # Retornos: clientes que tiveram suporte até 30 dias após instalação
            cur.execute("""
                SELECT COUNT(DISTINCT sup.id) AS retornos
                FROM su_oss_chamado inst
                JOIN su_oss_chamado sup ON sup.id_cliente = inst.id_cliente
                WHERE inst.id_assunto IN (2,14,15,49,227,239)
                  AND inst.status = 'F'
                  AND inst.data_fechamento >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  AND inst.id_tecnico IN (13,17,11,38,47,50,35,56,55,46,19)
                  AND sup.id_assunto IN (20,21,44,47,94,103,105,107)
                  AND sup.data_abertura > inst.data_fechamento
                  AND sup.data_abertura <= DATE_ADD(inst.data_fechamento, INTERVAL 30 DAY)
                  AND sup.id != inst.id
            """)
            total_ret_row = cur.fetchone()
            r = {
                "instalacoes": total_inst_row["instalacoes"],
                "retornos": total_ret_row["retornos"]
            }

            # Por técnico
            cur.execute("""
                SELECT f.funcionario AS nome,
                       COUNT(DISTINCT inst.id) AS instalacoes,
                       COUNT(DISTINCT sup.id) AS retornos
                FROM su_oss_chamado inst
                JOIN funcionarios f ON f.id = inst.id_tecnico
                LEFT JOIN su_oss_chamado sup ON sup.id_cliente = inst.id_cliente
                    AND sup.id_assunto IN (20,21,44,47,94,103,105,107)
                    AND sup.data_abertura > inst.data_fechamento
                    AND sup.data_abertura <= DATE_ADD(inst.data_fechamento, INTERVAL 30 DAY)
                    AND sup.id != inst.id
                WHERE inst.id_assunto IN (2,14,15,49,227,239)
                  AND inst.status = 'F'
                  AND inst.data_fechamento >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  AND inst.id_tecnico IN (13,17,11,38,47,50,35,56,55,46,19)
                GROUP BY inst.id_tecnico
                HAVING instalacoes > 0
                ORDER BY retornos DESC
            """)
            tecs = cur.fetchall()
        ixc.close()
    except Exception as e:
        db.close()
        return f"📦 <b>Qualidade de Instalações</b>\n❌ Erro: {e}"

    db.close()
    total_inst = r["instalacoes"] or 0
    total_ret  = r["retornos"] or 0
    taxa = round(total_ret/total_inst*100, 1) if total_inst else 0
    cor_taxa = "🟢" if taxa <= 5 else "🟡" if taxa <= 10 else "🔴"

    linhas = []
    for t in tecs[:8]:
        inst = t["instalacoes"] or 0
        ret  = t["retornos"] or 0
        tx   = round(ret/inst*100, 1) if inst else 0
        cor  = "🟢" if tx <= 5 else "🟡" if tx <= 10 else "🔴"
        linhas.append(f"  {cor} {t['nome'].split()[0]}: {ret}/{inst} ({tx}%)")

    return (
        f"📦 <b>Qualidade de Instalações — 30 dias</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📊 Total inst: <b>{total_inst}</b> · Retornos: <b>{total_ret}</b>\n"
        f"📉 Taxa retorno: {cor_taxa} <b>{taxa}%</b> (meta: ≤5%)\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Por técnico:</b>\n"
        + "\n".join(linhas)
    )

# ══════════════════════════════════════════════════════
# 5. CENTRAL DE ALERTAS
# ══════════════════════════════════════════════════════
def msg_alertas():
    db = get_db()
    total = db.execute("SELECT COUNT(*) n FROM sais_alertas WHERE lido=0").fetchone()["n"]
    criticos = db.execute(
        "SELECT COUNT(*) n FROM sais_alertas WHERE lido=0 AND criticidade='critico'"
    ).fetchone()["n"]
    rows = db.execute("""
        SELECT a.titulo, a.mensagem, a.criticidade, t.nome AS tecnico_nome
        FROM sais_alertas a
        LEFT JOIN prod_tecnicos t ON t.id=a.tecnico_id
        WHERE a.lido=0
        ORDER BY CASE a.criticidade WHEN 'critico' THEN 1 WHEN 'aviso' THEN 2 ELSE 3 END,
                 a.criado_em DESC
        LIMIT 8
    """).fetchall()
    db.close()

    linhas = []
    for a in rows:
        ic2 = "🔴" if a["criticidade"]=="critico" else "🟡"
        linhas.append(f"  {ic2} {a['titulo']}\n     {(a['mensagem'] or '')[:60]}")

    return (
        f"🚨 <b>Central de Alertas — Cliquedf</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📊 Pendentes: <b>{total}</b> · Críticos: <b>{criticos}</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        + "\n".join(linhas) +
        (f"\n<i>...e mais {total-8}</i>" if total > 8 else "")
    )

# ══════════════════════════════════════════════════════
# 6. AUDITORIA DO DIA
# ══════════════════════════════════════════════════════
def msg_auditoria():
    db = get_db()
    data = hoje_brt()
    tot = dict(db.execute("""
        SELECT COUNT(*) total,
               SUM(criticidade='critica') criticas,
               SUM(criticidade='alta') altas,
               SUM(criticidade='media') medias,
               SUM(resolvida=1) resolvidas
        FROM sais_auditorias
        WHERE DATE(criado_em)=?
    """, (data,)).fetchone())

    por_tec = db.execute("""
        SELECT t.nome, COUNT(*) total,
               SUM(a.criticidade='critica') criticas
        FROM sais_auditorias a
        JOIN prod_tecnicos t ON t.id=a.tecnico_id
        WHERE DATE(a.criado_em)=? AND a.resolvida=0
        GROUP BY a.tecnico_id ORDER BY criticas DESC, total DESC
        LIMIT 6
    """, (data,)).fetchall()
    db.close()

    linhas = "\n".join(
        f"  {'🔴' if t['criticas'] else '🟡'} {t['nome'].split()[0]}: "
        f"{t['total']} ocorr. ({t['criticas']} críticas)"
        for t in por_tec
    ) or "  Nenhuma ocorrência hoje"

    return (
        f"🔍 <b>Auditoria do Dia — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📊 Total: {tot['total'] or 0} · "
        f"🔴 Críticas: {tot['criticas'] or 0} · "
        f"🟡 Altas: {tot['altas'] or 0}\n"
        f"✅ Resolvidas: {tot['resolvidas'] or 0}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Por técnico:</b>\n{linhas}"
    )

# ══════════════════════════════════════════════════════
# 7. SLA DO DIA
# ══════════════════════════════════════════════════════
def msg_sla():
    db = get_db()
    data = hoje_brt()
    rows = db.execute("""
        SELECT t.nome,
               COUNT(o.id) total,
               ROUND(AVG(CASE WHEN o.sla_fila_min<=480 THEN o.sla_fila_min END),0) fila,
               ROUND(AVG(CASE WHEN o.sla_desloc_min<=480 THEN o.sla_desloc_min END),0) desloc,
               ROUND(AVG(CASE WHEN o.sla_exec_min<=480 THEN o.sla_exec_min END),0) exec_,
               ROUND(AVG(CASE WHEN o.sla_tecnico_min<=480 THEN o.sla_tecnico_min END),0) tec
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id=t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours')=?
            AND o.status='finalizada'
        WHERE t.ativo=1
        GROUP BY t.id HAVING total>0
        ORDER BY tec
    """, (data,)).fetchall()

    # OS críticas abertas
    criticas = db.execute("""
        SELECT COUNT(*) n FROM prod_os_cache
        WHERE status IN ('aberta','execucao')
          AND sla_tecnico_min > 240
    """).fetchone()["n"]
    db.close()

    if not rows:
        return (
            f"⏱ <b>SLA do Dia — Cliquedf</b>\n"
            f"📅 {fmt_data(data)}\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"Nenhuma OS finalizada hoje.\n"
            f"⚠️ OS com SLA crítico abertas: <b>{criticas}</b>"
        )

    linhas = []
    for t in rows:
        tec = f"{int(t['tec'])}min" if t["tec"] else "—"
        fila = f"{int(t['fila'])}min" if t["fila"] else "—"
        desloc = f"{int(t['desloc'])}min" if t["desloc"] else "—"
        cor = "🟢" if t["tec"] and t["tec"]<=60 else "🟡" if t["tec"] and t["tec"]<=120 else "🔴"
        linhas.append(
            f"  {cor} {t['nome'].split()[0]}: "
            f"técnico={tec} · fila={fila} · desloc={desloc}"
        )

    return (
        f"⏱ <b>SLA do Dia — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"⚠️ OS com SLA crítico abertas: <b>{criticas}</b>\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Tempo médio por técnico:</b>\n"
        + "\n".join(linhas)
    )

# ══════════════════════════════════════════════════════
# 8. DESTAQUES DO DIA
# ══════════════════════════════════════════════════════
def msg_destaques():
    db = get_db()
    data = hoje_brt()
    rows = db.execute("""
        SELECT d.*, t.nome AS tecnico_nome
        FROM sais_destaques d
        LEFT JOIN prod_tecnicos t ON t.id=d.tecnico_id
        WHERE d.data=?
        ORDER BY d.pontos_bonus DESC
        LIMIT 10
    """, (data,)).fetchall()

    # Premiação acumulada do mês
    mes = data[:7]
    premio_mes = db.execute("""
        SELECT t.nome, SUM(d.pontos_bonus) pts_bonus, COUNT(*) destaques
        FROM sais_destaques d
        JOIN prod_tecnicos t ON t.id=d.tecnico_id
        WHERE strftime('%Y-%m', d.data)=?
        GROUP BY d.tecnico_id ORDER BY pts_bonus DESC LIMIT 5
    """, (mes,)).fetchall()
    db.close()

    if not rows:
        linhas_hoje = "  Nenhum destaque hoje"
    else:
        linhas_hoje = "\n".join(
            f"  ⭐ {r['tecnico_nome'].split()[0]}: {r['descricao'] if 'descricao' in r.keys() else ''} +{r['pontos_bonus']}pts"
            for r in rows
        )

    linhas_mes = "\n".join(
        f"  {'🥇' if i==0 else '🥈' if i==1 else '🥉' if i==2 else '▫️'} "
        f"{r['nome'].split()[0]}: {r['pts_bonus']}pts bonus ({r['destaques']} destaques)"
        for i, r in enumerate(premio_mes)
    ) or "  Sem dados do mês"

    return (
        f"⭐ <b>Destaques e Premiação — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Destaques de hoje:</b>\n{linhas_hoje}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Ranking bônus do mês:</b>\n{linhas_mes}"
    )

# ══════════════════════════════════════════════════════
# 9. REINCIDÊNCIAS
# ══════════════════════════════════════════════════════
def msg_reincidencias():
    db = get_db()
    data = hoje_brt()
    # OS de suporte que são reincidência (cliente já teve OS nos últimos 30 dias)
    rows = db.execute("""
        SELECT t.nome AS tecnico,
               COUNT(*) total_reincidencias
        FROM prod_os_cache o
        JOIN prod_tecnicos t ON t.id=o.tecnico_id
        WHERE o.categoria='suporte'
          AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours')=?
          AND o.status='finalizada'
        GROUP BY o.tecnico_id ORDER BY total_reincidencias DESC
    """, (data,)).fetchall()

    # Taxa de reincidência do mês (30 dias)
    mes = db.execute("""
        SELECT
            COUNT(CASE WHEN categoria='suporte' THEN 1 END) suportes,
            COUNT(*) total
        FROM prod_os_cache
        WHERE status='finalizada'
          AND DATE(data_abertura) >= date(datetime('now','-3 hours'),'-30 days')
    """).fetchone()
    db.close()

    total_sup = mes["suportes"] or 0
    total_os  = mes["total"] or 0
    taxa_sup  = round(total_sup/total_os*100, 1) if total_os else 0
    cor = "🟢" if taxa_sup <= 20 else "🟡" if taxa_sup <= 30 else "🔴"

    linhas = "\n".join(
        f"  🔄 {r['tecnico'].split()[0]}: {r['total_reincidencias']} OS suporte"
        for r in rows
    ) or "  Nenhuma OS de suporte hoje"

    return (
        f"🔄 <b>Suporte e Reincidências — Cliquedf</b>\n"
        f"📅 {fmt_data(data)}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📊 OS suporte 30 dias: <b>{total_sup}</b> ({cor} {taxa_sup}% do total)\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"<b>Suportes hoje por técnico:</b>\n{linhas}"
    )

# ══════════════════════════════════════════════════════
# 10. PERFORMANCE MENSAL
# ══════════════════════════════════════════════════════
def msg_performance():
    db = get_db()
    mes = hoje_brt()[:7]
    rows = db.execute("""
        SELECT t.nome,
               COUNT(o.id) total_os,
               SUM(o.status='finalizada') fins,
               COALESCE(SUM(p.pontos_final),0) pts,
               ROUND(AVG(CASE WHEN o.sla_tecnico_min<=480 THEN o.sla_tecnico_min END),0) sla_med,
               COUNT(CASE WHEN o.categoria='servico' THEN 1 END) servicos,
               COUNT(CASE WHEN o.categoria='suporte' THEN 1 END) suportes
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id=t.id
            AND strftime('%Y-%m', COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura))=?
            AND o.status='finalizada'
        LEFT JOIN sais_os_pontuacao p ON p.os_id=o.ixc_os_id
        WHERE t.ativo=1
        GROUP BY t.id ORDER BY pts DESC
    """, (mes,)).fetchall()
    db.close()

    meta_mes = 80 * 22  # 80pts/dia × 22 dias úteis
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟","▫️"]
    linhas = []
    for i, t in enumerate(rows):
        pts  = t["pts"] or 0
        pct  = round(pts/meta_mes*100) if meta_mes else 0
        sla  = f"{int(t['sla_med'])}min" if t["sla_med"] else "—"
        linhas.append(
            f"  {medals[i]} {t['nome'].split()[0]}: "
            f"<b>{pts}pts</b> ({ic(pct)}{pct}%) · "
            f"{t['fins'] or 0} OS · ⏱{sla}"
        )

    return (
        f"📈 <b>Performance Mensal — Cliquedf</b>\n"
        f"📅 {mes}\n"
        f"━━━━━━━━━━━━━━━━━\n"
        + "\n".join(linhas) +
        f"\n━━━━━━━━━━━━━━━━━\n"
        f"<i>Pontos · %Meta · OS · Tempo médio</i>"
    )


# ══════════════════════════════════════════════════════
# 11. RETORNOS PÓS-INSTALAÇÃO (detalhe)
# ══════════════════════════════════════════════════════
def msg_retornos():
    try:
        import pymysql
        ixc = pymysql.connect(
            host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT",3306)),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"), cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=8
        )
        with ixc.cursor() as cur:
            cur.execute("""
                SELECT
                    c.razao AS cliente,
                    f_inst.funcionario AS tecnico_inst,
                    a_inst.assunto AS servico_inst,
                    DATE(inst.data_fechamento) AS data_inst,
                    a_sup.assunto AS servico_sup,
                    DATE(sup.data_abertura) AS data_retorno,
                    DATEDIFF(sup.data_abertura, inst.data_fechamento) AS dias_apos,
                    sup.status AS status_ret
                FROM su_oss_chamado inst
                JOIN su_oss_chamado sup ON sup.id_cliente = inst.id_cliente
                    AND sup.id_assunto IN (20,21,44,47,94,103,105,107)
                    AND sup.data_abertura > inst.data_fechamento
                    AND sup.data_abertura <= DATE_ADD(inst.data_fechamento, INTERVAL 30 DAY)
                    AND sup.id != inst.id
                JOIN cliente c ON c.id = inst.id_cliente
                JOIN funcionarios f_inst ON f_inst.id = inst.id_tecnico
                JOIN su_oss_assunto a_inst ON a_inst.id = inst.id_assunto
                JOIN su_oss_assunto a_sup ON a_sup.id = sup.id_assunto
                WHERE inst.id_assunto IN (2,14,15,49,227,239,30,48)
                  AND inst.status = 'F'
                  AND inst.data_fechamento >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                  AND inst.id_tecnico IN (13,17,11,38,47,50,35,56,55,46,19)
                GROUP BY inst.id, sup.id
                ORDER BY dias_apos ASC, inst.data_fechamento DESC
                LIMIT 15
            """)
            rows = cur.fetchall()
        ixc.close()
    except Exception as e:
        return f"🔁 <b>Retornos Pós-Instalação</b>\n❌ Erro: {e}"

    if not rows:
        return (
            f"🔁 <b>Retornos Pós-Instalação — 30 dias</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"✅ Nenhum retorno registrado no período."
        )

    linhas = []
    for r in rows:
        dias = r["dias_apos"]
        cor = "🔴" if dias <= 7 else "🟡" if dias <= 15 else "🟠"
        cliente = (r["cliente"] or "").split()[0:2]
        cliente = " ".join(cliente)[:18]
        tec = (r["tecnico_inst"] or "").split()[0]
        sup = (r["servico_sup"] or "")[:22]
        data_i = str(r["data_inst"])[5:].replace("-","/")
        data_r = str(r["data_retorno"])[5:].replace("-","/")
        status = "✅" if r["status_ret"]=="F" else "🔄"
        linhas.append(
            f"  {cor} <b>{tec}</b> → {cliente}\n"
            f"     {sup} · {dias}d após ({data_i}→{data_r}) {status}"
        )

    return (
        f"🔁 <b>Retornos Pós-Instalação — 30 dias</b>\n"
        f"Total: <b>{len(rows)}</b> retornos\n"
        f"━━━━━━━━━━━━━━━━━\n"
        + "\n".join(linhas)
    )

# ══════════════════════════════════════════════════════
# DISPATCHER
# ══════════════════════════════════════════════════════
TIPOS = {
    "producao":     msg_producao,
    "ranking":      msg_ranking,
    "eficiencia":   msg_eficiencia,
    "qualidade":    msg_qualidade,
    "alertas":      msg_alertas,
    "auditoria":    msg_auditoria,
    "sla":          msg_sla,
    "destaques":    msg_destaques,
    "reincidencias": msg_reincidencias,
    "performance":  msg_performance,
    "retornos":     msg_retornos,
}

def main():
    tipo = "producao"
    for arg in sys.argv[1:]:
        if arg.startswith("--tipo="):
            tipo = arg.split("=", 1)[1]

    if tipo not in TIPOS:
        log.error(f"Tipo desconhecido: {tipo}. Opções: {list(TIPOS.keys())}")
        sys.exit(1)

    log.info(f"Enviando: {tipo}")
    try:
        msg = TIPOS[tipo]()
        send(msg)
        log.info(f"✅ {tipo} enviado")
    except Exception as e:
        log.error(f"Erro em {tipo}: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
