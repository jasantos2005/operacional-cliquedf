#!/usr/bin/env python3
"""
Patch SAIS — Modal OS Agendadas
1. Novo endpoint GET /api/sais/agenda/os/{os_id} — busca dados completos no IXC
2. Modal HTML modal-os-ag com 3 abas: Resumo, SLA, Histórico Cliente
3. Função abrirModalOSAg() no JS
4. Troca abrirModalOS → abrirModalOSAg nos itens de agenda
"""

INDEX    = "/opt/automacoes/cliquedf/operacional/static/index.html"
AG_PATH  = "/opt/automacoes/cliquedf/operacional/app/routes/sais/agenda.py"

import shutil, time
ts = int(time.time())
shutil.copy2(INDEX,   f"{INDEX}.bak.{ts}")
shutil.copy2(AG_PATH, f"{AG_PATH}.bak.{ts}")
print(f"💾 Backups criados (.bak.{ts})")

# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 1 — Novo endpoint no agenda.py
# ═══════════════════════════════════════════════════════════════════════════════

with open(AG_PATH, "r", encoding="utf-8") as f:
    ag = f.read()

NEW_ENDPOINT = '''

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
'''

ANCHOR = '@router.get("/reagendamentos")'
if ANCHOR in ag:
    ag = ag.replace(ANCHOR, NEW_ENDPOINT + "\n\n" + ANCHOR, 1)
    with open(AG_PATH, "w", encoding="utf-8") as f:
        f.write(ag)
    print("✅ PARTE 1: endpoint /agenda/os/{os_id} adicionado")
else:
    print("❌ PARTE 1: anchor não encontrado em agenda.py")
    # Adiciona no final
    ag += NEW_ENDPOINT
    with open(AG_PATH, "w", encoding="utf-8") as f:
        f.write(ag)
    print("✅ PARTE 1: endpoint adicionado no final do arquivo")


# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 2 — index.html: CSS + Modal HTML + JS
# ═══════════════════════════════════════════════════════════════════════════════

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

# ── CSS ──────────────────────────────────────────────────────────────────────
OLD_CSS = ".det-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}"
NEW_CSS = """.det-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}
.ag-alert{padding:12px 16px;border-radius:8px;font-size:12px;font-weight:700;margin-top:12px}
.ag-alert.critico{background:rgba(255,77,106,.15);color:var(--red)}
.ag-alert.aviso{background:rgba(255,184,63,.15);color:var(--amber)}
.ag-alert.info{background:rgba(0,212,255,.1);color:var(--cyan)}"""

if OLD_CSS in html:
    html = html.replace(OLD_CSS, NEW_CSS, 1)
    print("✅ PARTE 2a: CSS adicionado")
else:
    print("⚠️  PARTE 2a: CSS anchor não encontrado")

# ── Modal HTML ───────────────────────────────────────────────────────────────
OLD_MODAL_ANCHOR = "<!-- ══ MODAL LISTA OS FINALIZADAS ══════════════════ -->"
NEW_MODAL = """<!-- ══ MODAL OS AGENDADA ════════════════════════════ -->
<div class="modal-overlay" id="modal-os-ag" onclick="overlayClick(event,'modal-os-ag')">
  <div class="modal" style="max-width:720px">
    <div class="modal-hd">
      <div>
        <div class="modal-title" id="modal-os-ag-title">OS #—</div>
        <div class="modal-sub" id="modal-os-ag-sub"></div>
      </div>
      <button class="modal-close" onclick="fecharModal('modal-os-ag')">×</button>
    </div>
    <div class="modal-tabs">
      <div class="modal-tab active" onclick="switchTab('os-ag','resumo',this)">Resumo</div>
      <div class="modal-tab" onclick="switchTab('os-ag','sla',this)">SLA</div>
      <div class="modal-tab" onclick="switchTab('os-ag','historico',this)">Histórico Cliente</div>
    </div>
    <div id="os-ag-tab-resumo"    class="modal-tab-content active"></div>
    <div id="os-ag-tab-sla"       class="modal-tab-content"></div>
    <div id="os-ag-tab-historico" class="modal-tab-content"></div>
  </div>
</div>

""" + OLD_MODAL_ANCHOR

if OLD_MODAL_ANCHOR in html:
    html = html.replace(OLD_MODAL_ANCHOR, NEW_MODAL, 1)
    print("✅ PARTE 2b: modal HTML adicionado")
else:
    print("❌ PARTE 2b: anchor do modal não encontrado")

# ── JS ───────────────────────────────────────────────────────────────────────
OLD_JS_ANCHOR = "// ═══════════════════════════════════════════════════\n// MODAL LISTA OS FINALIZADAS"
NEW_JS = """// ═══════════════════════════════════════════════════
// MODAL OS AGENDADA
// ═══════════════════════════════════════════════════
async function abrirModalOSAg(osId){
  document.getElementById('modal-os-ag-title').textContent = `OS #${osId}`
  document.getElementById('modal-os-ag-sub').textContent = 'Carregando...'
  document.getElementById('os-ag-tab-resumo').innerHTML =
    '<div class="loading"><div class="spinner"></div>Carregando...</div>'
  switchTab('os-ag','resumo', document.querySelector('#modal-os-ag .modal-tab'))
  abrirModal('modal-os-ag')

  try{
    const d = await apiFetch(`${SAIS}/agenda/os/${osId}`)

    document.getElementById('modal-os-ag-title').textContent = `OS #${osId}`
    document.getElementById('modal-os-ag-sub').textContent =
      `Cliente: ${d.cliente_nome} · ${d.nome_assunto} · ${statusLabel(d.status)}`

    const sla    = d.sla || {}
    const hist   = d.historico_cliente || {}
    const alerta = d.alerta

    const slaCor = sla.status==='no_prazo' ? 'var(--green)'
                 : sla.status==='em_risco'  ? 'var(--amber)' : 'var(--red)'

    const abertCor = (d.tempo_abertura_h||0) > (sla.horas_previstas||4)
                   ? 'var(--red)' : 'var(--amber)'

    const execCor = d.exec_em_h != null
                  ? (d.exec_em_h < 0 ? 'var(--red)' : d.exec_em_h < 2 ? 'var(--amber)' : 'var(--cyan)')
                  : 'var(--muted)'

    const alertaHtml = alerta ? `
      <div class="ag-alert ${alerta.nivel}">
        ${alerta.nivel==='critico'?'🚨':alerta.nivel==='aviso'?'⚠️':'💬'} ${alerta.msg}
      </div>` : ''

    // ── Tab Resumo ──────────────────────────────────
    document.getElementById('os-ag-tab-resumo').innerHTML = `
      <div class="det-kpis">
        <div class="det-kpi">
          <div class="det-kpi-lbl">Status</div>
          <div class="det-kpi-val" style="color:var(--cyan);font-size:14px">Agendada</div>
        </div>
        <div class="det-kpi">
          <div class="det-kpi-lbl">Tempo desde abertura</div>
          <div class="det-kpi-val" style="color:${abertCor}">${d.tempo_abertura_h!=null?d.tempo_abertura_h+'h':'—'}</div>
        </div>
        <div class="det-kpi">
          <div class="det-kpi-lbl">Execução em</div>
          <div class="det-kpi-val" style="color:${execCor}">${d.exec_label||'—'}</div>
        </div>
        <div class="det-kpi">
          <div class="det-kpi-lbl">Técnico</div>
          <div class="det-kpi-val" style="font-size:13px">${esc(d.tecnico_nome)}</div>
        </div>
      </div>

      <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Informações</div>
      <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:14px">
        ${infoRow('Assunto', d.nome_assunto)}
        ${infoRow('Cliente', d.cliente_nome)}
        ${infoRow('Abertura', fmtData(d.data_abertura))}
        ${infoRow('Agendado para', fmtData(d.data_agenda))}
      </div>
      ${alertaHtml}
    `

    // ── Tab SLA ─────────────────────────────────────
    const slaPct = Math.min(sla.pct||0, 150)
    const slaLabel = sla.status==='no_prazo' ? '✅ No prazo'
                   : sla.status==='em_risco'  ? '⚠️ Em risco' : '🚨 Estourado'
    document.getElementById('os-ag-tab-sla').innerHTML = `
      <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:16px">
        ${infoRow('Tempo limite', (sla.horas_previstas||4)+'h')}
        ${infoRow('Consumido', `<span style="color:${slaCor};font-weight:700">${sla.pct!=null?sla.pct+'%':'—'}</span>`)}
        ${infoRow('Status', `<span style="color:${slaCor};font-weight:700">${slaLabel}</span>`)}
        ${infoRow('Tempo decorrido', d.tempo_abertura_h!=null?d.tempo_abertura_h+'h':'—')}
      </div>
      <div style="font-size:10px;color:var(--muted);margin-bottom:6px">Consumo do SLA: ${sla.pct||0}%</div>
      <div class="sla-bar"><div class="sla-bar-fill" style="width:${Math.min(sla.pct||0,100)}%;background:${slaCor}"></div></div>
      ${alertaHtml}
    `

    // ── Tab Histórico Cliente ────────────────────────
    const padraoCor = hist.padrao==='Recorrente' ? 'var(--red)'
                    : hist.padrao==='Ocasional'   ? 'var(--amber)' : 'var(--green)'
    document.getElementById('os-ag-tab-historico').innerHTML = `
      <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">
        Histórico do Cliente (mesmo assunto)
      </div>
      <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:16px">
        ${infoRow('Cliente', d.cliente_nome)}
        ${infoRow('Últimos 30 dias', `<span style="color:${hist.ultimos_30>=3?'var(--red)':hist.ultimos_30>=1?'var(--amber)':'var(--green)'}">${hist.ultimos_30} ocorrência${hist.ultimos_30!==1?'s':''}</span>`)}
        ${infoRow('Últimos 60 dias', `<span style="color:${hist.ultimos_60>=5?'var(--red)':hist.ultimos_60>=2?'var(--amber)':'var(--green)'}">${hist.ultimos_60} ocorrência${hist.ultimos_60!==1?'s':''}</span>`)}
        ${infoRow('Padrão detectado', `<span style="color:${padraoCor};font-weight:700">${hist.padrao}</span>`)}
      </div>
      ${alertaHtml}
      ${hist.ultimos_30===0 ? '<div style="color:var(--green);font-size:12px;padding:4px 0">✅ Sem histórico de reincidência</div>' : ''}
    `
  }catch(e){
    document.getElementById('os-ag-tab-resumo').innerHTML =
      '<div class="empty"><div class="empty-icon">⚠️</div>Erro ao carregar OS</div>'
  }
}

""" + "// ═══════════════════════════════════════════════════\n// MODAL LISTA OS FINALIZADAS"

if "// ═══════════════════════════════════════════════════\n// MODAL LISTA OS FINALIZADAS" in html:
    html = html.replace(
        "// ═══════════════════════════════════════════════════\n// MODAL LISTA OS FINALIZADAS",
        NEW_JS, 1
    )
    print("✅ PARTE 2c: JS adicionado")
else:
    print("❌ PARTE 2c: anchor JS não encontrado")

# ── Troca abrirModalOS por abrirModalOSAg nos itens de agenda ────────────────
# Apenas nos onclick dentro das funções de agenda (linhas 1103 e 1009 aprox)
OLD_AG1 = "onclick=\"abrirModalOS(${o.ixc_os_id})\">\n            <span class=\"agenda-hora\">"
NEW_AG1 = "onclick=\"abrirModalOSAg(${o.ixc_os_id})\">\n            <span class=\"agenda-hora\">"

if OLD_AG1 in html:
    html = html.replace(OLD_AG1, NEW_AG1, 1)
    print("✅ PARTE 2d: agenda dia → abrirModalOSAg")
else:
    print("⚠️  PARTE 2d: trecho agenda dia não encontrado — ajuste manual se necessário")

# ─── Salva ───────────────────────────────────────────────────────────────────
with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("✅ index.html salvo!")
print("\n✅ Rode: systemctl restart hubprod_cliquedf")
