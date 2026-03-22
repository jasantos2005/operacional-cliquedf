#!/usr/bin/env python3
"""
Patch SAIS — Modal OS Finalizadas
1. Adiciona endpoint GET /api/sais/visao-geral/os-finalizadas
2. Adiciona modal de lista de OS finalizadas (modal-lista-os)
3. Adiciona modal de detalhe da OS finalizada (modal-os-det)
4. Implementa abrirModalListaOS() e abrirModalOSDet()
"""

INDEX   = "/opt/automacoes/cliquedf/operacional/static/index.html"
VG_PATH = "/opt/automacoes/cliquedf/operacional/app/routes/sais/visao_geral.py"

import shutil, time

# ─── Backups ─────────────────────────────────────────────────────────────────
ts = int(time.time())
shutil.copy2(INDEX,   f"{INDEX}.bak.{ts}")
shutil.copy2(VG_PATH, f"{VG_PATH}.bak.{ts}")
print(f"💾 Backups criados (.bak.{ts})")

# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 1 — Novo endpoint no visao_geral.py
# ═══════════════════════════════════════════════════════════════════════════════

with open(VG_PATH, "r", encoding="utf-8") as f:
    vg = f.read()

NEW_ENDPOINT = '''

@router.get("/os-finalizadas")
async def get_os_finalizadas(data: str = Query(None)):
    """Lista de OS finalizadas do dia com dados de pontuação para o modal."""
    db = get_db()
    data = str(data) if data else hoje_brt()

    rows = db.execute("""
        SELECT
            o.ixc_os_id,
            o.status,
            o.categoria,
            o.data_abertura,
            o.data_fechamento,
            t.nome        AS tecnico_nome,
            t.ixc_funcionario_id AS tecnico_id,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS nome_assunto,
            -- pontuação (pode ser NULL se ainda não calculada)
            p.pontos_final,
            p.pontos_base,
            p.pen_foto,
            p.pen_app,
            p.pen_produto,
            p.pen_descricao,
            p.bonus_tempo,
            p.bonus_fibra,
            p.total_fotos,
            p.tem_produto,
            p.tem_comodato,
            p.tem_app,
            p.metros_fibra,
            p.minutos_exec,
            p.len_descricao,
            p.pendencias,
            p.aprovada,
            -- sla
            s.horas_sla
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t      ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a      ON a.id = o.ixc_assunto_id
        LEFT JOIN sais_os_pontuacao p  ON p.os_id = o.ixc_os_id
        LEFT JOIN sais_sla_config s    ON s.assunto_id = o.ixc_assunto_id
        WHERE o.status = 'finalizada'
          AND DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
        ORDER BY COALESCE(o.data_fechamento, o.data_abertura) DESC
    """, (data,)).fetchall()
    db.close()

    result = []
    for r in rows:
        d = dict(r)
        # Calcular tempo de execução em minutos
        tempo_min = None
        if d.get("data_abertura") and d.get("data_fechamento"):
            try:
                from datetime import datetime as dt
                fmt = "%Y-%m-%d %H:%M:%S"
                ab  = dt.strptime(d["data_abertura"][:19],  fmt)
                fe  = dt.strptime(d["data_fechamento"][:19], fmt)
                tempo_min = round((fe - ab).total_seconds() / 60)
            except:
                pass

        # SLA
        horas_sla = d.get("horas_sla") or 4.0
        sla_min   = horas_sla * 60
        pct_sla   = round(tempo_min / sla_min * 100) if tempo_min else None
        status_sla = None
        if pct_sla is not None:
            status_sla = "no_prazo" if pct_sla <= 80 else "em_risco" if pct_sla <= 100 else "estourado"

        pends = [p for p in (d.get("pendencias") or "").split(" | ") if p]

        result.append({
            "os_id":        d["ixc_os_id"],
            "tecnico_nome": d["tecnico_nome"] or "—",
            "tecnico_id":   d["tecnico_id"],
            "nome_assunto": d["nome_assunto"] or "—",
            "categoria":    d["categoria"] or "—",
            "data_fechamento": d["data_fechamento"],
            "tempo_min":    tempo_min,
            "sla": {
                "horas_previstas": horas_sla,
                "pct":            pct_sla,
                "status":         status_sla,
            },
            "pontuacao": {
                "calculado":     d["pontos_final"] is not None,
                "pontos_base":   d["pontos_base"]   or 0,
                "pontos_final":  d["pontos_final"]  or 0,
                "aproveitamento": round(d["pontos_final"] / d["pontos_base"] * 100)
                                  if d["pontos_base"] and d["pontos_final"] is not None else 0,
                "pen_foto":      d["pen_foto"]      or 0,
                "pen_app":       d["pen_app"]       or 0,
                "pen_produto":   d["pen_produto"]   or 0,
                "pen_descricao": d["pen_descricao"] or 0,
                "bonus_tempo":   d["bonus_tempo"]   or 0,
                "bonus_fibra":   d["bonus_fibra"]   or 0,
            },
            "evidencias": {
                "total_fotos":   d["total_fotos"]  or 0,
                "tem_produto":   bool(d["tem_produto"]),
                "tem_comodato":  bool(d["tem_comodato"]),
                "tem_app":       bool(d["tem_app"]),
                "metros_fibra":  d["metros_fibra"]  or 0,
                "minutos_exec":  d["minutos_exec"]  or 0,
                "len_descricao": d["len_descricao"] or 0,
                "aprovada":      bool(d["aprovada"]),
            },
            "pendencias": pends,
        })

    return {"data": data, "total": len(result), "os": result}
'''

# Adiciona antes do último @router.get que encontrar no final do arquivo
ANCHOR = '@router.get("/alertas")'
if ANCHOR in vg:
    vg = vg.replace(ANCHOR, NEW_ENDPOINT + "\n\n" + ANCHOR, 1)
    with open(VG_PATH, "w", encoding="utf-8") as f:
        f.write(vg)
    print("✅ PARTE 1: endpoint /os-finalizadas adicionado")
else:
    print("❌ PARTE 1: anchor não encontrado no visao_geral.py")


# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 2 — index.html: CSS + Modais HTML + JS
# ═══════════════════════════════════════════════════════════════════════════════

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

# ── CSS ──────────────────────────────────────────────────────────────────────
OLD_CSS_ANCHOR = ".os-status-chip.finalizada{background:rgba(0,229,160,.12);color:var(--green)}"
NEW_CSS = """.os-status-chip.finalizada{background:rgba(0,229,160,.12);color:var(--green)}
.det-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px}
.det-kpi{background:var(--s2);border-radius:8px;padding:12px}
.det-kpi-lbl{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px}
.det-kpi-val{font-size:20px;font-weight:800;font-family:'DM Mono',monospace}
.checklist-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px}
.checklist-row:last-child{border:none}
.chk-ok{color:var(--green)}
.chk-fail{color:var(--red)}
.chk-warn{color:var(--amber)}
.lista-os-row{display:flex;align-items:center;gap:12px;padding:10px 0;border-bottom:1px solid rgba(255,255,255,.04);cursor:pointer;transition:background .15s;border-radius:6px}
.lista-os-row:hover{background:rgba(255,255,255,.03)}
.lista-os-row:last-child{border:none}
.lista-os-id{font-family:'DM Mono',monospace;font-size:12px;color:var(--muted);min-width:70px}
.lista-os-info{flex:1}
.lista-os-tec{font-size:11px;color:var(--muted)}
.lista-os-pts{font-family:'DM Mono',monospace;font-size:14px;font-weight:700;min-width:50px;text-align:right}"""

if OLD_CSS_ANCHOR in html:
    html = html.replace(OLD_CSS_ANCHOR, NEW_CSS, 1)
    print("✅ PARTE 2a: CSS adicionado")
else:
    print("❌ PARTE 2a: anchor CSS não encontrado")

# ── Modais HTML ──────────────────────────────────────────────────────────────
OLD_MODAL_ANCHOR = '<div class="modal-overlay" id="modal-os" onclick="overlayClick(event,\'modal-os\')">'
NEW_MODALS = """<!-- ══ MODAL LISTA OS FINALIZADAS ══════════════════ -->
<div class="modal-overlay" id="modal-lista-os" onclick="overlayClick(event,'modal-lista-os')">
  <div class="modal" style="max-width:680px">
    <div class="modal-hd">
      <div>
        <div class="modal-title" id="modal-lista-os-title">OS Finalizadas</div>
        <div class="modal-sub" id="modal-lista-os-sub"></div>
      </div>
      <button class="modal-close" onclick="fecharModal('modal-lista-os')">×</button>
    </div>
    <div style="padding:16px 20px;overflow-y:auto;max-height:70vh" id="modal-lista-os-body">
      <div class="loading"><div class="spinner"></div>Carregando...</div>
    </div>
  </div>
</div>

<!-- ══ MODAL DETALHE OS FINALIZADA ══════════════════ -->
<div class="modal-overlay" id="modal-os-det" onclick="overlayClick(event,'modal-os-det')">
  <div class="modal" style="max-width:720px">
    <div class="modal-hd">
      <div>
        <div class="modal-title" id="modal-os-det-title">OS #—</div>
        <div class="modal-sub" id="modal-os-det-sub"></div>
      </div>
      <button class="modal-close" onclick="fecharModal('modal-os-det')">×</button>
    </div>
    <div class="modal-tabs">
      <div class="modal-tab active" onclick="switchTab('osdet','resumo',this)">Resumo</div>
      <div class="modal-tab" onclick="switchTab('osdet','validacao',this)">Validação</div>
      <div class="modal-tab" onclick="switchTab('osdet','pontuacao',this)">Pontuação</div>
    </div>
    <div id="osdet-tab-resumo"    class="modal-tab-content active"></div>
    <div id="osdet-tab-validacao" class="modal-tab-content"></div>
    <div id="osdet-tab-pontuacao" class="modal-tab-content"></div>
  </div>
</div>

""" + OLD_MODAL_ANCHOR

if OLD_MODAL_ANCHOR in html:
    html = html.replace(OLD_MODAL_ANCHOR, NEW_MODALS, 1)
    print("✅ PARTE 2b: modais HTML adicionados")
else:
    print("❌ PARTE 2b: anchor do modal-os não encontrado")

# ── JS ───────────────────────────────────────────────────────────────────────
OLD_JS_ANCHOR = "// ═══════════════════════════════════════════════════\nasync function abrirModalOS"

NEW_JS = """// ═══════════════════════════════════════════════════
// MODAL LISTA OS FINALIZADAS
// ═══════════════════════════════════════════════════
async function abrirModalListaOS(status='finalizada', data=null){
  document.getElementById('modal-lista-os-title').textContent =
    status==='finalizada' ? 'OS Finalizadas' : 'Lista de OS'
  document.getElementById('modal-lista-os-sub').textContent = 'Carregando...'
  document.getElementById('modal-lista-os-body').innerHTML =
    '<div class="loading"><div class="spinner"></div>Carregando...</div>'
  abrirModal('modal-lista-os')

  try{
    const url = data
      ? `${SAIS}/visao-geral/os-finalizadas?data=${data}`
      : `${SAIS}/visao-geral/os-finalizadas`
    const d = await apiFetch(url)
    const lista = d.os || []

    document.getElementById('modal-lista-os-sub').textContent =
      `${lista.length} OS finalizadas hoje`

    if(!lista.length){
      document.getElementById('modal-lista-os-body').innerHTML =
        '<div class="empty"><div class="empty-icon">📋</div>Nenhuma OS finalizada hoje</div>'
      return
    }

    // Guardar dados para o modal de detalhe
    window._osFinalizadasCache = {}
    lista.forEach(o => { window._osFinalizadasCache[o.os_id] = o })

    document.getElementById('modal-lista-os-body').innerHTML = lista.map(o => {
      const pts     = o.pontuacao?.pontos_final ?? '—'
      const ptsMax  = o.pontuacao?.pontos_base  ?? 0
      const pctMeta = o.pontuacao?.calculado ? o.pontuacao.aproveitamento : null
      const ptsCor  = !o.pontuacao?.calculado ? 'var(--muted)'
                    : pctMeta >= 80 ? 'var(--green)'
                    : pctMeta >= 50 ? 'var(--amber)' : 'var(--red)'
      const slaSt   = o.sla?.status
      const slaCor  = slaSt==='no_prazo' ? 'var(--green)'
                    : slaSt==='em_risco' ? 'var(--amber)' : 'var(--red)'
      const pends   = o.pendencias?.length || 0

      return `<div class="lista-os-row" onclick="abrirModalOSDet(${o.os_id})">
        <div class="lista-os-id">#${o.os_id}</div>
        <div class="lista-os-info">
          <div style="font-size:13px;font-weight:600">${esc(o.nome_assunto)}</div>
          <div class="lista-os-tec">${esc(o.tecnico_nome)} · ${o.tempo_min ? o.tempo_min+'min' : '—'}</div>
        </div>
        ${o.sla?.pct!=null ? `<span class="badge" style="color:${slaCor};background:rgba(0,0,0,.2)">SLA ${o.sla.pct}%</span>` : ''}
        ${pends > 0 ? `<span class="badge red">${pends} pend.</span>` : '<span class="badge green">✓</span>'}
        <div class="lista-os-pts" style="color:${ptsCor}">${o.pontuacao?.calculado ? pts+'pts' : '—'}</div>
      </div>`
    }).join('')
  }catch(e){
    document.getElementById('modal-lista-os-body').innerHTML =
      '<div class="empty"><div class="empty-icon">⚠️</div>Erro ao carregar</div>'
  }
}

// ═══════════════════════════════════════════════════
// MODAL DETALHE OS FINALIZADA
// ═══════════════════════════════════════════════════
function abrirModalOSDet(osId){
  const os = (window._osFinalizadasCache || {})[osId]
  if(!os) return

  document.getElementById('modal-os-det-title').textContent = `OS #${osId}`
  document.getElementById('modal-os-det-sub').textContent =
    `${os.tecnico_nome} · ${os.nome_assunto}`

  switchTab('osdet','resumo', document.querySelector('#modal-os-det .modal-tab'))
  abrirModal('modal-os-det')

  const pont   = os.pontuacao || {}
  const ev     = os.evidencias || {}
  const sla    = os.sla || {}
  const pends  = os.pendencias || []
  const pct    = pont.aproveitamento || 0
  const pctCor = !pont.calculado ? 'var(--muted)'
               : pct>=80 ? 'var(--green)' : pct>=50 ? 'var(--amber)' : 'var(--red)'
  const slaCor = sla.status==='no_prazo' ? 'var(--green)'
               : sla.status==='em_risco'  ? 'var(--amber)' : 'var(--red)'

  // ── Tab Resumo ──────────────────────────────────
  document.getElementById('osdet-tab-resumo').innerHTML = `
    <div class="det-kpis">
      <div class="det-kpi">
        <div class="det-kpi-lbl">Status SLA</div>
        <div class="det-kpi-val" style="color:${slaCor};font-size:14px">
          ${sla.status==='no_prazo'?'✅ No prazo':sla.status==='em_risco'?'⚠️ Em risco':'🚨 Estourado'}
        </div>
      </div>
      <div class="det-kpi">
        <div class="det-kpi-lbl">SLA consumido</div>
        <div class="det-kpi-val" style="color:${slaCor}">${sla.pct!=null?sla.pct+'%':'—'}</div>
      </div>
      <div class="det-kpi">
        <div class="det-kpi-lbl">Pontos</div>
        <div class="det-kpi-val" style="color:${pctCor}">${pont.calculado?pont.pontos_final:'—'}</div>
      </div>
      <div class="det-kpi">
        <div class="det-kpi-lbl">Técnico</div>
        <div class="det-kpi-val" style="font-size:13px">${esc(os.tecnico_nome)}</div>
      </div>
    </div>

    <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Informações</div>
    <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:14px">
      ${infoRow('Assunto', os.nome_assunto)}
      ${infoRow('Categoria', os.categoria)}
      ${infoRow('Tempo execução', os.tempo_min ? os.tempo_min+'min' : '—')}
      ${infoRow('SLA previsto', (sla.horas_previstas||4)+'h')}
      ${infoRow('Fechamento', fmtData(os.data_fechamento))}
    </div>

    ${pends.length ? `
      <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Pendências</div>
      <div style="display:flex;flex-wrap:wrap;gap:6px">
        ${pends.map(p=>`<span class="badge red">${p}</span>`).join('')}
      </div>
    ` : `<div style="color:var(--green);font-size:12px;padding:4px 0">✅ OS sem pendências</div>`}
  `

  // ── Tab Validação (Checklist) ───────────────────
  function chkRow(label, ok, warn=false){
    const cls  = ok ? 'chk-ok' : warn ? 'chk-warn' : 'chk-fail'
    const icon = ok ? '✔ OK'   : warn ? '⚠ Parcial' : '✖ Faltando'
    return `<div class="checklist-row">
      <span>${label}</span>
      <span class="${cls}">${icon}</span>
    </div>`
  }

  const temFoto   = (ev.total_fotos || 0) > 0
  const temProd   = ev.tem_produto
  const temCom    = ev.tem_comodato
  const temApp    = ev.tem_app
  const temDesc   = (ev.len_descricao || 0) >= 10
  const aprovada  = ev.aprovada

  document.getElementById('osdet-tab-validacao').innerHTML = `
    <div class="card" style="padding:0;margin-bottom:12px">
      <div style="padding:12px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em">Auditoria de Campo</div>
      <div style="padding:0 14px">
        ${chkRow('Fotos registradas', temFoto)}
        ${chkRow('Técnico usou app (deslocamento)', temApp)}
        ${chkRow('Descrição preenchida', temDesc)}
      </div>
    </div>
    <div class="card" style="padding:0">
      <div style="padding:12px 14px;border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em">Checklist de Validação</div>
      <div style="padding:0 14px">
        ${chkRow('Fotos obrigatórias', temFoto)}
        ${chkRow('Produtos vinculados', temProd)}
        ${chkRow('Comodato lançado', temCom, !temCom && temProd)}
        ${chkRow('Aprovada pelo supervisor', aprovada)}
      </div>
    </div>
    ${ev.metros_fibra > 0 ? `
      <div style="margin-top:10px;padding:10px 14px;background:var(--s2);border-radius:8px;font-size:12px">
        🔌 <strong>${ev.metros_fibra}m</strong> de fibra passada
      </div>` : ''}
  `

  // ── Tab Pontuação ───────────────────────────────
  if(pont.calculado){
    document.getElementById('osdet-tab-pontuacao').innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px">
        ${statBox('Base', pont.pontos_base, 'var(--muted)')}
        ${statBox('Final', pont.pontos_final, pctCor)}
        ${statBox('Aproveit.', pct+'%', pctCor)}
      </div>
      <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Penalidades & Bônus</div>
      <div style="display:flex;flex-direction:column;gap:4px">
        ${pont.pen_foto    < 0 ? infoRow('📷 Sem foto',             `<span style="color:var(--red)">${pont.pen_foto} pts</span>`) : ''}
        ${pont.pen_app     < 0 ? infoRow('📱 Sem app',              `<span style="color:var(--red)">${pont.pen_app} pts</span>`) : ''}
        ${pont.pen_produto < 0 ? infoRow('📦 Sem produto/comodato', `<span style="color:var(--red)">${pont.pen_produto} pts</span>`) : ''}
        ${pont.pen_descricao<0 ? infoRow('📝 Descrição curta',      `<span style="color:var(--red)">${pont.pen_descricao} pts</span>`) : ''}
        ${pont.bonus_tempo > 0 ? infoRow('⏱️ Bônus tempo',          `<span style="color:var(--green)">+${pont.bonus_tempo} pts</span>`) : ''}
        ${pont.bonus_tempo < 0 ? infoRow('⏱️ Pen. tempo',           `<span style="color:var(--red)">${pont.bonus_tempo} pts</span>`) : ''}
        ${pont.bonus_fibra > 0 ? infoRow('🔌 Bônus fibra',          `<span style="color:var(--green)">+${pont.bonus_fibra} pts</span>`) : ''}
        ${pont.pen_foto>=0&&pont.pen_app>=0&&pont.pen_produto>=0&&pont.pen_descricao>=0&&!pont.bonus_tempo&&!pont.bonus_fibra
          ? '<div style="color:var(--green);font-size:11px;padding:4px 0">✅ Nenhuma penalidade</div>' : ''}
      </div>
    `
  } else {
    document.getElementById('osdet-tab-pontuacao').innerHTML =
      '<div class="empty"><div class="empty-icon">⏳</div>Pontuação não calculada ainda</div>'
  }
}

""" + "// ═══════════════════════════════════════════════════\nasync function abrirModalOS"

if "// ═══════════════════════════════════════════════════\nasync function abrirModalOS" in html:
    html = html.replace(
        "// ═══════════════════════════════════════════════════\nasync function abrirModalOS",
        NEW_JS,
        1
    )
    print("✅ PARTE 2c: JS adicionado")
else:
    print("❌ PARTE 2c: anchor JS não encontrado")

# ─── Salva index.html ─────────────────────────────────────────────────────────
with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("✅ index.html salvo!")
print("\n✅ Tudo pronto! Rode: systemctl restart hubprod_cliquedf")
