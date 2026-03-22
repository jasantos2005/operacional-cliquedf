#!/usr/bin/env python3
"""
Patch index.html — SAIS v2.1
1. loadRanking → /api/sais/pontuacao/ranking (pontuação real por regras)
2. Modal OS → nova aba "Pontuação" com breakdown detalhado
"""

INDEX = "/opt/automacoes/cliquedf/operacional/static/index.html"

# ─── Lê o arquivo ────────────────────────────────────────────────────────────
with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

original = html  # guarda cópia para checar se mudou

# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 1 — loadRanking: trocar endpoint + campos da tabela
# ═══════════════════════════════════════════════════════════════════════════════

OLD_LOAD_RANKING = """async function loadRanking(data,btn=null){
  if(btn){document.querySelectorAll('#page-ranking .period-tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active')}
  const url=data?`${SAIS}/produtividade/ranking?data=${data}`:`${SAIS}/produtividade/ranking`
  const d=await apiFetch(url)
  document.getElementById('ranking-table').innerHTML=(d.ranking||[]).map((t,i)=>`
    <tr onclick="abrirModalTecnico(${t.tecnico_id},'${esc(t.nome)}')">
      <td><span class="rpos ${rposCls(i)}">${i+1}</span></td>
      <td><strong>${t.nome}</strong></td>
      <td class="r">${t.por_categoria?.servico||0}</td>
      <td class="r">${t.por_categoria?.suporte||0}</td>
      <td class="r">${t.por_categoria?.infra||0}</td>
      <td class="r">${t.por_categoria?.retirada||0}</td>
      <td class="r" style="color:var(--cyan);font-weight:700">${t.total_os}</td>
      <td class="r"><span class="badge ${t.eficiencia>=80?'green':t.eficiencia>=50?'amber':'red'}">${t.eficiencia}%</span></td>
      <td class="r"><span class="badge ${t.pct_meta>=80?'green':t.pct_meta>=50?'amber':'red'}">${t.pct_meta}%</span></td>
      <td class="r"><span class="badge cyan">${t.score}</span></td>
    </tr>
  `).join('')
}"""

NEW_LOAD_RANKING = """async function loadRanking(data,btn=null){
  if(btn){document.querySelectorAll('#page-ranking .period-tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active')}
  const url=data?`${SAIS}/pontuacao/ranking?data=${data}`:`${SAIS}/pontuacao/ranking`
  const d=await apiFetch(url)
  document.getElementById('ranking-table').innerHTML=(d.ranking||[]).map((t,i)=>`
    <tr onclick="abrirModalTecnico(${t.tecnico_id},'${esc(t.nome)}')">
      <td><span class="rpos ${rposCls(i)}">${i+1}</span></td>
      <td><strong>${t.nome}</strong></td>
      <td class="r" style="color:var(--cyan);font-weight:700">${t.total_os}</td>
      <td class="r"><span class="badge cyan" style="font-family:'DM Mono',monospace;font-size:13px">${t.total_pontos}</span></td>
      <td class="r"><span class="badge ${t.aproveitamento>=80?'green':t.aproveitamento>=50?'amber':'red'}">${t.aproveitamento}%</span></td>
      <td class="r"><span class="badge ${t.pct_qualidade>=80?'green':t.pct_qualidade>=50?'amber':'red'}">${t.pct_qualidade}%</span></td>
      <td class="r"><span class="badge ${t.pct_meta>=100?'green':t.pct_meta>=60?'amber':'red'}">${t.pct_meta}%</span></td>
      <td class="r" style="color:var(--muted);font-size:10px">${t.os_perfeitas}/${t.total_os}</td>
    </tr>
  `).join('')
}"""

if OLD_LOAD_RANKING in html:
    html = html.replace(OLD_LOAD_RANKING, NEW_LOAD_RANKING)
    print("✅ PATCH 1: loadRanking atualizado")
else:
    print("❌ PATCH 1: trecho não encontrado — verifique o índice")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 2 — Cabeçalho da tabela de ranking (colunas novas)
# ═══════════════════════════════════════════════════════════════════════════════

OLD_THEAD = """Score: serviço×3 + suporte×2 + infra×2 + retirada×1"""

NEW_THEAD = """Pontuação por regras · meta: 80 pts/dia"""

if OLD_THEAD in html:
    html = html.replace(OLD_THEAD, NEW_THEAD, 1)
    print("✅ PATCH 2: subtítulo do ranking atualizado")
else:
    print("⚠️  PATCH 2: subtítulo não encontrado (não crítico)")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 3 — Colunas do <thead> da tabela de ranking
# ═══════════════════════════════════════════════════════════════════════════════

OLD_THEAD_COLS = """<thead><tr>
              <th>#</th><th>Técnico</th>
              <th class="r">Serviço</th><th class="r">Suporte</th><th class="r">Infra</th><th class="r">Retirada</th>
              <th class="r">Total OS</th><th class="r">Eficiência</th><th class="r">Meta</th><th class="r">Score</th>
            </tr></thead>"""

NEW_THEAD_COLS = """<thead><tr>
              <th>#</th><th>Técnico</th>
              <th class="r">OS</th><th class="r">Pontos</th><th class="r">Aproveit.</th>
              <th class="r">Qualidade</th><th class="r">Meta</th><th class="r">Perfeitas</th>
            </tr></thead>"""

if OLD_THEAD_COLS in html:
    html = html.replace(OLD_THEAD_COLS, NEW_THEAD_COLS, 1)
    print("✅ PATCH 3: colunas do thead atualizadas")
else:
    # Tenta alternativa sem espaços exatos
    print("⚠️  PATCH 3: thead não encontrado — pode precisar de ajuste manual")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 4 — Modal OS: adicionar aba "Pontuação" no HTML das tabs
# ═══════════════════════════════════════════════════════════════════════════════

OLD_OS_TABS = """      <div class="modal-tab active" onclick="switchTab('os','resumo',this)">Resumo</div>
      <div class="modal-tab" onclick="switchTab('os','sla',this)">SLA</div>
      <div class="modal-tab" onclick="switchTab('os','auditoria',this)">Auditoria</div>
      <div class="modal-tab" onclick="switchTab('os','historico',this)">Histórico</div>
"""

NEW_OS_TABS = """      <div class="modal-tab active" onclick="switchTab('os','resumo',this)">Resumo</div>
      <div class="modal-tab" onclick="switchTab('os','sla',this)">SLA</div>
      <div class="modal-tab" onclick="switchTab('os','auditoria',this)">Auditoria</div>
      <div class="modal-tab" onclick="switchTab('os','pontuacao',this)">Pontuação</div>
      <div class="modal-tab" onclick="switchTab('os','historico',this)">Histórico</div>
"""

if OLD_OS_TABS in html:
    html = html.replace(OLD_OS_TABS, NEW_OS_TABS, 1)
    print("✅ PATCH 4: aba Pontuação adicionada ao modal OS")
else:
    print("❌ PATCH 4: tabs do modal OS não encontradas")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 5 — Modal OS: adicionar div do conteúdo da aba pontuação
# ═══════════════════════════════════════════════════════════════════════════════

OLD_OS_TAB_DIVS = """    <div id="os-tab-resumo"   class="modal-tab-content active"></div>
    <div id="os-tab-sla"      class="modal-tab-content"></div>
    <div id="os-tab-auditoria" class="modal-tab-content"></div>
    <div id="os-tab-historico" class="modal-tab-content"></div>"""

NEW_OS_TAB_DIVS = """    <div id="os-tab-resumo"    class="modal-tab-content active"></div>
    <div id="os-tab-sla"       class="modal-tab-content"></div>
    <div id="os-tab-auditoria" class="modal-tab-content"></div>
    <div id="os-tab-pontuacao" class="modal-tab-content"></div>
    <div id="os-tab-historico" class="modal-tab-content"></div>"""

if OLD_OS_TAB_DIVS in html:
    html = html.replace(OLD_OS_TAB_DIVS, NEW_OS_TAB_DIVS, 1)
    print("✅ PATCH 5: div os-tab-pontuacao adicionado")
else:
    print("❌ PATCH 5: divs do modal OS não encontrados")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 6 — abrirModalOS: buscar pontuação e renderizar aba
# ═══════════════════════════════════════════════════════════════════════════════

OLD_MODAL_OS_FETCH = """  try{
    const d=await apiFetch(`${SAIS}/visao-geral/os/${osId}`)
    const os=d.os||{}
    const sla=d.sla||{}
    const audits=d.auditorias||[]"""

NEW_MODAL_OS_FETCH = """  try{
    const [d, pont]=await Promise.all([
      apiFetch(`${SAIS}/visao-geral/os/${osId}`),
      apiFetch(`${SAIS}/pontuacao/os/${osId}`)
    ])
    const os=d.os||{}
    const sla=d.sla||{}
    const audits=d.auditorias||[]"""

if OLD_MODAL_OS_FETCH in html:
    html = html.replace(OLD_MODAL_OS_FETCH, NEW_MODAL_OS_FETCH, 1)
    print("✅ PATCH 6: fetch paralelo de pontuação adicionado")
else:
    print("❌ PATCH 6: try{ da abrirModalOS não encontrado")


# ═══════════════════════════════════════════════════════════════════════════════
# PATCH 7 — Renderizar aba de pontuação no modal OS
# ═══════════════════════════════════════════════════════════════════════════════

OLD_HISTORICO_TAB = """    // Tab Histórico
    document.getElementById('os-tab-historico').innerHTML=`
      <div style="font-size:11px;color:var(--muted);padding:8px 0">
        <div>${infoRow('OS ID', os.ixc_os_id)}</div>
        <div>${infoRow('Assunto ID', os.ixc_assunto_id)}</div>
        <div>${infoRow('Técnico ID', os.tecnico_id)}</div>
        <div>${infoRow('Sincronizado', fmtData(os.sincronizado_em))}</div>
      </div>
    `"""

NEW_HISTORICO_TAB = """    // Tab Pontuação
    if(pont.calculado){
      const det=pont.detalhes||{}
      const ev=pont.evidencias||{}
      const pends=pont.pendencias||[]
      const pct=pont.pontos_base>0?Math.round(pont.pontos_final/pont.pontos_base*100):0
      const pctCor=pct>=80?'var(--green)':pct>=50?'var(--amber)':'var(--red)'
      document.getElementById('os-tab-pontuacao').innerHTML=`
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px">
          ${statBox('Pontos Base', pont.pontos_base, 'var(--muted)')}
          ${statBox('Pontos Finais', pont.pontos_final, pctCor)}
          ${statBox('Aproveitamento', pct+'%', pctCor)}
        </div>

        <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Penalidades & Bônus</div>
        <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:14px">
          ${det.pen_foto<0?`${infoRow('📷 Sem foto', '<span style="color:var(--red)">'+det.pen_foto+' pts</span>')}`:''}
          ${det.pen_app<0?`${infoRow('📱 Sem app/deslocamento', '<span style="color:var(--red)">'+det.pen_app+' pts</span>')}`:''}
          ${det.pen_produto<0?`${infoRow('📦 Sem produto/comodato', '<span style="color:var(--red)">'+det.pen_produto+' pts</span>')}`:''}
          ${det.pen_descricao<0?`${infoRow('📝 Descrição curta', '<span style="color:var(--red)">'+det.pen_descricao+' pts</span>')}`:''}
          ${det.bonus_tempo>0?`${infoRow('⏱️ Bônus tempo', '<span style="color:var(--green)">+'+det.bonus_tempo+' pts</span>')}`:''}
          ${det.bonus_tempo<0?`${infoRow('⏱️ Pen. tempo', '<span style="color:var(--red)">'+det.bonus_tempo+' pts</span>')}`:''}
          ${det.bonus_fibra>0?`${infoRow('🔌 Bônus fibra', '<span style="color:var(--green)">+'+det.bonus_fibra+' pts</span>')}`:''}
          ${det.pen_foto===0&&det.pen_app===0&&det.pen_produto===0&&det.pen_descricao===0&&det.bonus_tempo===0&&det.bonus_fibra===0?'<div style="color:var(--green);font-size:11px;padding:4px 0">✅ Nenhuma penalidade</div>':''}
        </div>

        <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Evidências</div>
        <div style="display:flex;flex-direction:column;gap:4px;margin-bottom:14px">
          ${infoRow('Fotos', ev.total_fotos)}
          ${infoRow('Usou App', ev.tem_app?'<span class="badge green">Sim</span>':'<span class="badge red">Não</span>')}
          ${infoRow('Produto registrado', ev.tem_produto?'<span class="badge green">Sim</span>':'<span class="badge red">Não</span>')}
          ${ev.metros_fibra>0?infoRow('Fibra passada', ev.metros_fibra+'m'):''}
          ${ev.minutos_exec>0?infoRow('Tempo execução', Math.round(ev.minutos_exec)+'min'):''}
          ${infoRow('Tam. descrição', ev.len_descricao+' chars')}
          ${infoRow('Aprovada', pont.aprovada?'<span class="badge green">Sim</span>':'<span class="badge amber">Pendente</span>')}
        </div>

        ${pends.length?`
          <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px">Pendências</div>
          <div style="display:flex;flex-wrap:wrap;gap:6px">
            ${pends.map(p=>`<span class="badge red">${p}</span>`).join('')}
          </div>
        `:''}

        <div style="font-size:9px;color:var(--muted);margin-top:12px">Calculado em: ${fmtData(pont.calculado_em)}</div>
      `
    } else {
      document.getElementById('os-tab-pontuacao').innerHTML=`
        <div class="empty">
          <div class="empty-icon">⏳</div>
          ${pont.mensagem||'Pontuação não calculada. Aguarde o próximo ciclo (15 min).'}
        </div>
      `
    }

    // Tab Histórico
    document.getElementById('os-tab-historico').innerHTML=`
      <div style="font-size:11px;color:var(--muted);padding:8px 0">
        <div>${infoRow('OS ID', os.ixc_os_id)}</div>
        <div>${infoRow('Assunto ID', os.ixc_assunto_id)}</div>
        <div>${infoRow('Técnico ID', os.tecnico_id)}</div>
        <div>${infoRow('Sincronizado', fmtData(os.sincronizado_em))}</div>
      </div>
    `"""

if OLD_HISTORICO_TAB in html:
    html = html.replace(OLD_HISTORICO_TAB, NEW_HISTORICO_TAB, 1)
    print("✅ PATCH 7: aba de pontuação renderizada no modal OS")
else:
    print("❌ PATCH 7: // Tab Histórico não encontrado")


# ─── Salva ───────────────────────────────────────────────────────────────────
if html != original:
    # Backup
    import shutil, time
    bk = INDEX + f".bak.{int(time.time())}"
    shutil.copy2(INDEX, bk)
    print(f"💾 Backup: {bk}")

    with open(INDEX, "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ index.html salvo com sucesso!")
else:
    print("⚠️  Nenhuma mudança aplicada — verifique os patches acima")
