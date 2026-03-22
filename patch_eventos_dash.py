#!/usr/bin/env python3
"""
Patch SAIS — Página Eventos Tempo Real
Transforma a página de eventos em dashboard operacional com cards:
- OS sem agendamento (abertas sem data_agenda)
- Técnicos ociosos
- SLA estourado
- OS críticas paradas
- OS em execução
- Meta do dia
- Eventos WebSocket recentes
"""

INDEX = "/opt/automacoes/cliquedf/operacional/static/index.html"

import shutil, time
ts = int(time.time())
shutil.copy2(INDEX, f"{INDEX}.bak.{ts}")
print(f"💾 Backup criado (.bak.{ts})")

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

# ── CSS ──────────────────────────────────────────────────────────────────────
OLD_CSS = ".ag-alert{padding:12px 16px;border-radius:8px;font-size:12px;font-weight:700;margin-top:12px}"
NEW_CSS = """.ag-alert{padding:12px 16px;border-radius:8px;font-size:12px;font-weight:700;margin-top:12px}
.ev-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px}
.ev-card{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:16px;cursor:pointer;transition:all .2s;position:relative;overflow:hidden}
.ev-card:hover{transform:translateY(-2px);border-color:var(--cyan)}
.ev-card.critico{border-left:4px solid var(--red)}
.ev-card.alerta{border-left:4px solid var(--amber)}
.ev-card.ok{border-left:4px solid var(--green)}
.ev-card.execucao{border-left:4px solid var(--cyan)}
.ev-card-titulo{font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px}
.ev-card-valor{font-size:32px;font-weight:800;font-family:'DM Mono',monospace;margin-bottom:4px}
.ev-card-desc{font-size:11px;color:var(--muted)}
.ev-card-pulse{position:absolute;top:12px;right:12px;width:8px;height:8px;border-radius:50%;animation:pulse 2s infinite}
.ev-card-pulse.red{background:var(--red)}
.ev-card-pulse.amber{background:var(--amber)}
.ev-card-pulse.green{background:var(--green)}
.ev-card-pulse.cyan{background:var(--cyan)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(1.3)}}
.ev-modal-item{padding:10px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px;display:flex;justify-content:space-between;align-items:center}
.ev-modal-item:last-child{border:none}
.ev-recomendacao{margin-top:14px;padding:12px 14px;border:1px solid var(--amber);border-radius:8px;background:rgba(255,184,63,.08);font-size:12px;color:var(--amber)}
.ev-ws-item{display:flex;gap:10px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.03);font-size:11px}
.ev-ws-item:last-child{border:none}"""

if OLD_CSS in html:
    html = html.replace(OLD_CSS, NEW_CSS, 1)
    print("✅ CSS adicionado")
else:
    print("❌ CSS anchor não encontrado")

# ── HTML da página eventos ────────────────────────────────────────────────────
OLD_PAGE = """    <div class="page" id="page-eventos">
      <div class="pg-hd"><div><div class="pg-title">Eventos <span>Tempo Real</span></div><div class="pg-sub">Atualizando via WebSocket</div></div></div>
      <div id="eventos-list"><div class="loading"><div class="spinner"></div>Aguardando eventos...</div></div>
    </div>"""

NEW_PAGE = """    <div class="page" id="page-eventos">
      <div class="pg-hd">
        <div><div class="pg-title">Central <span>Operacional</span></div><div class="pg-sub" id="ev-sub">Carregando...</div></div>
        <button class="period-tab active" onclick="loadEventosDash()">↻ Atualizar</button>
      </div>

      <!-- Cards operacionais -->
      <div class="ev-grid" id="ev-cards">
        <div class="ev-card alerta"><div class="ev-card-titulo">Carregando</div><div class="ev-card-valor">...</div></div>
      </div>

      <!-- Eventos WebSocket recentes -->
      <div class="card" style="margin-top:4px">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px;display:flex;justify-content:space-between">
          <span>📡 Eventos WebSocket Recentes</span>
          <span id="ev-ws-count" style="color:var(--cyan)"></span>
        </div>
        <div id="eventos-list"><div class="empty"><div class="empty-icon">📡</div>Aguardando eventos do WebSocket...</div></div>
      </div>
    </div>"""

if OLD_PAGE in html:
    html = html.replace(OLD_PAGE, NEW_PAGE, 1)
    print("✅ HTML da página atualizado")
else:
    print("❌ HTML da página não encontrado")

# ── JS: substituir loadEventos e renderEventos ────────────────────────────────
OLD_JS = """function loadEventos(){renderEventos()}
function renderEventos(){
  const el=document.getElementById('eventos-list')
  if(!eventosBuffer.length){el.innerHTML=`<div class="empty"><div class="empty-icon">📡</div>Aguardando eventos do WebSocket...</div>`;return}
  el.innerHTML=eventosBuffer.map(e=>`
    <div class="os-row" style="margin-bottom:6px">
      <div style="font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);margin-right:10px;flex-shrink:0">${e.recebido}</div>
      <div style="flex:1"><div style="font-size:11px;font-weight:600">${e.tipo}</div><div style="font-size:10px;color:var(--muted)">${JSON.stringify(e.dados||{}).slice(0,80)}</div></div>
    </div>
  `).join('')
}"""

NEW_JS = """function loadEventos(){ loadEventosDash() }

// ─── Dashboard Operacional ───────────────────────────────────────────────────
let _evDashData = {}

async function loadEventosDash(){
  document.getElementById('ev-sub').textContent = 'Atualizando...'
  try{
    const [resumo, ociosos, criticas, pont] = await Promise.all([
      apiFetch(`${SAIS}/visao-geral/resumo`),
      apiFetch(`${SAIS}/central/tecnicos-ociosos`),
      apiFetch(`${SAIS}/central/os-criticas`),
      apiFetch(`${SAIS}/pontuacao/resumo-dia`),
    ])

    const r       = resumo.resumo || {}
    const ocList  = ociosos.tecnicos_ociosos || []
    const crList  = criticas.os_criticas || []
    const slaList = crList.filter(o => (o.horas_abertas||0) > 4)
    const execList= crList.filter(o => o.status === 'execucao')

    _evDashData = { resumo: r, ocList, crList, slaList, execList, pont }

    const agora = new Date().toLocaleTimeString('pt-BR')
    document.getElementById('ev-sub').textContent =
      `Atualizado às ${agora} · ${r.total||0} OS hoje`

    const metaPct = resumo.meta_percentual || 0
    const ptsPct  = pont.aproveitamento || 0

    document.getElementById('ev-cards').innerHTML = `
      <div class="ev-card ${crList.length>5?'critico':crList.length>0?'alerta':'ok'}"
           onclick="evModal('criticas')">
        <div class="ev-card-pulse ${crList.length>5?'red':crList.length>0?'amber':'green'}"></div>
        <div class="ev-card-titulo">OS Críticas Paradas</div>
        <div class="ev-card-valor" style="color:${crList.length>5?'var(--red)':crList.length>0?'var(--amber)':'var(--green)'}">${crList.length}</div>
        <div class="ev-card-desc">Atrasadas acima de 4h</div>
      </div>

      <div class="ev-card ${slaList.length>0?'critico':'ok'}"
           onclick="evModal('sla')">
        <div class="ev-card-pulse ${slaList.length>0?'red':'green'}"></div>
        <div class="ev-card-titulo">SLA Estourado</div>
        <div class="ev-card-valor" style="color:${slaList.length>0?'var(--red)':'var(--green)'}">${slaList.length}</div>
        <div class="ev-card-desc">Urgência máxima</div>
      </div>

      <div class="ev-card ${ocList.length>2?'critico':ocList.length>0?'alerta':'ok'}"
           onclick="evModal('ociosos')">
        <div class="ev-card-pulse ${ocList.length>2?'red':ocList.length>0?'amber':'green'}"></div>
        <div class="ev-card-titulo">Técnicos Ociosos</div>
        <div class="ev-card-valor" style="color:${ocList.length>0?'var(--amber)':'var(--green)'}">${ocList.length}</div>
        <div class="ev-card-desc">Sem atividade há +1.5h</div>
      </div>

      <div class="ev-card execucao" onclick="evModal('execucao')">
        <div class="ev-card-pulse cyan"></div>
        <div class="ev-card-titulo">OS em Execução</div>
        <div class="ev-card-valor" style="color:var(--cyan)">${r.em_campo||0}</div>
        <div class="ev-card-desc">Em atendimento agora</div>
      </div>

      <div class="ev-card ${metaPct>=80?'ok':metaPct>=50?'alerta':'critico'}"
           onclick="evModal('meta')">
        <div class="ev-card-pulse ${metaPct>=80?'green':metaPct>=50?'amber':'red'}"></div>
        <div class="ev-card-titulo">Meta do Dia</div>
        <div class="ev-card-valor" style="color:${metaPct>=80?'var(--green)':metaPct>=50?'var(--amber)':'var(--red)'}">${metaPct}%</div>
        <div class="ev-card-desc">${r.finalizadas||0} de ${resumo.meta_os_dia||150} OS</div>
      </div>

      <div class="ev-card ${ptsPct>=80?'ok':ptsPct>=50?'alerta':'critico'}"
           onclick="evModal('pontuacao')">
        <div class="ev-card-pulse ${ptsPct>=80?'green':ptsPct>=50?'amber':'red'}"></div>
        <div class="ev-card-titulo">Qualidade (Pontos)</div>
        <div class="ev-card-valor" style="color:${ptsPct>=80?'var(--green)':ptsPct>=50?'var(--amber)':'var(--red)'}">${ptsPct}%</div>
        <div class="ev-card-desc">${pont.total_pontos||0} pts · ${pont.os_perfeitas||0} perfeitas</div>
      </div>
    `
    renderEventos()
  }catch(e){
    document.getElementById('ev-sub').textContent = 'Erro ao carregar dados'
    console.error('loadEventosDash:', e)
  }
}

function evModal(tipo){
  const d = _evDashData
  let titulo = '', itens = [], rec = ''

  if(tipo==='criticas'){
    titulo = 'OS Críticas Paradas'
    itens  = (d.crList||[]).map(o=>`
      <div class="ev-modal-item">
        <span><strong>#${o.ixc_os_id}</strong> — ${o.assunto||'—'} · ${o.tecnico_nome||'—'}</span>
        <span class="badge red">${o.horas_abertas}h</span>
      </div>`)
    rec = '🚨 Priorizar atendimento imediato nas OS acima de 4h'
  }
  else if(tipo==='sla'){
    titulo = 'SLA Estourado'
    itens  = (d.slaList||[]).map(o=>`
      <div class="ev-modal-item">
        <span><strong>#${o.ixc_os_id}</strong> — ${o.assunto||'—'} · ${o.tecnico_nome||'—'}</span>
        <span class="badge red">${o.horas_abertas}h atraso</span>
      </div>`)
    rec = '🚨 Acionar técnico imediatamente e notificar supervisão'
  }
  else if(tipo==='ociosos'){
    titulo = 'Técnicos Ociosos'
    itens  = (d.ocList||[]).map(t=>`
      <div class="ev-modal-item">
        <span><strong>${t.nome||'—'}</strong></span>
        <span class="badge amber">${t.horas_ocioso}h parado</span>
      </div>`)
    rec = '🔧 Alocar OS pendentes imediatamente para estes técnicos'
  }
  else if(tipo==='execucao'){
    titulo = 'OS em Execução'
    const r = d.resumo || {}
    itens  = [`<div class="ev-modal-item"><span>OS em campo agora</span><span class="badge cyan">${r.em_campo||0}</span></div>`]
    rec = '⏱ Monitorar tempo de execução e evitar estouro de SLA'
  }
  else if(tipo==='meta'){
    titulo = 'Meta do Dia'
    const r = d.resumo || {}
    itens  = [
      `<div class="ev-modal-item"><span>Finalizadas hoje</span><span class="badge green">${r.finalizadas||0}</span></div>`,
      `<div class="ev-modal-item"><span>Em campo</span><span class="badge cyan">${r.em_campo||0}</span></div>`,
      `<div class="ev-modal-item"><span>Agendadas</span><span class="badge amber">${r.agendadas||0}</span></div>`,
    ]
    rec = '📊 Meta diária: 150 OS finalizadas pela equipe'
  }
  else if(tipo==='pontuacao'){
    titulo = 'Qualidade de Pontuação'
    const p = d.pont || {}
    itens  = [
      `<div class="ev-modal-item"><span>Total de pontos</span><span class="badge cyan">${p.total_pontos||0}</span></div>`,
      `<div class="ev-modal-item"><span>OS perfeitas</span><span class="badge green">${p.os_perfeitas||0}</span></div>`,
      `<div class="ev-modal-item"><span>Aproveitamento</span><span class="badge ${(p.aproveitamento||0)>=80?'green':(p.aproveitamento||0)>=50?'':'red'}">${p.aproveitamento||0}%</span></div>`,
      p.pendencias?.sem_app    ? `<div class="ev-modal-item"><span>Sem app</span><span class="badge red">${p.pendencias.sem_app}</span></div>` : '',
      p.pendencias?.sem_produto? `<div class="ev-modal-item"><span>Sem produto</span><span class="badge red">${p.pendencias.sem_produto}</span></div>` : '',
      p.pendencias?.sem_foto   ? `<div class="ev-modal-item"><span>Sem foto</span><span class="badge red">${p.pendencias.sem_foto}</span></div>` : '',
    ]
    rec = '📋 Treinar técnicos nas pendências mais comuns'
  }

  // Abre modal lista-os reaproveitado
  document.getElementById('modal-lista-os-title').textContent = titulo
  document.getElementById('modal-lista-os-sub').textContent = `${itens.filter(Boolean).length} itens`
  document.getElementById('modal-lista-os-body').innerHTML =
    itens.filter(Boolean).join('') +
    (rec ? `<div class="ev-recomendacao">🔧 ${rec}</div>` : '')
  abrirModal('modal-lista-os')
}

function renderEventos(){
  const el = document.getElementById('eventos-list')
  const ct = document.getElementById('ev-ws-count')
  if(!eventosBuffer.length){
    el.innerHTML=`<div style="color:var(--muted);font-size:11px;padding:8px 0">Nenhum evento recebido ainda</div>`
    if(ct) ct.textContent=''
    return
  }
  if(ct) ct.textContent = eventosBuffer.length + ' eventos'
  const tipoIcon = {
    os_atrasada:'🚨', tecnico_ocioso:'😴', meta_atingida:'🏆',
    meta_em_risco:'⚠️', os_finalizada:'✅', os_aberta:'📋'
  }
  el.innerHTML=eventosBuffer.slice(0,20).map(e=>`
    <div class="ev-ws-item">
      <span style="color:var(--muted);font-family:'DM Mono',monospace;min-width:55px">${e.recebido}</span>
      <span style="min-width:20px">${tipoIcon[e.tipo]||'📡'}</span>
      <span style="font-weight:600;min-width:140px;color:var(--text)">${e.tipo}</span>
      <span style="color:var(--muted)">${JSON.stringify(e.dados||{}).slice(0,60)}</span>
    </div>
  `).join('')
}"""

if OLD_JS in html:
    html = html.replace(OLD_JS, NEW_JS, 1)
    print("✅ JS atualizado")
else:
    print("❌ JS anchor não encontrado")

with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("✅ index.html salvo!")
print("✅ Rode: systemctl restart hubprod_cliquedf")
