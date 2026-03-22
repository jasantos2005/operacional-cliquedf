#!/usr/bin/env python3
"""Patch SAIS — Filtros Globais na Topbar (index.html only)"""

INDEX = "/opt/automacoes/cliquedf/operacional/static/index.html"
import shutil, time
ts = int(time.time())
shutil.copy2(INDEX, f"{INDEX}.bak.{ts}")
print(f"Backup: .bak.{ts}")

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

patches = []

# ── 1. CSS ────────────────────────────────────────────────────────────────────
OLD = ".topbar-right{margin-left:auto;display:flex;align-items:center;gap:8px}"
NEW = """.topbar-right{margin-left:auto;display:flex;align-items:center;gap:8px}
.topbar-filtros{display:flex;gap:6px;flex-wrap:wrap;align-items:center;padding:6px 16px 8px;border-top:1px solid rgba(255,255,255,.04);background:var(--bg)}
.tf-btn{background:var(--s2);border:1px solid var(--border);padding:4px 10px;border-radius:6px;font-size:10px;cursor:pointer;transition:all .15s;color:var(--muted);white-space:nowrap}
.tf-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.tf-btn.ativo{border-color:var(--cyan);color:var(--cyan);background:rgba(0,212,255,.1);font-weight:700}
.tf-data{background:var(--s2);border:1px solid var(--border);color:var(--text);padding:4px 6px;border-radius:6px;font-size:10px;font-family:inherit;width:110px}
.tf-sep{width:1px;height:16px;background:var(--border);flex-shrink:0}
.tf-clear{background:rgba(255,77,106,.12);border:1px solid rgba(255,77,106,.3);color:var(--red);padding:4px 10px;border-radius:6px;font-size:10px;cursor:pointer;white-space:nowrap}
.tf-status{font-size:9px;color:var(--muted);margin-left:4px}"""
patches.append(("CSS topbar", OLD, NEW))

# ── 2. Topbar HTML ────────────────────────────────────────────────────────────
OLD = """  <div class="topbar">
    <div>
      <div class="topbar-title" id="tb-title">Dashboard do dia</div>
      <div class="topbar-path" id="tb-path">/visao-geral/dashboard</div>
    </div>
    <div class="topbar-right">
      <div class="ws-dot" id="ws-dot" title="WebSocket"></div>
      <div class="date-chip" id="date-chip"></div>
      <button class="btn-sync" onclick="syncAgora()">↻ Sync IXC</button>
      <button class="btn-tv" onclick="abrirTV()">📺 TV</button>
    </div>
  </div>"""
NEW = """  <div class="topbar" style="flex-direction:column;gap:0;padding:0">
    <div style="display:flex;align-items:center;padding:0 16px;height:52px;width:100%;box-sizing:border-box">
      <div>
        <div class="topbar-title" id="tb-title">Dashboard do dia</div>
        <div class="topbar-path" id="tb-path">/visao-geral/dashboard</div>
      </div>
      <div class="topbar-right">
        <div class="ws-dot" id="ws-dot" title="WebSocket"></div>
        <div class="date-chip" id="date-chip"></div>
        <button class="btn-sync" onclick="syncAgora()">↻ Sync IXC</button>
        <button class="btn-tv" onclick="abrirTV()">📺 TV</button>
      </div>
    </div>
    <div class="topbar-filtros" id="topbar-filtros">
      <input type="date" class="tf-data" id="f-data-inicio" onchange="globalFiltroChange()" title="Data início">
      <input type="date" class="tf-data" id="f-data-fim"    onchange="globalFiltroChange()" title="Data fim">
      <div class="tf-sep"></div>
      <div class="tf-btn" id="fb-tecnico"      onclick="abrirFiltroModal('tecnico')">👷 Técnicos</div>
      <div class="tf-btn" id="fb-categoria"    onclick="abrirFiltroModal('categoria')">📂 Categoria</div>
      <div class="tf-btn" id="fb-cidade"       onclick="abrirFiltroModal('cidade')">🏙️ Cidade</div>
      <div class="tf-btn" id="fb-bairro"       onclick="abrirFiltroModal('bairro')">📍 Bairro</div>
      <div class="tf-btn" id="fb-concentrador" onclick="abrirFiltroModal('concentrador')">🔌 Concentrador</div>
      <div class="tf-btn" id="fb-assunto"      onclick="abrirFiltroModal('assunto')">📋 Serviço</div>
      <div class="tf-sep"></div>
      <div class="tf-clear" id="filtro-clear-btn" onclick="filtroLimparTodos()" style="display:none">✕ Limpar</div>
      <span class="tf-status" id="filtro-status"></span>
    </div>
  </div>"""
patches.append(("Topbar HTML", OLD, NEW))

# ── 3. Remove barra antiga do dashboard ──────────────────────────────────────
OLD = """      <!-- Barra de Filtros -->
      <div class="filtros-bar" id="filtros-bar">
        <input type="date" class="filtro-data" id="f-data-inicio" onchange="dashFiltroChange()" title="Data início">
        <input type="date" class="filtro-data" id="f-data-fim" onchange="dashFiltroChange()" title="Data fim">
        <div class="sep"></div>
        <div class="filtro-btn" id="fb-tecnico"     onclick="abrirFiltroModal('tecnico')">👷 Técnicos</div>
        <div class="filtro-btn" id="fb-categoria"   onclick="abrirFiltroModal('categoria')">📂 Categoria</div>
        <div class="filtro-btn" id="fb-bairro"      onclick="abrirFiltroModal('bairro')">📍 Bairro</div>
        <div class="filtro-btn" id="fb-concentrador" onclick="abrirFiltroModal('concentrador')">🔌 Concentrador</div>
        <div class="filtro-btn" id="fb-assunto"     onclick="abrirFiltroModal('assunto')">📋 Serviço</div>
        <div class="sep"></div>
        <div class="filtro-clear" id="filtro-clear-btn" onclick="filtroLimparTodos()" style="display:none">✕ Limpar filtros</div>
        <div style="font-size:10px;color:var(--muted);margin-left:auto" id="filtro-status"></div>
      </div>

      """
NEW = "      "
patches.append(("Remove barra antiga", OLD, NEW))

# ── 4. _filtros + cidade ──────────────────────────────────────────────────────
OLD = "let _filtros = { tecnico:[], categoria:[], bairro:[], concentrador:[], assunto:[] }"
NEW = "let _filtros = { tecnico:[], categoria:[], cidade:[], bairro:[], concentrador:[], assunto:[] }"
patches.append(("_filtros cidade", OLD, NEW))

# ── 5. titulos modal ──────────────────────────────────────────────────────────
OLD = "  const titulos = {\n    tecnico:'👷 Técnicos', categoria:'📂 Categoria',\n    bairro:'📍 Bairros', concentrador:'🔌 Concentradores', assunto:'📋 Serviços'\n  }"
NEW = "  const titulos = {\n    tecnico:'👷 Técnicos', categoria:'📂 Categoria', cidade:'🏙️ Cidades',\n    bairro:'📍 Bairros', concentrador:'🔌 Concentradores', assunto:'📋 Serviços'\n  }"
patches.append(("Titulos modal", OLD, NEW))

# ── 6. listas modal ───────────────────────────────────────────────────────────
OLD = """  const listas = {
    tecnico:      opts.tecnicos?.map(t=>({id:t.id,  nome:t.nome})) || [],
    categoria:    opts.categorias?.map(c=>({id:c.id, nome:c.nome})) || [],
    bairro:       (opts.bairros||[]).map(b=>({id:b,  nome:b})),
    concentrador: (opts.concentradores||[]).map(c=>({id:c, nome:c})),
    assunto:      opts.assuntos?.map(a=>({id:a.id,  nome:a.nome})) || [],
  }"""
NEW = """  const listas = {
    tecnico:      opts.tecnicos?.map(t=>({id:t.id,  nome:t.nome})) || [],
    categoria:    opts.categorias?.map(c=>({id:c.id, nome:c.nome})) || [],
    cidade:       (opts.cidades||[]).map(c=>({id:c,  nome:c})),
    bairro:       (opts.bairros||[]).map(b=>({id:b,  nome:b})),
    concentrador: (opts.concentradores||[]).map(c=>({id:c, nome:c})),
    assunto:      opts.assuntos?.map(a=>({id:a.id,  nome:a.nome})) || [],
  }"""
patches.append(("Listas modal", OLD, NEW))

# ── 7. atualizarBotoesFiltro ──────────────────────────────────────────────────
OLD = """function atualizarBotoesFiltro(){
  const tipos = ['tecnico','categoria','bairro','concentrador','assunto']
  let algumAtivo = false
  tipos.forEach(t=>{
    const btn = document.getElementById('fb-'+t)
    const ativo = (_filtros[t]||[]).length > 0
    if(ativo){ algumAtivo=true; btn.classList.add('ativo') }
    else btn.classList.remove('ativo')
    const cnt = (_filtros[t]||[]).length
    const labels = {tecnico:'👷 Técnicos',categoria:'📂 Categoria',bairro:'📍 Bairro',concentrador:'🔌 Concentrador',assunto:'📋 Serviço'}
    btn.textContent = cnt > 0 ? `${labels[t]} (${cnt})` : labels[t]
  })
  const diInicio = document.getElementById('f-data-inicio')?.value
  const diFim    = document.getElementById('f-data-fim')?.value
  if(diInicio||diFim) algumAtivo = true
  const clearBtn = document.getElementById('filtro-clear-btn')
  if(clearBtn) clearBtn.style.display = algumAtivo ? '' : 'none'
  const status = document.getElementById('filtro-status')
  if(status){
    const total = tipos.reduce((s,t)=>s+(_filtros[t]||[]).length, 0)
    status.textContent = total > 0 ? `${total} filtro${total>1?'s':''} ativo${total>1?'s':''}` : ''
  }
}"""
NEW = """function atualizarBotoesFiltro(){
  const tipos = ['tecnico','categoria','cidade','bairro','concentrador','assunto']
  let algumAtivo = false
  tipos.forEach(t=>{
    const btn = document.getElementById('fb-'+t)
    if(!btn) return
    const ativo = (_filtros[t]||[]).length > 0
    if(ativo){ algumAtivo=true; btn.classList.add('ativo') }
    else btn.classList.remove('ativo')
    const cnt = (_filtros[t]||[]).length
    const labels = {tecnico:'👷 Técnicos',categoria:'📂 Categoria',cidade:'🏙️ Cidade',bairro:'📍 Bairro',concentrador:'🔌 Concentrador',assunto:'📋 Serviço'}
    btn.textContent = cnt > 0 ? `${labels[t]} (${cnt})` : labels[t]
  })
  const diInicio = document.getElementById('f-data-inicio')?.value
  const diFim    = document.getElementById('f-data-fim')?.value
  if(diInicio||diFim) algumAtivo = true
  const clearBtn = document.getElementById('filtro-clear-btn')
  if(clearBtn) clearBtn.style.display = algumAtivo ? '' : 'none'
  const status = document.getElementById('filtro-status')
  if(status){
    const total = tipos.reduce((s,t)=>s+(_filtros[t]||[]).length, 0)
    status.textContent = total > 0 ? `${total} filtro${total>1?'s':''}` : ''
  }
}"""
patches.append(("atualizarBotoesFiltro", OLD, NEW))

# ── 8. getFiltrosParams + cidade ──────────────────────────────────────────────
OLD = """  if(_filtros.tecnico.length)      p.set('tecnico_id',  _filtros.tecnico.join(','))
  if(_filtros.categoria.length)    p.set('categoria',   _filtros.categoria.join(','))
  if(_filtros.bairro.length)       p.set('bairro',      _filtros.bairro.join(','))
  if(_filtros.assunto.length)      p.set('assunto_id',  _filtros.assunto.join(','))"""
NEW = """  if(_filtros.tecnico.length)      p.set('tecnico_id',  _filtros.tecnico.join(','))
  if(_filtros.categoria.length)    p.set('categoria',   _filtros.categoria.join(','))
  if(_filtros.cidade.length)       p.set('cidade',      _filtros.cidade.join(','))
  if(_filtros.bairro.length)       p.set('bairro',      _filtros.bairro.join(','))
  if(_filtros.assunto.length)      p.set('assunto_id',  _filtros.assunto.join(','))"""
patches.append(("getFiltrosParams cidade", OLD, NEW))

# ── 9. filtroLimparTodos ──────────────────────────────────────────────────────
OLD = "  _filtros = { tecnico:[], categoria:[], bairro:[], concentrador:[], assunto:[] }"
NEW = "  _filtros = { tecnico:[], categoria:[], cidade:[], bairro:[], concentrador:[], assunto:[] }"
patches.append(("filtroLimparTodos cidade", OLD, NEW))

# ── 10. dashFiltroChange → globalFiltroChange ─────────────────────────────────
OLD = "function dashFiltroChange(){ loadDashboard() }"
NEW = """function globalFiltroChange(){
  const paginasReativas = ['dashboard','ranking','agenda-dia','auditoria','os-atrasadas','tecnicos-ociosos']
  if(paginasReativas.includes(currentPage)){
    const cfg = PAGES_CFG[currentPage]
    if(cfg?.load) cfg.load()
  }
}"""
patches.append(("globalFiltroChange", OLD, NEW))

# ── 11. filtroAplicar usa globalFiltroChange ──────────────────────────────────
OLD = """  document.getElementById('filtro-modal-overlay').classList.remove('active')
  atualizarBotoesFiltro()
  loadDashboard()"""
NEW = """  document.getElementById('filtro-modal-overlay').classList.remove('active')
  atualizarBotoesFiltro()
  globalFiltroChange()"""
patches.append(("filtroAplicar globalFiltroChange", OLD, NEW))

# ── 12. loadRanking com filtros ───────────────────────────────────────────────
OLD = """async function loadRanking(data,btn=null){
  if(btn){document.querySelectorAll('#page-ranking .period-tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active')}
  const params = getFiltrosParams()
  const base = data?`${SAIS}/pontuacao/ranking?data=${data}`:`${SAIS}/pontuacao/ranking`
  const url = params ? base+(base.includes('?')?'&':'?')+params : base
  const d=await apiFetch(url)"""
NEW = """async function loadRanking(data,btn=null){
  if(btn){document.querySelectorAll('#page-ranking .period-tab').forEach(t=>t.classList.remove('active'));btn.classList.add('active')}
  const fp = getFiltrosParams()
  const base = data?`${SAIS}/pontuacao/ranking?data=${data}`:`${SAIS}/pontuacao/ranking`
  const url = fp ? base+(base.includes('?')?'&':'?')+fp : base
  const d=await apiFetch(url)"""
patches.append(("loadRanking filtros", OLD, NEW))

# ── 13. loadAgendaDia com filtros ─────────────────────────────────────────────
OLD = """  const params=getFiltrosParams()
  const agUrl=params?`${SAIS}/agenda/dia?data=${dataStr}&${params}`:`${SAIS}/agenda/dia?data=${dataStr}`
  const d=await apiFetch(agUrl)"""
# fallback se patch anterior não foi aplicado
OLD_ALT = "  const d=await apiFetch(`${SAIS}/agenda/dia?data=${dataStr}`)"
NEW_AG = """  const _agFp=getFiltrosParams()
  const d=await apiFetch(_agFp?`${SAIS}/agenda/dia?data=${dataStr}&${_agFp}`:`${SAIS}/agenda/dia?data=${dataStr}`)"""

if OLD in html:
    html = html.replace(OLD, NEW_AG, 1)
    print("✅ patch 13a: loadAgendaDia filtros (já tinha params)")
elif OLD_ALT in html:
    html = html.replace(OLD_ALT, NEW_AG, 1)
    print("✅ patch 13b: loadAgendaDia filtros")
else:
    print("❌ patch 13: loadAgendaDia não encontrado")

# ── 14. loadAuditoria com filtros ─────────────────────────────────────────────
OLD14 = "async function loadAuditoria(){\n  const d=await apiFetch(`${SAIS}/auditoria/ocorrencias`)"
NEW14 = "async function loadAuditoria(){\n  const _auFp=getFiltrosParams()\n  const d=await apiFetch(_auFp?`${SAIS}/auditoria/ocorrencias?${_auFp}`:`${SAIS}/auditoria/ocorrencias`)"
patches.append(("loadAuditoria filtros", OLD14, NEW14))

# ── Aplica todos os patches ───────────────────────────────────────────────────
for nome, old, new in patches:
    if old in html:
        html = html.replace(old, new, 1)
        print(f"✅ {nome}")
    else:
        print(f"⚠️  {nome} — não encontrado")

with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("\n✅ index.html salvo!")
print("✅ Rode: systemctl restart hubprod_cliquedf")
