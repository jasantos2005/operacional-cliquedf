#!/usr/bin/env python3
"""
Patch SAIS — Filtros em Modal + Topbar Limpa
1. Remove título, path e date-chip da topbar
2. Remove segunda linha de filtros da topbar
3. Adiciona botão "Filtros" na topbar
4. Cria modal de filtros completo (datas + grid de filtros)
"""

INDEX = "/opt/automacoes/cliquedf/operacional/static/index.html"
import shutil, time
ts = int(time.time())
shutil.copy2(INDEX, f"{INDEX}.bak.{ts}")
print(f"Backup: .bak.{ts}")

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

patches = []

# ── 1. CSS: remove topbar-filtros, adiciona modal de filtros ──────────────────
OLD_CSS = """.topbar-filtros{display:flex;gap:6px;flex-wrap:wrap;align-items:center;padding:6px 16px 8px;border-top:1px solid rgba(255,255,255,.04);background:var(--bg)}
.tf-btn{background:var(--s2);border:1px solid var(--border);padding:4px 10px;border-radius:6px;font-size:10px;cursor:pointer;transition:all .15s;color:var(--muted);white-space:nowrap}
.tf-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.tf-btn.ativo{border-color:var(--cyan);color:var(--cyan);background:rgba(0,212,255,.1);font-weight:700}
.tf-data{background:var(--s2);border:1px solid var(--border);color:var(--text);padding:4px 6px;border-radius:6px;font-size:10px;font-family:inherit;width:110px}
.tf-sep{width:1px;height:16px;background:var(--border);flex-shrink:0}
.tf-clear{background:rgba(255,77,106,.12);border:1px solid rgba(255,77,106,.3);color:var(--red);padding:4px 10px;border-radius:6px;font-size:10px;cursor:pointer;white-space:nowrap}
.tf-status{font-size:9px;color:var(--muted);margin-left:4px}"""

NEW_CSS = """.btn-filtros{background:var(--s2);border:1px solid var(--border);color:var(--muted);padding:5px 12px;border-radius:6px;font-size:11px;cursor:pointer;transition:all .2s;display:flex;align-items:center;gap:5px}
.btn-filtros:hover{border-color:var(--cyan);color:var(--cyan)}
.btn-filtros.ativo{border-color:var(--cyan);color:var(--cyan);background:rgba(0,212,255,.1);font-weight:700}
.fm-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:3000;display:none;align-items:flex-start;justify-content:flex-end;padding-top:56px;padding-right:16px}
.fm-overlay.active{display:flex}
.fm-box{background:var(--s1);border:1px solid var(--border);border-radius:12px;padding:20px;width:520px;max-height:80vh;overflow-y:auto;box-shadow:0 8px 32px rgba(0,0,0,.5)}
.fm-titulo{font-size:14px;font-weight:800;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center}
.fm-section{margin-bottom:16px}
.fm-section-lbl{font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:8px}
.fm-datas{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px}
.fm-data-input{background:var(--s2);border:1px solid var(--border);color:var(--text);padding:7px 10px;border-radius:6px;font-size:12px;font-family:inherit;width:100%;box-sizing:border-box}
.fm-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.fm-filtro-btn{background:var(--s2);border:1px solid var(--border);padding:8px 12px;border-radius:8px;font-size:11px;cursor:pointer;transition:all .15s;color:var(--muted);text-align:left;display:flex;justify-content:space-between;align-items:center}
.fm-filtro-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.fm-filtro-btn.ativo{border-color:var(--cyan);color:var(--cyan);background:rgba(0,212,255,.08);font-weight:700}
.fm-actions{display:flex;gap:8px;margin-top:16px;padding-top:14px;border-top:1px solid var(--border)}
.fm-aplicar{flex:1;background:var(--cyan);color:#000;border:none;padding:9px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer}
.fm-limpar{background:rgba(255,77,106,.15);color:var(--red);border:1px solid rgba(255,77,106,.3);padding:9px 16px;border-radius:8px;font-size:12px;cursor:pointer}
.fm-status{font-size:10px;color:var(--muted);text-align:center;margin-top:8px}"""

patches.append(("CSS modal filtros", OLD_CSS, NEW_CSS))

# ── 2. Topbar HTML: remove título, path, date-chip e filtros-bar ──────────────
OLD_TOPBAR = """  <div class="topbar" style="flex-direction:column;gap:0;padding:0">
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

NEW_TOPBAR = """  <div class="topbar">
    <div class="topbar-right" style="margin-left:0;width:100%">
      <div class="ws-dot" id="ws-dot" title="WebSocket"></div>
      <div style="flex:1"></div>
      <button class="btn-filtros" id="btn-filtros-global" onclick="abrirModalFiltros()">
        <span>⚙</span> Filtros <span id="filtros-badge" style="display:none;background:var(--cyan);color:#000;border-radius:10px;padding:1px 6px;font-size:9px;font-weight:800"></span>
      </button>
      <button class="btn-sync" onclick="syncAgora()">↻ Sync IXC</button>
      <button class="btn-tv" onclick="abrirTV()">📺 TV</button>
    </div>
  </div>"""

patches.append(("Topbar limpa", OLD_TOPBAR, NEW_TOPBAR))

# ── 3. Modal de filtros HTML (antes do modal de filtro antigo) ─────────────────
OLD_MODAL_ANT = "<!-- ══ MODAL FILTROS ════════════════════════════════ -->\n<div class=\"filtro-modal-overlay\" id=\"filtro-modal-overlay\">"
NEW_MODAL = """<!-- ══ MODAL FILTROS GLOBAL ═══════════════════════════ -->
<div class="fm-overlay" id="fm-overlay" onclick="fmOverlayClick(event)">
  <div class="fm-box">
    <div class="fm-titulo">
      <span>⚙ Filtros Globais</span>
      <span onclick="fecharModalFiltros()" style="cursor:pointer;color:var(--muted);font-size:18px">×</span>
    </div>

    <!-- Datas -->
    <div class="fm-section">
      <div class="fm-section-lbl">Período</div>
      <div class="fm-datas">
        <div>
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px">Data início</div>
          <input type="date" class="fm-data-input" id="f-data-inicio" onchange="fmAtualizar()">
        </div>
        <div>
          <div style="font-size:10px;color:var(--muted);margin-bottom:4px">Data fim</div>
          <input type="date" class="fm-data-input" id="f-data-fim" onchange="fmAtualizar()">
        </div>
      </div>
    </div>

    <!-- Filtros em grid -->
    <div class="fm-section">
      <div class="fm-section-lbl">Filtros</div>
      <div class="fm-grid">
        <div class="fm-filtro-btn" id="fb-tecnico"      onclick="abrirFiltroModal('tecnico')">
          <span>👷 Técnicos</span><span id="fc-tecnico" style="color:var(--cyan);font-size:10px"></span>
        </div>
        <div class="fm-filtro-btn" id="fb-categoria"    onclick="abrirFiltroModal('categoria')">
          <span>📂 Categoria</span><span id="fc-categoria" style="color:var(--cyan);font-size:10px"></span>
        </div>
        <div class="fm-filtro-btn" id="fb-cidade"       onclick="abrirFiltroModal('cidade')">
          <span>🏙️ Cidade</span><span id="fc-cidade" style="color:var(--cyan);font-size:10px"></span>
        </div>
        <div class="fm-filtro-btn" id="fb-bairro"       onclick="abrirFiltroModal('bairro')">
          <span>📍 Bairro</span><span id="fc-bairro" style="color:var(--cyan);font-size:10px"></span>
        </div>
        <div class="fm-filtro-btn" id="fb-concentrador" onclick="abrirFiltroModal('concentrador')">
          <span>🔌 Concentrador</span><span id="fc-concentrador" style="color:var(--cyan);font-size:10px"></span>
        </div>
        <div class="fm-filtro-btn" id="fb-assunto"      onclick="abrirFiltroModal('assunto')">
          <span>📋 Serviço</span><span id="fc-assunto" style="color:var(--cyan);font-size:10px"></span>
        </div>
      </div>
    </div>

    <div class="fm-actions">
      <button class="fm-limpar" onclick="filtroLimparTodos()">✕ Limpar tudo</button>
      <button class="fm-aplicar" onclick="fmAplicarEFechar()">✓ Aplicar filtros</button>
    </div>
    <div class="fm-status" id="fm-status"></div>
  </div>
</div>

<!-- ══ MODAL FILTROS ITEM (seleção múltipla) ═════════════════════════════ -->
""" + "<!-- ══ MODAL FILTROS ════════════════════════════════ -->\n<div class=\"filtro-modal-overlay\" id=\"filtro-modal-overlay\">"

patches.append(("Modal filtros global", OLD_MODAL_ANT, NEW_MODAL))

# ── 4. JS: funções do modal de filtros global ─────────────────────────────────
OLD_SISTEMA = "// ═══════════════════════════════════════════════════\n// SISTEMA DE FILTROS GLOBAIS\n// ═══════════════════════════════════════════════════"
NEW_SISTEMA = """// ═══════════════════════════════════════════════════
// SISTEMA DE FILTROS GLOBAIS
// ═══════════════════════════════════════════════════
function abrirModalFiltros(){
  document.getElementById('fm-overlay').classList.add('active')
  fmAtualizar()
}
function fecharModalFiltros(){
  document.getElementById('fm-overlay').classList.remove('active')
}
function fmOverlayClick(e){
  if(e.target===document.getElementById('fm-overlay')) fecharModalFiltros()
}
function fmAtualizar(){
  const tipos = ['tecnico','categoria','cidade','bairro','concentrador','assunto']
  let total = 0
  tipos.forEach(t=>{
    const cnt = (_filtros[t]||[]).length
    total += cnt
    const fc = document.getElementById('fc-'+t)
    const fb = document.getElementById('fb-'+t)
    if(fc) fc.textContent = cnt > 0 ? cnt+' sel.' : ''
    if(fb){ fb.classList.toggle('ativo', cnt>0) }
  })
  const di = document.getElementById('f-data-inicio')?.value
  const df = document.getElementById('f-data-fim')?.value
  if(di||df) total++

  const badge = document.getElementById('filtros-badge')
  const btnFiltros = document.getElementById('btn-filtros-global')
  if(badge){
    badge.style.display = total > 0 ? '' : 'none'
    badge.textContent = total
  }
  if(btnFiltros) btnFiltros.classList.toggle('ativo', total>0)

  const status = document.getElementById('fm-status')
  if(status) status.textContent = total > 0 ? `${total} filtro${total>1?'s':''} ativo${total>1?'s':''}` : 'Nenhum filtro ativo'
}
function fmAplicarEFechar(){
  fecharModalFiltros()
  globalFiltroChange()
}"""

patches.append(("JS modal filtros global", OLD_SISTEMA, NEW_SISTEMA))

# ── 5. Atualiza atualizarBotoesFiltro para chamar fmAtualizar ──────────────────
OLD_ATU = """  const clearBtn = document.getElementById('filtro-clear-btn')
  if(clearBtn) clearBtn.style.display = algumAtivo ? '' : 'none'
  const status = document.getElementById('filtro-status')
  if(status){
    const total = tipos.reduce((s,t)=>s+(_filtros[t]||[]).length, 0)
    status.textContent = total > 0 ? `${total} filtro${total>1?'s':''}` : ''
  }
}"""
NEW_ATU = """  fmAtualizar()
}"""
patches.append(("atualizarBotoesFiltro simplificado", OLD_ATU, NEW_ATU))

# ── 6. filtroLimparTodos fecha modal e recarrega ──────────────────────────────
OLD_LIMPAR = """function filtroLimparTodos(){
  _filtros = { tecnico:[], categoria:[], cidade:[], bairro:[], concentrador:[], assunto:[] }
  document.getElementById('f-data-inicio').value = ''
  document.getElementById('f-data-fim').value = ''
  atualizarBotoesFiltro()
  globalFiltroChange()
}"""
NEW_LIMPAR = """function filtroLimparTodos(){
  _filtros = { tecnico:[], categoria:[], cidade:[], bairro:[], concentrador:[], assunto:[] }
  const di = document.getElementById('f-data-inicio')
  const df = document.getElementById('f-data-fim')
  if(di) di.value = ''
  if(df) df.value = ''
  atualizarBotoesFiltro()
  fecharModalFiltros()
  globalFiltroChange()
}"""
patches.append(("filtroLimparTodos", OLD_LIMPAR, NEW_LIMPAR))

# ── 7. temFiltrosAtivos usa os inputs corretos ────────────────────────────────
OLD_TEM = """function temFiltrosAtivos(){
  const di = document.getElementById('f-data-inicio')?.value
  const df = document.getElementById('f-data-fim')?.value
  return di || df || Object.values(_filtros).some(v=>v.length>0)
}"""
NEW_TEM = """function temFiltrosAtivos(){
  const di = document.getElementById('f-data-inicio')?.value
  const df = document.getElementById('f-data-fim')?.value
  return !!(di || df || Object.values(_filtros).some(v=>v.length>0))
}"""
patches.append(("temFiltrosAtivos", OLD_TEM, NEW_TEM))

# ── 8. Remove date-chip do JS (não existe mais no HTML) ────────────────────────
OLD_DATECHIP = "document.getElementById('date-chip').textContent=new Date().toLocaleDateString('pt-BR',{weekday:'short',day:'2-digit',month:'short'})"
NEW_DATECHIP = "// date-chip removido da topbar"
patches.append(("date-chip JS", OLD_DATECHIP, NEW_DATECHIP))

# ── 9. Remove tb-title e tb-path do nav (não existem mais) ────────────────────
OLD_NAV_TITLE = """  document.getElementById('tb-title').textContent=cfg.title
  document.getElementById('tb-path').textContent=cfg.path"""
NEW_NAV_TITLE = "  // tb-title/path removidos"
patches.append(("nav title/path", OLD_NAV_TITLE, NEW_NAV_TITLE))

# ── Aplica ────────────────────────────────────────────────────────────────────
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
