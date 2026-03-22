#!/usr/bin/env python3
"""
Patch SAIS — Filtros no Dashboard
1. Novo endpoint /api/sais/visao-geral/resumo-filtrado (aceita todos os filtros)
2. Novo endpoint /api/sais/visao-geral/filtros-opcoes (retorna técnicos, categorias, bairros do IXC)
3. Barra de filtros no dashboard (data, técnico, categoria, bairro, concentrador, setor)
4. loadDashboard usa filtros ativos
"""

INDEX   = "/opt/automacoes/cliquedf/operacional/static/index.html"
VG_PATH = "/opt/automacoes/cliquedf/operacional/app/routes/sais/visao_geral.py"

import shutil, time
ts = int(time.time())
shutil.copy2(INDEX,   f"{INDEX}.bak.{ts}")
shutil.copy2(VG_PATH, f"{VG_PATH}.bak.{ts}")
print(f"💾 Backups criados (.bak.{ts})")

# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 1 — Novos endpoints no visao_geral.py
# ═══════════════════════════════════════════════════════════════════════════════

with open(VG_PATH, "r", encoding="utf-8") as f:
    vg = f.read()

NEW_ENDPOINTS = '''

@router.get("/filtros-opcoes")
async def get_filtros_opcoes():
    """Retorna opções para os filtros: técnicos, categorias, bairros (IXC), assuntos."""
    import os as _os, pymysql
    db = get_db()

    # Técnicos ativos
    tecs = db.execute(
        "SELECT id, nome, ixc_funcionario_id FROM prod_tecnicos WHERE ativo=1 ORDER BY nome"
    ).fetchall()

    # Assuntos disponíveis
    assuntos = db.execute(
        "SELECT id, assunto FROM prod_assuntos ORDER BY assunto LIMIT 100"
    ).fetchall()

    db.close()

    categorias = [
        {"id": "servico",  "nome": "Serviço"},
        {"id": "suporte",  "nome": "Suporte"},
        {"id": "infra",    "nome": "Infra"},
        {"id": "retirada", "nome": "Retirada"},
    ]

    # Bairros do IXC
    bairros = []
    concentradores = []
    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=8,
        )
        with ixc.cursor() as cur:
            # Bairros das OS recentes
            cur.execute("""
                SELECT DISTINCT c.bairro
                FROM su_oss_chamado o
                LEFT JOIN cliente c ON c.id = o.id_cliente
                WHERE o.data_abertura >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND c.bairro IS NOT NULL AND c.bairro != ''
                ORDER BY c.bairro
                LIMIT 80
            """)
            bairros = [r["bairro"] for r in cur.fetchall() if r["bairro"]]

            # Concentradores (assuntos que contém CTO/OLT/splitter)
            cur.execute("""
                SELECT DISTINCT descricao as nome
                FROM su_oss_assunto
                WHERE descricao LIKE '%CTO%' OR descricao LIKE '%OLT%'
                   OR descricao LIKE '%SPLITTER%' OR descricao LIKE '%FIBRA%'
                ORDER BY descricao
                LIMIT 50
            """)
            concentradores = [r["nome"] for r in cur.fetchall() if r["nome"]]
        ixc.close()
    except Exception as e:
        pass

    return {
        "tecnicos":      [{"id": t["ixc_funcionario_id"], "nome": t["nome"]} for t in tecs],
        "categorias":    categorias,
        "assuntos":      [{"id": a["id"], "nome": a["assunto"]} for a in assuntos],
        "bairros":       bairros,
        "concentradores": concentradores,
    }


@router.get("/resumo-filtrado")
async def get_resumo_filtrado(
    data_inicio:   Optional[str] = Query(None),
    data_fim:      Optional[str] = Query(None),
    tecnico_id:    Optional[str] = Query(None),   # comma-separated ids
    categoria:     Optional[str] = Query(None),   # comma-separated
    bairro:        Optional[str] = Query(None),   # comma-separated
    assunto_id:    Optional[str] = Query(None),   # comma-separated
):
    """Resumo do dashboard com filtros aplicados."""
    import os as _os, pymysql
    db = get_db()
    hoje = hoje_brt()
    data_inicio = str(data_inicio) if data_inicio else hoje
    data_fim    = str(data_fim)    if data_fim    else hoje

    # Monta filtros SQLite
    where  = ["DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') BETWEEN ? AND ?"]
    params = [data_inicio, data_fim]

    # Técnico: converter ixc_funcionario_id → id interno
    tec_ids_internos = []
    if tecnico_id:
        ixc_ids = [int(x) for x in tecnico_id.split(",") if x.strip().isdigit()]
        if ixc_ids:
            ph = ",".join("?" * len(ixc_ids))
            rows = db.execute(
                f"SELECT id FROM prod_tecnicos WHERE ixc_funcionario_id IN ({ph})", ixc_ids
            ).fetchall()
            tec_ids_internos = [r["id"] for r in rows]
            if tec_ids_internos:
                ph2 = ",".join("?" * len(tec_ids_internos))
                where.append(f"tecnico_id IN ({ph2})")
                params.extend(tec_ids_internos)

    # Categoria
    if categoria:
        cats = [c.strip() for c in categoria.split(",") if c.strip()]
        if cats:
            ph = ",".join("?" * len(cats))
            where.append(f"categoria IN ({ph})")
            params.extend(cats)

    # Assunto
    if assunto_id:
        aids = [int(x) for x in assunto_id.split(",") if x.strip().isdigit()]
        if aids:
            ph = ",".join("?" * len(aids))
            where.append(f"ixc_assunto_id IN ({ph})")
            params.extend(aids)

    # Bairro — precisamos filtrar via IXC (busca os os_ids do bairro)
    if bairro:
        bairros_lista = [b.strip() for b in bairro.split(",") if b.strip()]
        os_ids_bairro = []
        try:
            ixc = pymysql.connect(
                host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
                user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
                database=_os.getenv("DB_NAME"),
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=8,
            )
            ph_b = ",".join(["%s"] * len(bairros_lista))
            with ixc.cursor() as cur:
                cur.execute(f"""
                    SELECT o.id FROM su_oss_chamado o
                    LEFT JOIN cliente c ON c.id = o.id_cliente
                    WHERE c.bairro IN ({ph_b})
                      AND o.data_abertura >= DATE_SUB(NOW(), INTERVAL 60 DAY)
                """, bairros_lista)
                os_ids_bairro = [r["id"] for r in cur.fetchall()]
            ixc.close()
        except: pass
        if os_ids_bairro:
            ph = ",".join("?" * len(os_ids_bairro))
            where.append(f"ixc_os_id IN ({ph})")
            params.extend(os_ids_bairro)

    where_sql = " AND ".join(where)

    resumo = dict(db.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas,
            SUM(CASE WHEN categoria='servico'  THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN categoria='suporte'  THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN categoria='infra'    THEN 1 ELSE 0 END) AS infra,
            SUM(CASE WHEN categoria='retirada' THEN 1 ELSE 0 END) AS retiradas
        FROM prod_os_cache
        WHERE {where_sql}
    """, params).fetchone())

    meta_row = db.execute("SELECT valor FROM sais_config WHERE chave='meta_dia'").fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150
    fins = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    # Ranking filtrado
    rank_where  = [w for w in where]
    rank_params = list(params)
    ranking_rows = db.execute(f"""
        SELECT
            t.nome, t.ixc_funcionario_id AS tecnico_id,
            COUNT(*) AS total_os,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            o.categoria
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        WHERE {where_sql}
        GROUP BY o.tecnico_id
        ORDER BY finalizadas DESC
        LIMIT 20
    """, rank_params).fetchall()

    # Pontuação
    pontos_row = db.execute(f"""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS pontos
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
        WHERE o.status='finalizada' AND {where_sql}
    """, params).fetchone()

    tecs_ativos = db.execute(f"""
        SELECT COUNT(DISTINCT tecnico_id) as total
        FROM prod_os_cache WHERE {where_sql}
    """, params).fetchone()

    db.close()

    return {
        "data_inicio":   data_inicio,
        "data_fim":      data_fim,
        "filtros_ativos": {
            "tecnico_id": tecnico_id,
            "categoria":  categoria,
            "bairro":     bairro,
            "assunto_id": assunto_id,
        },
        "resumo":         resumo,
        "meta_dia":       meta,
        "meta_percentual": pct_meta,
        "tecnicos_ativos": tecs_ativos["total"] if tecs_ativos else 0,
        "total_pontos":   pontos_row["pontos"] or 0 if pontos_row else 0,
        "ranking": [{
            "nome":       r["nome"] or "—",
            "tecnico_id": r["tecnico_id"],
            "total_os":   r["total_os"],
            "finalizadas": r["finalizadas"],
            "pct_meta":   round(r["finalizadas"] / (meta / 11) * 100) if meta > 0 else 0,
            "score":      r["finalizadas"],
            "eficiencia": round(r["finalizadas"] / r["total_os"] * 100) if r["total_os"] > 0 else 0,
        } for r in ranking_rows],
    }
'''

ANCHOR = '@router.get("/alertas")'
if ANCHOR in vg:
    vg = vg.replace(ANCHOR, NEW_ENDPOINTS + "\n\n" + ANCHOR, 1)
    with open(VG_PATH, "w", encoding="utf-8") as f:
        f.write(vg)
    print("✅ PARTE 1: endpoints adicionados")
else:
    print("❌ PARTE 1: anchor não encontrado")


# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 2 — index.html: CSS + HTML da barra de filtros + JS
# ═══════════════════════════════════════════════════════════════════════════════

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

# ── CSS ──────────────────────────────────────────────────────────────────────
OLD_CSS = ".ev-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px}"
NEW_CSS = """.ev-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:20px}
.filtros-bar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;padding:12px 16px;background:var(--s1);border:1px solid var(--border);border-radius:10px;align-items:center}
.filtro-btn{background:var(--s2);border:1px solid var(--border);padding:5px 12px;border-radius:6px;font-size:11px;cursor:pointer;transition:all .2s;color:var(--muted);white-space:nowrap}
.filtro-btn:hover{border-color:var(--cyan);color:var(--cyan)}
.filtro-btn.ativo{border-color:var(--cyan);color:var(--cyan);background:rgba(0,212,255,.1)}
.filtro-data{background:var(--s2);border:1px solid var(--border);color:var(--text);padding:5px 8px;border-radius:6px;font-size:11px;font-family:inherit}
.filtros-bar .sep{width:1px;height:20px;background:var(--border);flex-shrink:0}
.filtro-clear{background:rgba(255,77,106,.15);border:1px solid var(--red);color:var(--red);padding:5px 12px;border-radius:6px;font-size:11px;cursor:pointer;white-space:nowrap}
/* Modal de filtro */
.filtro-modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:3000;display:none;align-items:center;justify-content:center}
.filtro-modal-overlay.active{display:flex}
.filtro-modal-box{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:16px;width:340px;max-height:480px;display:flex;flex-direction:column}
.filtro-modal-title{font-size:13px;font-weight:700;margin-bottom:10px}
.filtro-search{background:var(--s2);border:1px solid var(--border);color:var(--text);padding:7px 10px;border-radius:6px;font-size:11px;width:100%;margin-bottom:8px;box-sizing:border-box}
.filtro-lista{flex:1;overflow-y:auto;max-height:280px;margin-bottom:10px}
.filtro-item{display:flex;align-items:center;gap:8px;padding:7px 4px;cursor:pointer;border-radius:4px;font-size:12px}
.filtro-item:hover{background:rgba(255,255,255,.04)}
.filtro-item input{accent-color:var(--cyan)}
.filtro-modal-actions{display:flex;gap:8px;justify-content:flex-end}
.filtro-modal-actions button{padding:6px 14px;border-radius:6px;font-size:11px;cursor:pointer;border:none}
.fm-ok{background:var(--cyan);color:#000;font-weight:700}
.fm-clear{background:rgba(255,77,106,.2);color:var(--red);border:1px solid var(--red)!important}"""

if OLD_CSS in html:
    html = html.replace(OLD_CSS, NEW_CSS, 1)
    print("✅ PARTE 2a: CSS adicionado")
else:
    print("❌ PARTE 2a: CSS anchor não encontrado")

# ── Modal de filtro HTML (antes do </body>) ───────────────────────────────────
OLD_SCRIPT = "<script>"
NEW_MODAL_HTML = """<!-- ══ MODAL FILTROS ════════════════════════════════ -->
<div class="filtro-modal-overlay" id="filtro-modal-overlay">
  <div class="filtro-modal-box">
    <div class="filtro-modal-title" id="filtro-modal-title">Filtro</div>
    <input class="filtro-search" id="filtro-search" placeholder="Buscar..." oninput="filtroSearchUpdate()">
    <div class="filtro-lista" id="filtro-lista"></div>
    <div class="filtro-modal-actions">
      <button class="fm-clear" onclick="filtroClear()">Limpar</button>
      <button class="fm-ok" onclick="filtroAplicar()">Aplicar</button>
    </div>
  </div>
</div>

<script>"""

# Substitui só a primeira ocorrência de <script> (antes do JS principal)
html = html.replace(OLD_SCRIPT, NEW_MODAL_HTML, 1)
print("✅ PARTE 2b: modal HTML adicionado")

# ── HTML da barra de filtros no dashboard ─────────────────────────────────────
OLD_DB_PAGE = """      <div class="alert-chips" id="db-alertas"></div>
      <div class="kpi-grid c5" id="db-kpis"><div class="loading"><div class="spinner"></div>Carregando...</div></div>"""

NEW_DB_PAGE = """      <div class="alert-chips" id="db-alertas"></div>

      <!-- Barra de Filtros -->
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

      <div class="kpi-grid c5" id="db-kpis"><div class="loading"><div class="spinner"></div>Carregando...</div></div>"""

if OLD_DB_PAGE in html:
    html = html.replace(OLD_DB_PAGE, NEW_DB_PAGE, 1)
    print("✅ PARTE 2c: barra de filtros adicionada ao dashboard")
else:
    print("❌ PARTE 2c: anchor do dashboard não encontrado")

# ── JS: sistema de filtros + atualizar loadDashboard ─────────────────────────
OLD_LOAD_DASH = """async function loadDashboard(){
  try{
    const [resumo,alertas,top]=await Promise.all([
      apiFetch(`${SAIS}/visao-geral/resumo`),
      apiFetch(`${SAIS}/visao-geral/alertas?limit=5&apenas_nao_lidos=true`),
      apiFetch(`${SAIS}/produtividade/ranking`),
    ])
    renderDashboardKPIs(resumo)
    renderDashboardAlertas(alertas.alertas||[])
    renderDashboardCats(resumo.resumo)
    renderTopTecs(top.ranking||[])
    renderMeta(resumo)
    // Atualizar badge
    if(resumo.alertas_pendentes>0){
      const b=document.getElementById('badge-alertas')
      b.textContent=resumo.alertas_pendentes
      b.style.display='inline'
    }
  }catch(e){console.error('dashboard:',e)}
}"""

NEW_LOAD_DASH = """// ═══════════════════════════════════════════════════
// SISTEMA DE FILTROS DO DASHBOARD
// ═══════════════════════════════════════════════════
let _filtros = { tecnico:[], categoria:[], bairro:[], concentrador:[], assunto:[] }
let _filtroOpcoes = null
let _filtroAtual = null

async function carregarFiltroOpcoes(){
  if(_filtroOpcoes) return _filtroOpcoes
  try{
    _filtroOpcoes = await apiFetch(`${SAIS}/visao-geral/filtros-opcoes`)
  }catch(e){ _filtroOpcoes = {tecnicos:[],categorias:[],bairros:[],concentradores:[],assuntos:[]} }
  return _filtroOpcoes
}

async function abrirFiltroModal(tipo){
  _filtroAtual = tipo
  const opts = await carregarFiltroOpcoes()
  const titulos = {
    tecnico:'👷 Técnicos', categoria:'📂 Categoria',
    bairro:'📍 Bairros', concentrador:'🔌 Concentradores', assunto:'📋 Serviços'
  }
  document.getElementById('filtro-modal-title').textContent = titulos[tipo] || tipo
  document.getElementById('filtro-search').value = ''

  const listas = {
    tecnico:      opts.tecnicos?.map(t=>({id:t.id,  nome:t.nome})) || [],
    categoria:    opts.categorias?.map(c=>({id:c.id, nome:c.nome})) || [],
    bairro:       (opts.bairros||[]).map(b=>({id:b,  nome:b})),
    concentrador: (opts.concentradores||[]).map(c=>({id:c, nome:c})),
    assunto:      opts.assuntos?.map(a=>({id:a.id,  nome:a.nome})) || [],
  }
  window._filtroListaAtual = listas[tipo] || []
  renderFiltroLista(window._filtroListaAtual)
  document.getElementById('filtro-modal-overlay').classList.add('active')
}

function renderFiltroLista(lista){
  const sel = _filtros[_filtroAtual] || []
  document.getElementById('filtro-lista').innerHTML = lista.map(item=>`
    <label class="filtro-item">
      <input type="checkbox" value="${item.id}" ${sel.includes(String(item.id))||sel.includes(item.id)?'checked':''}>
      ${esc(item.nome)}
    </label>
  `).join('')
}

function filtroSearchUpdate(){
  const termo = document.getElementById('filtro-search').value.toLowerCase()
  const filtrado = (window._filtroListaAtual||[]).filter(i=>i.nome.toLowerCase().includes(termo))
  renderFiltroLista(filtrado)
}

function filtroClear(){
  _filtros[_filtroAtual] = []
  renderFiltroLista(window._filtroListaAtual||[])
}

function filtroAplicar(){
  const checks = document.querySelectorAll('#filtro-lista input:checked')
  _filtros[_filtroAtual] = [...checks].map(c=>c.value)
  document.getElementById('filtro-modal-overlay').classList.remove('active')
  atualizarBotoesFiltro()
  loadDashboard()
}

function filtroLimparTodos(){
  _filtros = { tecnico:[], categoria:[], bairro:[], concentrador:[], assunto:[] }
  document.getElementById('f-data-inicio').value = ''
  document.getElementById('f-data-fim').value = ''
  atualizarBotoesFiltro()
  loadDashboard()
}

function dashFiltroChange(){ loadDashboard() }

function atualizarBotoesFiltro(){
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
}

function getFiltrosParams(){
  const p = new URLSearchParams()
  const di = document.getElementById('f-data-inicio')?.value
  const df = document.getElementById('f-data-fim')?.value
  if(di) p.set('data_inicio', di)
  if(df) p.set('data_fim', df)
  if(_filtros.tecnico.length)      p.set('tecnico_id',  _filtros.tecnico.join(','))
  if(_filtros.categoria.length)    p.set('categoria',   _filtros.categoria.join(','))
  if(_filtros.bairro.length)       p.set('bairro',      _filtros.bairro.join(','))
  if(_filtros.assunto.length)      p.set('assunto_id',  _filtros.assunto.join(','))
  return p.toString()
}

function temFiltrosAtivos(){
  const di = document.getElementById('f-data-inicio')?.value
  const df = document.getElementById('f-data-fim')?.value
  return di || df || Object.values(_filtros).some(v=>v.length>0)
}

// Fechar modal de filtro clicando fora
document.addEventListener('click', e=>{
  const overlay = document.getElementById('filtro-modal-overlay')
  if(overlay && e.target === overlay) overlay.classList.remove('active')
})

// ═══════════════════════════════════════════════════
// DASHBOARD (com filtros)
// ═══════════════════════════════════════════════════
async function loadDashboard(){
  try{
    const usarFiltros = temFiltrosAtivos()
    const params = getFiltrosParams()

    let resumo, top
    if(usarFiltros){
      // Usa endpoint filtrado
      const [rf, alertas] = await Promise.all([
        apiFetch(`${SAIS}/visao-geral/resumo-filtrado?${params}`),
        apiFetch(`${SAIS}/visao-geral/alertas?limit=5&apenas_nao_lidos=true`),
      ])
      resumo = rf
      // Adapta estrutura para ser compatível com renderDashboardKPIs
      resumo.alertas_pendentes = 0
      resumo.auditorias_criticas = 0
      top = { ranking: rf.ranking || [] }
      renderDashboardAlertas(alertas.alertas||[])
    } else {
      const [r, alertas, t] = await Promise.all([
        apiFetch(`${SAIS}/visao-geral/resumo`),
        apiFetch(`${SAIS}/visao-geral/alertas?limit=5&apenas_nao_lidos=true`),
        apiFetch(`${SAIS}/pontuacao/ranking`),
      ])
      resumo = r
      top = t
      renderDashboardAlertas(alertas.alertas||[])
      if(resumo.alertas_pendentes>0){
        const b=document.getElementById('badge-alertas')
        b.textContent=resumo.alertas_pendentes
        b.style.display='inline'
      }
    }

    renderDashboardKPIs(resumo)
    renderDashboardCats(resumo.resumo)
    renderTopTecs(top.ranking||[])
    renderMeta(resumo)
  }catch(e){console.error('dashboard:',e)}
}"""

if OLD_LOAD_DASH in html:
    html = html.replace(OLD_LOAD_DASH, NEW_LOAD_DASH, 1)
    print("✅ PARTE 2d: loadDashboard atualizado com filtros")
else:
    print("❌ PARTE 2d: loadDashboard não encontrado")

# ─── Salva ───────────────────────────────────────────────────────────────────
with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("✅ index.html salvo!")
print("✅ Rode: systemctl restart hubprod_cliquedf")
