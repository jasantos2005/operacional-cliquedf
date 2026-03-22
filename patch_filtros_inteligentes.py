#!/usr/bin/env python3
"""
Patch SAIS — Filtros Inteligentes
1. filtros-opcoes: itens com movimentação no período vêm primeiro
2. Cidade: nome real da tabela `cidade` do IXC
3. Técnicos: ordenados por movimentação
4. Bairros/Serviços: ordenados por movimentação
5. JS: data inicial = hoje ao abrir o sistema
6. JS: modal de filtro mostra badge "com mov." nos itens ativos
"""

VG_PATH = "/opt/automacoes/cliquedf/operacional/app/routes/sais/visao_geral.py"
INDEX   = "/opt/automacoes/cliquedf/operacional/static/index.html"

import shutil, time
ts = int(time.time())
shutil.copy2(VG_PATH, f"{VG_PATH}.bak.{ts}")
shutil.copy2(INDEX,   f"{INDEX}.bak.{ts}")
print(f"💾 Backups criados (.bak.{ts})")

# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 1 — Reescreve filtros-opcoes com movimentação + nome cidade
# ═══════════════════════════════════════════════════════════════════════════════

with open(VG_PATH, "r", encoding="utf-8") as f:
    vg = f.read()

OLD_ENDPOINT = '''@router.get("/filtros-opcoes")
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
    cidades = []
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
                  AND c.bairro IS NOT NULL AND c.bairro != \'\'
                ORDER BY c.bairro
                LIMIT 80
            """)
            bairros = [r["bairro"] for r in cur.fetchall() if r["bairro"]]
            cur.execute("SELECT DISTINCT c.cidade FROM su_oss_chamado o LEFT JOIN cliente c ON c.id = o.id_cliente WHERE o.data_abertura >= DATE_SUB(NOW(), INTERVAL 30 DAY) AND c.cidade IS NOT NULL AND c.cidade != \'\' ORDER BY c.cidade LIMIT 50")
            cidades = [r["cidade"] for r in cur.fetchall() if r["cidade"]]
            # Concentradores (assuntos que contém CTO/OLT/splitter)
            cur.execute("""
                SELECT DISTINCT descricao as nome
                FROM su_oss_assunto
                WHERE descricao LIKE \'%CTO%\' OR descricao LIKE \'%OLT%\'
                   OR descricao LIKE \'%SPLITTER%\' OR descricao LIKE \'%FIBRA%\'
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
        "cidades":       cidades,
        "concentradores": concentradores,
    }'''

NEW_ENDPOINT = '''@router.get("/filtros-opcoes")
async def get_filtros_opcoes(
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
):
    """Retorna opções para os filtros ordenadas por movimentação no período."""
    import os as _os, pymysql
    db = get_db()
    hoje = hoje_brt()
    di = str(data_inicio) if data_inicio else hoje
    df = str(data_fim)    if data_fim    else hoje

    # Técnicos: ordenados por movimentação no período
    tecs_mov = db.execute("""
        SELECT t.ixc_funcionario_id AS id, t.nome,
               COUNT(o.id) AS total
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id = t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours') BETWEEN ? AND ?
        WHERE t.ativo = 1
        GROUP BY t.id
        ORDER BY total DESC, t.nome
    """, (di, df)).fetchall()

    # Assuntos: ordenados por movimentação no período
    assuntos_mov = db.execute("""
        SELECT a.id, a.assunto, COUNT(o.id) AS total
        FROM prod_assuntos a
        LEFT JOIN prod_os_cache o ON o.ixc_assunto_id = a.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours') BETWEEN ? AND ?
        GROUP BY a.id
        ORDER BY total DESC, a.assunto
        LIMIT 100
    """, (di, df)).fetchall()

    # Categorias com contagem
    cats_mov = db.execute("""
        SELECT categoria, COUNT(*) AS total
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento,data_agenda,data_abertura),'+3 hours') BETWEEN ? AND ?
          AND categoria IS NOT NULL
        GROUP BY categoria
        ORDER BY total DESC
    """, (di, df)).fetchall()
    cats_com_mov = {r["categoria"]: r["total"] for r in cats_mov}

    db.close()

    categorias = []
    for c in [("servico","Serviço"),("suporte","Suporte"),("infra","Infra"),("retirada","Retirada")]:
        categorias.append({"id": c[0], "nome": c[1], "total": cats_com_mov.get(c[0], 0)})
    categorias.sort(key=lambda x: -x["total"])

    # IXC: bairros e cidades com movimentação + nome real da cidade
    bairros = []
    cidades = []
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
            # Bairros com movimentação no período (ordenados por quantidade)
            cur.execute("""
                SELECT c.bairro, COUNT(o.id) AS total
                FROM su_oss_chamado o
                LEFT JOIN cliente c ON c.id = o.id_cliente
                WHERE DATE(CONVERT_TZ(o.data_abertura,'+00:00','-03:00')) BETWEEN %s AND %s
                  AND c.bairro IS NOT NULL AND c.bairro != ''
                GROUP BY c.bairro
                ORDER BY total DESC, c.bairro
                LIMIT 80
            """, (di, df))
            bairros = [{"nome": r["bairro"], "total": r["total"]}
                       for r in cur.fetchall() if r["bairro"]]

            # Cidades com movimentação — nome real da tabela cidade do IXC
            cur.execute("""
                SELECT cd.nome AS cidade_nome, COUNT(o.id) AS total
                FROM su_oss_chamado o
                LEFT JOIN cliente c  ON c.id  = o.id_cliente
                LEFT JOIN cidade  cd ON cd.id = c.id_cidade
                WHERE DATE(CONVERT_TZ(o.data_abertura,'+00:00','-03:00')) BETWEEN %s AND %s
                  AND cd.nome IS NOT NULL AND cd.nome != ''
                GROUP BY cd.nome
                ORDER BY total DESC, cd.nome
                LIMIT 60
            """, (di, df))
            cidades = [{"nome": r["cidade_nome"], "total": r["total"]}
                       for r in cur.fetchall() if r["cidade_nome"]]

            # Concentradores
            cur.execute("""
                SELECT DISTINCT descricao AS nome
                FROM su_oss_assunto
                WHERE descricao LIKE '%CTO%' OR descricao LIKE '%OLT%'
                   OR descricao LIKE '%SPLITTER%' OR descricao LIKE '%FIBRA%'
                ORDER BY descricao LIMIT 50
            """)
            concentradores = [r["nome"] for r in cur.fetchall() if r["nome"]]
        ixc.close()
    except Exception as e:
        pass

    return {
        "periodo":    {"data_inicio": di, "data_fim": df},
        "tecnicos":   [{"id": t["id"], "nome": t["nome"], "total": t["total"]} for t in tecs_mov],
        "categorias": categorias,
        "assuntos":   [{"id": a["id"], "nome": a["assunto"], "total": a["total"]} for a in assuntos_mov],
        "bairros":    bairros,
        "cidades":    cidades,
        "concentradores": [{"nome": c} for c in concentradores],
    }'''

if OLD_ENDPOINT in vg:
    vg = vg.replace(OLD_ENDPOINT, NEW_ENDPOINT, 1)
    with open(VG_PATH, "w", encoding="utf-8") as f:
        f.write(vg)
    print("✅ PARTE 1: filtros-opcoes reescrito")
else:
    print("❌ PARTE 1: endpoint não encontrado")


# ═══════════════════════════════════════════════════════════════════════════════
# PARTE 2 — index.html: data inicial + filtros inteligentes no modal
# ═══════════════════════════════════════════════════════════════════════════════

with open(INDEX, "r", encoding="utf-8") as f:
    html = f.read()

# ── 1. Data inicial = hoje ao carregar o sistema ──────────────────────────────
OLD_DATECHIP = "// date-chip removido da topbar"
NEW_DATECHIP = """// date-chip removido da topbar
// Define data inicial = hoje
;(function(){
  const hoje = new Date().toISOString().split('T')[0]
  const di = document.getElementById('f-data-inicio')
  const df = document.getElementById('f-data-fim')
  if(di && !di.value) di.value = hoje
  if(df && !df.value) df.value = hoje
})()"""

if OLD_DATECHIP in html:
    html = html.replace(OLD_DATECHIP, NEW_DATECHIP, 1)
    print("✅ PARTE 2a: data inicial = hoje")
else:
    print("⚠️  PARTE 2a: anchor não encontrado")

# ── 2. carregarFiltroOpcoes: passa período atual ──────────────────────────────
OLD_CACHE = """async function carregarFiltroOpcoes(){
  if(_filtroOpcoes) return _filtroOpcoes
  try{
    _filtroOpcoes = await apiFetch(`${SAIS}/visao-geral/filtros-opcoes`)
  }catch(e){ _filtroOpcoes = {tecnicos:[],categorias:[],bairros:[],concentradores:[],assuntos:[]} }
  return _filtroOpcoes
}"""

NEW_CACHE = """async function carregarFiltroOpcoes(forcar=false){
  const di = document.getElementById('f-data-inicio')?.value || ''
  const df = document.getElementById('f-data-fim')?.value || ''
  const chave = di+'_'+df
  if(!forcar && _filtroOpcoes && _filtroOpcoes._chave === chave) return _filtroOpcoes
  try{
    const params = []
    if(di) params.push('data_inicio='+di)
    if(df) params.push('data_fim='+df)
    const url = `${SAIS}/visao-geral/filtros-opcoes` + (params.length ? '?'+params.join('&') : '')
    _filtroOpcoes = await apiFetch(url)
    _filtroOpcoes._chave = chave
  }catch(e){ _filtroOpcoes = {tecnicos:[],categorias:[],bairros:[],concentradores:[],assuntos:[],_chave:chave} }
  return _filtroOpcoes
}"""

if OLD_CACHE in html:
    html = html.replace(OLD_CACHE, NEW_CACHE, 1)
    print("✅ PARTE 2b: carregarFiltroOpcoes com período")
else:
    print("❌ PARTE 2b: não encontrado")

# ── 3. abrirFiltroModal: renderiza com total de movimentação ──────────────────
OLD_LISTAS = """  const listas = {
    tecnico:      opts.tecnicos?.map(t=>({id:t.id,  nome:t.nome})) || [],
    categoria:    opts.categorias?.map(c=>({id:c.id, nome:c.nome})) || [],
    cidade:       (opts.cidades||[]).map(c=>({id:c,  nome:c})),
    bairro:       (opts.bairros||[]).map(b=>({id:b,  nome:b})),
    concentrador: (opts.concentradores||[]).map(c=>({id:c, nome:c})),
    assunto:      opts.assuntos?.map(a=>({id:a.id,  nome:a.nome})) || [],
  }"""

NEW_LISTAS = """  const listas = {
    tecnico:      (opts.tecnicos||[]).map(t=>({id:t.id, nome:t.nome, total:t.total||0})),
    categoria:    (opts.categorias||[]).map(c=>({id:c.id, nome:c.nome, total:c.total||0})),
    cidade:       (opts.cidades||[]).map(c=>({id:c.nome, nome:c.nome, total:c.total||0})),
    bairro:       (opts.bairros||[]).map(b=>({id:b.nome, nome:b.nome, total:b.total||0})),
    concentrador: (opts.concentradores||[]).map(c=>({id:c.nome||c, nome:c.nome||c, total:0})),
    assunto:      (opts.assuntos||[]).map(a=>({id:a.id, nome:a.nome, total:a.total||0})),
  }"""

if OLD_LISTAS in html:
    html = html.replace(OLD_LISTAS, NEW_LISTAS, 1)
    print("✅ PARTE 2c: listas com total")
else:
    print("❌ PARTE 2c: não encontrado")

# ── 4. renderFiltroLista: mostra badge de movimentação ───────────────────────
OLD_RENDER = """function renderFiltroLista(lista){
  const sel = _filtros[_filtroAtual] || []
  document.getElementById('filtro-lista').innerHTML = lista.map(item=>`
    <label class="filtro-item">
      <input type="checkbox" value="${item.id}" ${sel.includes(String(item.id))||sel.includes(item.id)?'checked':''}>
      ${esc(item.nome)}
    </label>
  `).join('')
}"""

NEW_RENDER = """function renderFiltroLista(lista){
  const sel = _filtros[_filtroAtual] || []
  document.getElementById('filtro-lista').innerHTML = lista.map(item=>{
    const checked = sel.includes(String(item.id)) || sel.includes(item.id)
    const temMov  = (item.total||0) > 0
    const badge   = temMov
      ? `<span style="margin-left:auto;font-size:9px;background:rgba(0,212,255,.15);color:var(--cyan);border-radius:4px;padding:1px 5px">${item.total}</span>`
      : `<span style="margin-left:auto;font-size:9px;color:var(--border)">—</span>`
    return `<label class="filtro-item" style="opacity:${temMov?1:.5}">
      <input type="checkbox" value="${item.id}" ${checked?'checked':''}>
      <span style="flex:1">${esc(item.nome)}</span>
      ${badge}
    </label>`
  }).join('')
}"""

if OLD_RENDER in html:
    html = html.replace(OLD_RENDER, NEW_RENDER, 1)
    print("✅ PARTE 2d: renderFiltroLista com badges")
else:
    print("❌ PARTE 2d: não encontrado")

# ── 5. globalFiltroChange: recarrega opções quando data muda ─────────────────
OLD_GLOBAL = """function globalFiltroChange(){
  const paginasReativas = ['dashboard','ranking','agenda-dia','auditoria','os-atrasadas','tecnicos-ociosos']
  if(paginasReativas.includes(currentPage)){
    const cfg = PAGES_CFG[currentPage]
    if(cfg?.load) cfg.load()
  }
}"""

NEW_GLOBAL = """function globalFiltroChange(){
  // Invalida cache de opções quando período muda
  if(_filtroOpcoes) _filtroOpcoes._chave = null
  atualizarBotoesFiltro()
  const paginasReativas = ['dashboard','ranking','agenda-dia','auditoria','os-atrasadas','tecnicos-ociosos']
  if(paginasReativas.includes(currentPage)){
    const cfg = PAGES_CFG[currentPage]
    if(cfg?.load) cfg.load()
  }
}"""

if OLD_GLOBAL in html:
    html = html.replace(OLD_GLOBAL, NEW_GLOBAL, 1)
    print("✅ PARTE 2e: globalFiltroChange invalida cache")
else:
    print("⚠️  PARTE 2e: não encontrado")

# ── Salva ─────────────────────────────────────────────────────────────────────
with open(INDEX, "w", encoding="utf-8") as f:
    f.write(html)
print("✅ index.html salvo!")
print("✅ Rode: systemctl restart hubprod_cliquedf")
