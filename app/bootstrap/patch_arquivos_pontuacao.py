"""
SAIS — Patch completo do sistema de pontuação nos arquivos do servidor.
Executar:
    cd /opt/automacoes/cliquedf/operacional
    source venv/bin/activate
    python3 /tmp/patch_arquivos_pontuacao.py
"""
import re

BASE = "/opt/automacoes/cliquedf/operacional"

# ═══════════════════════════════════════════════════
# PATCH 1 — visao_geral.py: adicionar pontos no /resumo
# ═══════════════════════════════════════════════════
def patch_visao_geral():
    path = f"{BASE}/app/routes/sais/visao_geral.py"
    with open(path) as f:
        content = f.read()

    old = '''    meta_row = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia'"
    ).fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150

    fins  = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    efic  = round(fins / total * 100, 1) if total > 0 else 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0'''

    new = '''    meta_row = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia'"
    ).fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150

    fins  = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    efic  = round(fins / total * 100, 1) if total > 0 else 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    # Pontuação ponderada por assunto
    pontos_row = db.execute("""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS pontos
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p
            ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo = 1
        WHERE o.status = 'finalizada'
          AND DATE(o.data_fechamento, \'+3 hours\') = ?
    """, (data,)).fetchone()

    meta_pts_row = db.execute(
        "SELECT valor FROM sais_config WHERE chave=\'meta_dia_pontos\'"
    ).fetchone()
    meta_pontos  = int(meta_pts_row["valor"]) if meta_pts_row else 80
    total_pontos = pontos_row["pontos"] or 0 if pontos_row else 0
    pct_meta_pontos = round(total_pontos / meta_pontos * 100, 1) if meta_pontos > 0 else 0'''

    if old not in content:
        print("  AVISO: trecho não encontrado em visao_geral.py — verifique manualmente")
        return False

    content = content.replace(old, new)

    # Adicionar campos no return
    old_return = '''        "alertas_pendentes": alertas["total"] if alertas else 0,
        "auditorias_criticas": audits["total"] if audits else 0,
    }'''
    new_return = '''        "alertas_pendentes":   alertas["total"] if alertas else 0,
        "auditorias_criticas": audits["total"] if audits else 0,
        "total_pontos":        total_pontos,
        "meta_dia_pontos":     meta_pontos,
        "pct_meta_pontos":     pct_meta_pontos,
    }'''
    content = content.replace(old_return, new_return)

    with open(path, "w") as f:
        f.write(content)
    print("  ✅ visao_geral.py atualizado")
    return True


# ═══════════════════════════════════════════════════
# PATCH 2 — produtividade.py: ranking por pontos
# ═══════════════════════════════════════════════════
def patch_produtividade():
    path = f"{BASE}/app/routes/sais/produtividade.py"
    with open(path) as f:
        content = f.read()

    # Atualizar import do score_engine
    old_import = "from app.engines.score_engine import ranking_dia"
    new_import = "from app.engines.score_engine import ranking_dia, calcular_pontos_tecnico"

    if old_import in content:
        content = content.replace(old_import, new_import)

    # Adicionar total_pontos no retorno de /tecnico/{id}
    old_ret = '''    return {
        "tecnico": dict(tecnico),
        "score_hoje": score_hoje,
        "historico_30d": historico,
        "por_categoria": [dict(r) for r in cats],
        "tempo_medio": [dict(r) for r in tempo_medio],
    }'''
    new_ret = '''    # Pontuação do dia
    pontos_hoje = calcular_pontos_tecnico(tecnico_id, data)

    return {
        "tecnico": dict(tecnico),
        "score_hoje": score_hoje,
        "pontos_hoje": pontos_hoje,
        "historico_30d": historico,
        "por_categoria": [dict(r) for r in cats],
        "tempo_medio": [dict(r) for r in tempo_medio],
    }'''
    content = content.replace(old_ret, new_ret)

    with open(path, "w") as f:
        f.write(content)
    print("  ✅ produtividade.py atualizado")
    return True


# ═══════════════════════════════════════════════════
# PATCH 3 — index.html: KPIs com pontos
# ═══════════════════════════════════════════════════
def patch_index_html():
    path = f"{BASE}/static/index.html"
    with open(path) as f:
        content = f.read()

    # Substituir renderDashboardKPIs para mostrar pontos
    old_kpis = '''    document.getElementById('db-kpis').innerHTML=`
    <div class="kpi" style="--ac:var(--cyan)" onclick="nav('ultimas-os','visao-geral')">
      <div class="kpi-lbl">Total OS</div><div class="kpi-val">${r.total||0}</div>
      <div class="kpi-sub">hoje</div>
    </div>
    <div class="kpi" style="--ac:var(--green)" onclick="abrirModalListaOS('finalizada')">
      <div class="kpi-lbl">Finalizadas</div><div class="kpi-val">${r.finalizadas||0}</div>
      <div class="kpi-sub">${pct}% da meta</div>
      <div class="kpi-badge ${pct>=80?'up':'dn'}">${pct}%</div>
    </div>
    <div class="kpi" style="--ac:var(--amber)" onclick="nav('os-atrasadas','central')">
      <div class="kpi-lbl">Em Campo</div><div class="kpi-val">${r.em_campo||0}</div>
      <div class="kpi-sub">em execução</div>
    </div>
    <div class="kpi" style="--ac:var(--cyan)" onclick="nav('agenda-dia','agenda')">
      <div class="kpi-lbl">Agendadas</div><div class="kpi-val">${r.agendadas||0}</div>
      <div class="kpi-sub">hoje</div>
    </div>
    <div class="kpi" style="--ac:var(--orange)" onclick="nav('ranking','produtividade')">
      <div class="kpi-lbl">Técnicos</div><div class="kpi-val">${d.tecnicos_ativos||0}</div>
      <div class="kpi-sub">ativos hoje</div>
    </div>
  `'''

    new_kpis = '''    const pts     = d.total_pontos || 0
    const metaPts = d.meta_dia_pontos || 80
    const pctPts  = d.pct_meta_pontos || 0
    document.getElementById('db-kpis').innerHTML=`
    <div class="kpi" style="--ac:var(--cyan)" onclick="nav('ultimas-os','visao-geral')">
      <div class="kpi-lbl">Total OS</div><div class="kpi-val">${r.total||0}</div>
      <div class="kpi-sub">hoje</div>
    </div>
    <div class="kpi" style="--ac:var(--green)" onclick="nav('ranking','produtividade')">
      <div class="kpi-lbl">Pontos Hoje</div><div class="kpi-val">${pts}</div>
      <div class="kpi-sub">meta: ${metaPts} pts</div>
      <div class="kpi-badge ${pctPts>=80?'up':'dn'}">${pctPts}%</div>
    </div>
    <div class="kpi" style="--ac:var(--green)" onclick="nav('ultimas-os','visao-geral')" style="opacity:.85">
      <div class="kpi-lbl">OS Finalizadas</div><div class="kpi-val">${r.finalizadas||0}</div>
      <div class="kpi-sub">${pct}% da meta OS</div>
    </div>
    <div class="kpi" style="--ac:var(--amber)" onclick="nav('os-atrasadas','central')">
      <div class="kpi-lbl">Em Campo</div><div class="kpi-val">${r.em_campo||0}</div>
      <div class="kpi-sub">em execução</div>
    </div>
    <div class="kpi" style="--ac:var(--cyan)" onclick="nav('agenda-dia','agenda')">
      <div class="kpi-lbl">Agendadas</div><div class="kpi-val">${r.agendadas||0}</div>
      <div class="kpi-sub">hoje</div>
    </div>
  `'''

    if old_kpis not in content:
        print("  AVISO: trecho KPIs não encontrado em index.html — verificar manualmente")
        return False

    content = content.replace(old_kpis, new_kpis)

    # Atualizar renderMeta para usar pontos
    old_meta = '''  const fins=d.resumo?.finalizadas||0
  const meta=d.meta_dia||150
  const pct=Math.min(d.meta_percentual||0,100)
  const cor=pct>=80?'var(--green)':pct>=50?'var(--amber)':'var(--red)'
  document.getElementById('db-meta').innerHTML=`
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px;text-align:center">
      <div><div style="font-size:18px;font-weight:800;font-family:'DM Mono',monospace;color:var(--cyan)">${fins}</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em">Finalizadas</div></div>
      <div><div style="font-size:18px;font-weight:800;font-family:'DM Mono',monospace;color:var(--amber)">${Math.max(0,meta-fins)}</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em">Restantes</div></div>
      <div><div style="font-size:18px;font-weight:800;font-family:'DM Mono',monospace;color:${cor}">${pct}%</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em">Atingimento</div></div>
      <div><div style="font-size:18px;font-weight:800;font-family:'DM Mono',monospace;color:var(--orange)">${meta}</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em">Meta</div></div>
    </div>
    <div class="meta-track"><div class="meta-fill" style="width:${pct}%;background:linear-gradient(90deg,var(--cyan),var(--green))"></div></div>
  `'''

    new_meta = '''  const fins    = d.resumo?.finalizadas || 0
  const meta    = d.meta_dia || 150
  const pct     = Math.min(d.meta_percentual || 0, 100)
  const pts2    = d.total_pontos || 0
  const metaPts2= d.meta_dia_pontos || 80
  const pctPts2 = Math.min(d.pct_meta_pontos || 0, 100)
  const corPts  = pctPts2>=80?'var(--green)':pctPts2>=50?'var(--amber)':'var(--red)'
  const corOS   = pct>=80?'var(--green)':pct>=50?'var(--amber)':'var(--red)'
  document.getElementById('db-meta').innerHTML=`
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">
      <!-- PONTOS (principal) -->
      <div style="background:var(--s2);border-radius:10px;padding:14px;border:1px solid var(--border)">
        <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px">🏆 Meta de Pontos</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px;text-align:center">
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:var(--cyan)">${pts2}</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Pontos</div></div>
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:var(--amber)">${Math.max(0,metaPts2-pts2)}</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Faltam</div></div>
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:${corPts}">${pctPts2}%</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Meta ${metaPts2}</div></div>
        </div>
        <div class="meta-track"><div class="meta-fill" style="width:${pctPts2}%;background:linear-gradient(90deg,var(--cyan),var(--green))"></div></div>
      </div>
      <!-- OS (secundário) -->
      <div style="background:var(--s2);border-radius:10px;padding:14px;border:1px solid var(--border);opacity:.8">
        <div style="font-size:9px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px">📋 Quantidade de OS</div>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px;text-align:center">
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:var(--green)">${fins}</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Fin.</div></div>
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:var(--amber)">${Math.max(0,meta-fins)}</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Faltam</div></div>
          <div><div style="font-size:20px;font-weight:800;font-family:'DM Mono',monospace;color:${corOS}">${pct}%</div><div style="font-size:8px;color:var(--muted);text-transform:uppercase">Meta ${meta}</div></div>
        </div>
        <div class="meta-track"><div class="meta-fill" style="width:${pct}%;background:var(--muted)"></div></div>
      </div>
    </div>
  `'''

    content = content.replace(old_meta, new_meta)

    # Atualizar renderTopTecs para mostrar pontos ao lado das OS
    old_tec_row = '''      <div style="font-family:'DM Mono',monospace;font-size:11px;color:var(--cyan);font-weight:700;margin-right:8px">${t.total_os} OS</div>
          <span class="badge ${cls}">${pct}%</span>'''
    new_tec_row = '''      <div style="font-family:'DM Mono',monospace;font-size:10px;color:var(--cyan);font-weight:700;margin-right:6px">${t.total_os} OS</div>
          <div style="font-family:'DM Mono',monospace;font-size:10px;color:var(--amber);font-weight:700;margin-right:6px">${t.total_pontos||0}pts</div>
          <span class="badge ${cls}">${pct}%</span>'''
    content = content.replace(old_tec_row, new_tec_row)

    with open(path, "w") as f:
        f.write(content)
    print("  ✅ index.html atualizado")
    return True


# ═══════════════════════════════════════════════════
# PATCH 4 — score_engine.py: substituir pelo v2
# ═══════════════════════════════════════════════════
def patch_score_engine():
    import shutil
    src = "/tmp/score_engine_v2.py"
    dst = f"{BASE}/app/engines/score_engine.py"
    try:
        shutil.copy(src, dst)
        print("  ✅ score_engine.py atualizado")
        return True
    except Exception as e:
        print(f"  ERRO: {e}")
        return False


# ═══════════════════════════════════════════════════
# PATCH 5 — event_engine.py: meta em pontos
# ═══════════════════════════════════════════════════
def patch_event_engine():
    path = f"{BASE}/app/engines/event_engine.py"
    with open(path) as f:
        content = f.read()

    # Substituir detectar_meta para usar pontos
    old = '''    hoje = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")
    fins = db.execute("""
        SELECT COUNT(*) as total FROM prod_os_cache
        WHERE status='finalizada'
          AND DATE(data_fechamento, '+3 hours') = ?
    """, (hoje,)).fetchone()

    total_fins = fins["total"] if fins else 0
    pct = round(total_fins / meta * 100) if meta > 0 else 0'''

    new = '''    hoje = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")

    # Usar pontos para verificar meta
    pts_row = db.execute("""
        SELECT SUM(COALESCE(p.pontuacao, 0)) as total
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
        WHERE o.status='finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = ?
    """, (hoje,)).fetchone()

    total_fins = pts_row["total"] or 0 if pts_row else 0
    pct = round(total_fins / meta * 100) if meta > 0 else 0'''

    old_config = '''    config = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia'"
    ).fetchone()
    meta = int(config["valor"]) if config else 150'''

    new_config = '''    config = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia_pontos'"
    ).fetchone()
    meta = int(config["valor"]) if config else 80'''

    content = content.replace(old_config, new_config)
    content = content.replace(old, new)

    # Atualizar mensagem do alerta
    old_msg = '''            "subtitulo": f"{total_fins}/{meta} OS finalizadas",'''
    new_msg = '''            "subtitulo": f"{total_fins}/{meta} pts",'''
    content = content.replace(old_msg, new_msg)

    with open(path, "w") as f:
        f.write(content)
    print("  ✅ event_engine.py atualizado")
    return True


# ═══════════════════════════════════════════════════
# EXECUÇÃO
# ═══════════════════════════════════════════════════
if __name__ == "__main__":
    print("="*50)
    print("SAIS — Patch sistema de pontuação")
    print("="*50)

    results = []
    results.append(("visao_geral.py", patch_visao_geral()))
    results.append(("produtividade.py", patch_produtividade()))
    results.append(("score_engine.py", patch_score_engine()))
    results.append(("event_engine.py", patch_event_engine()))
    results.append(("index.html", patch_index_html()))

    print("\nResumo:")
    for nome, ok in results:
        print(f"  {'✅' if ok else '❌'} {nome}")
