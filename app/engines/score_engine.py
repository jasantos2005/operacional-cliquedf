"""
SAIS — Motor de Score v2.0
Usa pontuação ponderada por assunto (prod_assuntos_pontuacao).
Mantém contagem de OS para exibição, mas metas são baseadas em pontos.

Regras:
- Produtividade = soma dos pontos das OS finalizadas
- Meta diária   = 80 pontos (configurável por técnico)
- Meta mensal   = 1780 pontos (configurável por técnico)
- OS sem pontuação mapeada = 0 pontos + log de inconsistência
- OS cancelada = não pontua
"""
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"
log = logging.getLogger("SCORE")

META_DIA_PADRAO = 80
META_MES_PADRAO = 1780


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")


def get_meta_tecnico(db, tecnico_id: int, tipo: str = "pontos_dia") -> int:
    """Retorna meta do técnico (pontos_dia ou pontos_mes)."""
    row = db.execute("""
        SELECT valor FROM prod_metas
        WHERE tecnico_id=? AND tipo=? AND vigente=1
        LIMIT 1
    """, (tecnico_id, tipo)).fetchone()
    if row:
        return int(row["valor"])
    # Fallback para config global
    cfg = db.execute(
        "SELECT valor FROM sais_config WHERE chave=?",
        ('meta_tec_dia' if tipo == 'pontos_dia' else 'meta_tec_mes',)
    ).fetchone()
    return int(cfg["valor"]) if cfg else (META_DIA_PADRAO if "dia" in tipo else META_MES_PADRAO)


def get_pontuacao(db, assunto_id: int) -> int:
    """Retorna pontuação de um assunto. 0 se não mapeado."""
    row = db.execute("""
        SELECT pontuacao FROM prod_assuntos_pontuacao
        WHERE id_assunto_ixc=? AND ativo=1
    """, (assunto_id,)).fetchone()
    return row["pontuacao"] if row else 0


def calcular_pontos_tecnico(tecnico_id: int, data: str = None) -> dict:
    """
    Calcula pontos e stats completos de um técnico para uma data.
    Retorna contagem de OS (para exibição) + pontos (para metas).
    """
    db = get_db()
    data = data or hoje_brt()

    # OS finalizadas com pontuação
    rows = db.execute("""
        SELECT
            o.ixc_os_id,
            o.ixc_assunto_id,
            o.categoria,
            COALESCE(p.pontuacao, 0) AS pontos,
            p.id IS NULL AS sem_mapeamento
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p
            ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo = 1
        WHERE o.tecnico_id = ?
          AND o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = ?
    """, (tecnico_id, data)).fetchall()

    # Totais
    total_os     = len(rows)
    total_pontos = sum(r["pontos"] for r in rows)
    sem_mapa     = sum(1 for r in rows if r["sem_mapeamento"])

    # Por categoria (contagem + pontos)
    cats = {}
    for r in rows:
        cat = r["categoria"] or "outros"
        if cat not in cats:
            cats[cat] = {"os": 0, "pontos": 0}
        cats[cat]["os"] += 1
        cats[cat]["pontos"] += r["pontos"]

    # Metas
    meta_dia = get_meta_tecnico(db, tecnico_id, "pontos_dia")
    meta_mes = get_meta_tecnico(db, tecnico_id, "pontos_mes")

    pct_meta_dia = round(total_pontos / meta_dia * 100, 1) if meta_dia > 0 else 0

    # Pontos no mês
    mes_inicio = data[:7] + "-01"
    pontos_mes_row = db.execute("""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS total
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
        WHERE o.tecnico_id = ?
          AND o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') >= ?
    """, (tecnico_id, mes_inicio)).fetchone()
    pontos_mes = pontos_mes_row["total"] or 0
    pct_meta_mes = round(pontos_mes / meta_mes * 100, 1) if meta_mes > 0 else 0

    # Log de inconsistências
    if sem_mapa > 0:
        db.execute("""
            INSERT OR IGNORE INTO sais_auditorias
                (os_id, tecnico_id, tipo, subtipo, criticidade, descricao)
            SELECT o.ixc_os_id, o.tecnico_id,
                   'pontuacao', 'assunto_sem_mapeamento', 'baixa',
                   'OS finalizada com assunto sem pontuação mapeada: assunto_id=' || o.ixc_assunto_id
            FROM prod_os_cache o
            LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
            WHERE o.tecnico_id = ?
              AND o.status = 'finalizada'
              AND DATE(o.data_fechamento, '+3 hours') = ?
              AND p.id IS NULL
        """, (tecnico_id, data))

    db.commit()
    db.close()

    return {
        "tecnico_id":    tecnico_id,
        "data":          data,
        # Contagem (exibição)
        "total_os":      total_os,
        "por_categoria": cats,
        # Pontos (metas)
        "total_pontos":  total_pontos,
        "pontos_mes":    pontos_mes,
        "sem_mapeamento": sem_mapa,
        # Metas
        "meta_dia":      meta_dia,
        "meta_mes":      meta_mes,
        "pct_meta_dia":  pct_meta_dia,
        "pct_meta_mes":  pct_meta_mes,
        # Compatibilidade legada
        "score_final":   total_pontos,
        "eficiencia":    pct_meta_dia,
        "pct_meta":      pct_meta_dia,
    }


def ranking_dia(data: str = None, limit: int = 20) -> list:
    """
    Ranking de técnicos ordenado por PONTOS (não quantidade de OS).
    Mantém contagem de OS para exibição.
    """
    db = get_db()
    data = data or hoje_brt()

    tecnicos = db.execute(
        "SELECT id, nome FROM prod_tecnicos WHERE ativo=1"
    ).fetchall()
    db.close()

    ranking = []
    for t in tecnicos:
        r = calcular_pontos_tecnico(t["id"], data)
        ranking.append({
            "posicao":       0,
            "tecnico_id":    t["id"],
            "nome":          t["nome"],
            # Contagem (exibição)
            "total_os":      r["total_os"],
            "por_categoria": {cat: v["os"] for cat, v in r["por_categoria"].items()},
            # Pontos (ranking e meta)
            "total_pontos":  r["total_pontos"],
            "meta_dia":      r["meta_dia"],
            "pct_meta_dia":  r["pct_meta_dia"],
            # Legado
            "score":         r["total_pontos"],
            "eficiencia":    r["pct_meta_dia"],
            "pct_meta":      r["pct_meta_dia"],
        })

    # Ordenar por PONTOS
    ranking.sort(key=lambda x: (x["total_pontos"], x["total_os"]), reverse=True)
    for i, r in enumerate(ranking):
        r["posicao"] = i + 1

    return ranking[:limit]


def historico_tecnico(tecnico_id: int, dias: int = 7) -> list:
    """Histórico de pontos dos últimos N dias."""
    resultado = []
    for i in range(dias - 1, -1, -1):
        data = (datetime.now() + timedelta(hours=-3, days=-i)).strftime("%Y-%m-%d")
        r = calcular_pontos_tecnico(tecnico_id, data)
        resultado.append({
            "data":          data,
            "total_os":      r["total_os"],
            "total_pontos":  r["total_pontos"],
            "pct_meta_dia":  r["pct_meta_dia"],
            # Legado
            "score":         r["total_pontos"],
            "eficiencia":    r["pct_meta_dia"],
        })
    return resultado


def resumo_pontos_equipe(data: str = None) -> dict:
    """Resumo de pontos da equipe para o dashboard."""
    db = get_db()
    data = data or hoje_brt()

    row = db.execute("""
        SELECT
            COUNT(*) AS total_os,
            COUNT(CASE WHEN o.status='finalizada' THEN 1 END) AS finalizadas,
            SUM(CASE WHEN o.status='finalizada' THEN COALESCE(p.pontuacao,0) ELSE 0 END) AS total_pontos,
            COUNT(CASE WHEN o.status='finalizada' AND p.id IS NULL THEN 1 END) AS sem_mapeamento
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
    """, (data,)).fetchone()

    meta_cfg = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia_pontos'"
    ).fetchone()
    meta_dia = int(meta_cfg["valor"]) if meta_cfg else META_DIA_PADRAO

    total_pontos = row["total_pontos"] or 0
    pct_meta = round(total_pontos / meta_dia * 100, 1) if meta_dia > 0 else 0

    db.close()

    return {
        "data":           data,
        "total_os":       row["total_os"] or 0,
        "finalizadas":    row["finalizadas"] or 0,
        "total_pontos":   total_pontos,
        "meta_dia":       meta_dia,
        "pct_meta":       pct_meta,
        "sem_mapeamento": row["sem_mapeamento"] or 0,
    }
