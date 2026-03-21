"""
SAIS — Motor de Score de Produtividade
Calcula score e eficiência por técnico.

Fórmula:
    score = (servicos * 3) + (suportes * 2) + (infra * 2) + (retiradas * 1)
            - (retrabalhos * 4) - (auditorias_graves * 3)
    eficiencia = ((total - retrabalhos) / total) * 100
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

PONTOS = {
    "servico":  3,
    "suporte":  2,
    "infra":    2,
    "retirada": 1,
    "outros":   1,
}

PENALIDADES = {
    "retrabalho":       -4,
    "auditoria_critica": -3,
    "auditoria_alta":    -2,
    "sla_estourado":     -2,
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def calcular_score_tecnico(tecnico_id: int, data: str = None) -> dict:
    """
    Calcula score completo de um técnico para uma data.
    """
    db = get_db()
    data = data or datetime.now().strftime("%Y-%m-%d")

    # OS finalizadas do dia
    rows = db.execute("""
        SELECT categoria, COUNT(*) as total
        FROM prod_os_cache
        WHERE tecnico_id = ?
          AND status = 'finalizada'
          AND DATE(COALESCE(data_fechamento, data_abertura)) = ?
        GROUP BY categoria
    """, (tecnico_id, data)).fetchall()

    cats = {r["categoria"]: r["total"] for r in rows}

    # Total e score base
    total_os   = sum(cats.values())
    score_base = sum(cats.get(cat, 0) * pts for cat, pts in PONTOS.items())

    # Penalidades por auditorias
    auditorias = db.execute("""
        SELECT criticidade, COUNT(*) as total
        FROM sais_auditorias
        WHERE tecnico_id = ?
          AND DATE(criado_em) = ?
          AND resolvida = 0
        GROUP BY criticidade
    """, (tecnico_id, data)).fetchall()

    penalidade = 0
    for a in auditorias:
        if a["criticidade"] == "critica":
            penalidade += a["total"] * abs(PENALIDADES["auditoria_critica"])
        elif a["criticidade"] == "alta":
            penalidade += a["total"] * abs(PENALIDADES["auditoria_alta"])

    score_final = max(0, score_base - penalidade)

    # Meta do técnico
    meta_row = db.execute("""
        SELECT meta_dia FROM prod_tecnicos WHERE id = ?
    """, (tecnico_id,)).fetchone()
    meta_dia = meta_row["meta_dia"] if meta_row else 8

    # Eficiência
    retrabalhos = 0  # TODO: detectar via audit_engine
    eficiencia = round(((total_os - retrabalhos) / total_os * 100), 1) if total_os > 0 else 0

    # Percentual da meta
    pct_meta = round(total_os / meta_dia * 100, 1) if meta_dia > 0 else 0

    db.close()

    return {
        "tecnico_id":   tecnico_id,
        "data":         data,
        "total_os":     total_os,
        "por_categoria": cats,
        "score_base":   score_base,
        "penalidade":   penalidade,
        "score_final":  score_final,
        "eficiencia":   eficiencia,
        "meta_dia":     meta_dia,
        "pct_meta":     pct_meta,
        "retrabalhos":  retrabalhos,
    }


def ranking_dia(data: str = None, limit: int = 20) -> list:
    """
    Gera ranking completo de todos os técnicos para uma data.
    """
    db = get_db()
    data = data or datetime.now().strftime("%Y-%m-%d")

    tecnicos = db.execute(
        "SELECT id, nome, meta_dia FROM prod_tecnicos WHERE ativo = 1"
    ).fetchall()
    db.close()

    ranking = []
    for t in tecnicos:
        score = calcular_score_tecnico(t["id"], data)
        ranking.append({
            "posicao":      0,
            "tecnico_id":   t["id"],
            "nome":         t["nome"],
            "total_os":     score["total_os"],
            "score":        score["score_final"],
            "eficiencia":   score["eficiencia"],
            "pct_meta":     score["pct_meta"],
            "meta_dia":     score["meta_dia"],
            "por_categoria": score["por_categoria"],
        })

    ranking.sort(key=lambda x: (x["score"], x["total_os"]), reverse=True)

    for i, r in enumerate(ranking):
        r["posicao"] = i + 1

    return ranking[:limit]


def historico_tecnico(tecnico_id: int, dias: int = 7) -> list:
    """
    Histórico de score dos últimos N dias para um técnico.
    """
    resultado = []
    for i in range(dias - 1, -1, -1):
        data = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        score = calcular_score_tecnico(tecnico_id, data)
        resultado.append({
            "data":     data,
            "total_os": score["total_os"],
            "score":    score["score_final"],
            "eficiencia": score["eficiencia"],
        })
    return resultado
