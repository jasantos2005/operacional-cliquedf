from fastapi import APIRouter, Query
import sqlite3

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@router.get("/")
async def get_ranking(periodo: str = Query("hoje", enum=["hoje","semana","mes"])):
    db = get_db()

    if periodo == "hoje":
        filtro = "date(o.data_abertura) = date(datetime('now','-3 hours'))"
    elif periodo == "semana":
        filtro = "date(o.data_abertura) >= date(datetime('now','-3 hours'), '-7 days')"
    else:
        filtro = "strftime('%Y-%m', o.data_abertura) = strftime('%Y-%m', datetime('now','-3 hours'))"

    rows = db.execute(f"""
        SELECT
            t.id, t.nome, t.meta_dia,
            COUNT(o.id)                    AS total,
            SUM(o.status='finalizada')     AS finalizadas,
            SUM(o.categoria='servico')     AS servicos,
            SUM(o.categoria='suporte')     AS suportes,
            SUM(o.categoria='infra')       AS infra,
            SUM(o.categoria='retirada')    AS retiradas
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id = t.id AND {filtro}
        WHERE t.ativo = 1
        GROUP BY t.id
        ORDER BY finalizadas DESC
    """).fetchall()

    db.close()

    result = []
    for i, r in enumerate(rows):
        d = dict(r)
        total = d["total"] or 0
        fins  = d["finalizadas"] or 0
        d["posicao"]    = i + 1
        d["score"]      = (d["servicos"] or 0)*3 + (d["suportes"] or 0)*2 + (d["infra"] or 0)*2
        d["eficiencia"] = round(fins/total*100, 1) if total > 0 else 0.0
        result.append(d)

    return {"periodo": periodo, "ranking": result}
