from fastapi import APIRouter, Query
import sqlite3

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@router.get("/")
async def get_os_tipos(periodo: str = Query("hoje", enum=["hoje","semana","mes"])):
    db = get_db()

    if periodo == "hoje":
        filtro = "date(data_abertura) = date(datetime('now','-3 hours'))"
    elif periodo == "semana":
        filtro = "date(data_abertura) >= date(datetime('now','-3 hours'), '-7 days')"
    else:
        filtro = "strftime('%Y-%m', data_abertura) = strftime('%Y-%m', datetime('now','-3 hours'))"

    cats = db.execute(f"""
        SELECT
            categoria,
            COUNT(*)                   AS total,
            SUM(status='finalizada')   AS finalizadas,
            SUM(status='aberta')       AS abertas
        FROM prod_os_cache
        WHERE {filtro}
        GROUP BY categoria
        ORDER BY total DESC
    """).fetchall()

    tec_tipos = db.execute(f"""
        SELECT
            t.nome,
            SUM(o.categoria='servico')  AS servicos,
            SUM(o.categoria='suporte')  AS suportes,
            SUM(o.categoria='infra')    AS infra,
            SUM(o.categoria='retirada') AS retiradas,
            SUM(o.categoria='outros')   AS outros,
            COUNT(o.id)                 AS total
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id = t.id AND {filtro}
        WHERE t.ativo = 1
        GROUP BY t.id
        ORDER BY total DESC
    """).fetchall()

    db.close()
    return {
        "periodo":   periodo,
        "categorias": [dict(r) for r in cats],
        "por_tecnico": [dict(r) for r in tec_tipos],
    }
