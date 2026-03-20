from fastapi import APIRouter
import sqlite3

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@router.get("/")
async def get_dashboard():
    db = get_db()
    hoje = "date(datetime('now','-3 hours'))"

    resumo = dict(db.execute(f"""
        SELECT
            COUNT(*)                     AS total,
            SUM(status='finalizada')     AS finalizadas,
            SUM(status='aberta')         AS execucao,
            SUM(categoria='servico')     AS servicos,
            SUM(categoria='suporte')     AS suportes,
            SUM(categoria='infra')       AS infra,
            SUM(categoria='retirada')    AS retiradas,
            SUM(categoria='outros')      AS outros
        FROM prod_os_cache
        WHERE date(data_abertura) = {hoje}
    """).fetchone())

    rows = db.execute(f"""
        SELECT
            t.id, t.nome, t.meta_dia,
            COUNT(o.id)                    AS total,
            SUM(o.status='finalizada')     AS finalizadas,
            SUM(o.status='aberta')         AS execucao,
            SUM(o.categoria='servico')     AS servicos,
            SUM(o.categoria='suporte')     AS suportes,
            SUM(o.categoria='infra')       AS infra,
            SUM(o.categoria='retirada')    AS retiradas
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o
            ON o.tecnico_id = t.id
            AND date(o.data_abertura) = {hoje}
        WHERE t.ativo = 1
        GROUP BY t.id
        ORDER BY finalizadas DESC
    """).fetchall()

    meta = db.execute("""
        SELECT valor FROM prod_metas
        WHERE tipo='os_dia' AND tecnico_id IS NULL AND vigente=1
        LIMIT 1
    """).fetchone()

    tecnicos = []
    for r in rows:
        d = dict(r)
        total = d["total"] or 0
        fins  = d["finalizadas"] or 0
        d["score"]      = (d["servicos"] or 0)*3 + (d["suportes"] or 0)*2 + (d["infra"] or 0)*2
        d["eficiencia"] = round(fins/total*100, 1) if total > 0 else 0.0
        tecnicos.append(d)

    db.close()
    return {
        "resumo":   resumo,
        "tecnicos": tecnicos,
        "meta_dia": meta["valor"] if meta else 150,
    }
