from fastapi import APIRouter
import sqlite3

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@router.get("/")
async def get_metas():
    db = get_db()
    rows = db.execute("""
        SELECT m.*, t.nome AS tecnico_nome
        FROM prod_metas m
        LEFT JOIN prod_tecnicos t ON t.id = m.tecnico_id
        WHERE m.vigente = 1
        ORDER BY m.tecnico_id, m.tipo
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]
