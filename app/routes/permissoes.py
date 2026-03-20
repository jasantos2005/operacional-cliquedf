from fastapi import APIRouter
import sqlite3

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@router.get("/")
async def get_permissoes():
    db = get_db()
    rows = db.execute(
        "SELECT modulo, nivel, permitido FROM prod_permissoes ORDER BY modulo, nivel"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]
