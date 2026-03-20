import hashlib, os, sqlite3
from datetime import datetime, timedelta
from jose import jwt

SECRET = os.getenv("SECRET_KEY", "dev_key")
ALGO   = "HS256"

def hash_senha(senha: str) -> str:
    return hashlib.sha256(senha.encode()).hexdigest()

def verificar_senha(senha: str, hash_armazenado: str) -> bool:
    return hash_senha(senha) == hash_armazenado

def criar_token(user_id: int, nivel: int) -> str:
    payload = {
        "sub": str(user_id),
        "nivel": nivel,
        "exp": datetime.utcnow() + timedelta(hours=8)
    }
    return jwt.encode(payload, SECRET, algorithm=ALGO)

def verificar_token(token: str) -> dict:
    return jwt.decode(token, SECRET, algorithms=[ALGO])

def checar_permissao(db: sqlite3.Connection, modulo: str, nivel: int) -> bool:
    row = db.execute(
        "SELECT permitido FROM prod_permissoes WHERE modulo=? AND nivel=?",
        (modulo, nivel)
    ).fetchone()
    return bool(row and row["permitido"])
