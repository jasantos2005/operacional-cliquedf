"""
app/routes/sais/auth.py — SAIS Auth v2.0
"""
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timedelta, timezone
import sqlite3, os, secrets, re
import bcrypt as _bcrypt
from jose import jwt, JWTError

router   = APIRouter()
security = HTTPBearer(auto_error=False)

DB_PATH   = "/opt/automacoes/cliquedf/operacional/prod_local.db"
SECRET    = os.getenv("JWT_SECRET", "sais-cliquedf-secret-2025")
ALGORITHM = "HS256"
TOKEN_EXP = 12
BRT       = timezone(timedelta(hours=-3))

GRUPOS = {
    "supervisao_tecnica": {"label":"Supervisão Técnica","nivel":3,"modulos":["visao_geral","agenda","produtividade","auditoria","central","tv"]},
    "rh":                 {"label":"RH",                "nivel":2,"modulos":["visao_geral","produtividade","pontuacao"]},
    "agendamento":        {"label":"Agendamento",        "nivel":2,"modulos":["visao_geral","agenda","central"]},
    "desenvolvimento":    {"label":"Desenvolvimento",    "nivel":99,"modulos":["*"]},
    "admin":              {"label":"Administrador",      "nivel":4,"modulos":["*"]},
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS sais_usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    login TEXT UNIQUE NOT NULL,
    nome TEXT NOT NULL,
    senha_hash TEXT NOT NULL,
    grupo TEXT NOT NULL DEFAULT 'agendamento',
    nivel INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pendente',
    motivo_req TEXT,
    aprovado_por TEXT,
    aprovado_em TEXT,
    ultimo_acesso TEXT,
    criado_em TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE TABLE IF NOT EXISTS sais_sessions_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario_id INTEGER,
    login TEXT,
    ip TEXT,
    acao TEXT,
    criado_em TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE INDEX IF NOT EXISTS idx_sais_usr_login  ON sais_usuarios(login);
CREATE INDEX IF NOT EXISTS idx_sais_usr_status ON sais_usuarios(status);
"""

def now_brt():
    return datetime.now(BRT).strftime("%Y-%m-%d %H:%M:%S")

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_schema():
    db = get_db()
    db.executescript(SCHEMA)
    db.commit()
    db.close()

def hash_senha(senha: str) -> str:
    return _bcrypt.hashpw(senha.encode("utf-8"), _bcrypt.gensalt(rounds=12)).decode("utf-8")

def verificar_senha(senha: str, hash_: str) -> bool:
    try:
        return _bcrypt.checkpw(senha.encode("utf-8"), hash_.encode("utf-8"))
    except Exception:
        return False

def criar_token(payload: dict) -> str:
    data = payload.copy()
    data["exp"] = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXP)
    return jwt.encode(data, SECRET, algorithm=ALGORITHM)

def verificar_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET, algorithms=[ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado.")

def get_usuario_atual(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Não autenticado.")
    return verificar_token(credentials.credentials)

def requer_nivel(nivel_min: int):
    def dep(user=Depends(get_usuario_atual)):
        if user.get("nivel", 0) < nivel_min:
            raise HTTPException(status_code=403, detail="Acesso negado.")
        return user
    return dep

def validar_login_str(login: str) -> str:
    login = login.strip().lower()
    if not re.match(r'^[a-z0-9._]{3,30}$', login):
        raise HTTPException(400, "Login inválido.")
    return login

def validar_senha_str(senha: str):
    if len(senha) < 8:
        raise HTTPException(400, "Senha deve ter pelo menos 8 caracteres.")

class LoginIn(BaseModel):
    login: str
    senha: str

class RegistroIn(BaseModel):
    nome:   str
    login:  str
    senha:  str
    grupo:  str
    motivo: Optional[str] = ""

class AprovarIn(BaseModel):
    usuario_id: int
    aprovado:   bool
    grupo:      Optional[str] = None
    nivel:      Optional[int] = None

class EditarUsuarioIn(BaseModel):
    usuario_id: int
    nome:       Optional[str] = None
    grupo:      Optional[str] = None
    nivel:      Optional[int] = None
    status:     Optional[str] = None

class AlterarSenhaIn(BaseModel):
    usuario_id: int
    nova_senha: str

@router.post("/login")
async def login(body: LoginIn):
    db = get_db()
    try:
        lc = validar_login_str(body.login)
        row = db.execute("SELECT * FROM sais_usuarios WHERE login=?", (lc,)).fetchone()
        if not row:
            raise HTTPException(401, "Credenciais inválidas.")
        if row["status"] == "pendente":
            raise HTTPException(403, "Conta aguardando aprovação do administrador.")
        if row["status"] == "bloqueado":
            raise HTTPException(403, "Conta bloqueada.")
        if not verificar_senha(body.senha, row["senha_hash"]):
            raise HTTPException(401, "Credenciais inválidas.")
        db.execute("UPDATE sais_usuarios SET ultimo_acesso=? WHERE id=?", (now_brt(), row["id"]))
        db.execute("INSERT INTO sais_sessions_log (usuario_id,login,acao,criado_em) VALUES (?,?,?,?)",
                   (row["id"], row["login"], "login", now_brt()))
        db.commit()
        token = criar_token({"sub":row["login"],"id":row["id"],"nome":row["nome"],"nivel":row["nivel"],"grupo":row["grupo"]})
        g = GRUPOS.get(row["grupo"], {})
        return {"access_token":token,"token_type":"bearer","nome":row["nome"],"nivel":row["nivel"],"grupo":row["grupo"],"grupo_label":g.get("label",row["grupo"]),"modulos":g.get("modulos",[])}
    finally:
        db.close()

@router.post("/registro")
async def registro(body: RegistroIn):
    db = get_db()
    try:
        init_schema()
        lc = validar_login_str(body.login)
        validar_senha_str(body.senha)
        if body.grupo not in GRUPOS:
            raise HTTPException(400, f"Grupo inválido: {body.grupo}")
        if db.execute("SELECT id FROM sais_usuarios WHERE login=?", (lc,)).fetchone():
            raise HTTPException(409, "Login já em uso.")
        db.execute("INSERT INTO sais_usuarios (login,nome,senha_hash,grupo,nivel,status,motivo_req,criado_em) VALUES (?,?,?,?,?,?,?,?)",
                   (lc, body.nome.strip(), hash_senha(body.senha), body.grupo, 1, "pendente", body.motivo or "", now_brt()))
        db.commit()
        return {"ok":True,"mensagem":"Solicitação enviada. Aguarde aprovação."}
    finally:
        db.close()

@router.get("/me")
async def get_me(user=Depends(get_usuario_atual)):
    db = get_db()
    try:
        row = db.execute("SELECT id,login,nome,grupo,nivel,status,ultimo_acesso FROM sais_usuarios WHERE login=?", (user["sub"],)).fetchone()
        if not row:
            raise HTTPException(404, "Usuário não encontrado.")
        g = GRUPOS.get(row["grupo"], {})
        return {**dict(row), "grupo_label":g.get("label",row["grupo"]), "modulos":g.get("modulos",[])}
    finally:
        db.close()

@router.get("/admin/pendentes")
async def listar_pendentes(user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        rows = db.execute("SELECT id,login,nome,grupo,motivo_req,criado_em FROM sais_usuarios WHERE status='pendente' ORDER BY criado_em DESC").fetchall()
        return {"pendentes":[dict(r) for r in rows]}
    finally:
        db.close()

@router.get("/admin/usuarios")
async def listar_usuarios(user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        rows = db.execute("SELECT id,login,nome,grupo,nivel,status,ultimo_acesso,aprovado_por,criado_em FROM sais_usuarios ORDER BY criado_em DESC").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["grupo_label"] = GRUPOS.get(d["grupo"],{}).get("label",d["grupo"])
            result.append(d)
        return {"usuarios":result,"total":len(result)}
    finally:
        db.close()

@router.post("/admin/aprovar")
async def aprovar_usuario(body: AprovarIn, user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM sais_usuarios WHERE id=?", (body.usuario_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Usuário não encontrado.")
        if body.aprovado:
            grupo = body.grupo or row["grupo"]
            nivel = body.nivel or GRUPOS.get(grupo,{}).get("nivel",1)
            db.execute("UPDATE sais_usuarios SET status='ativo',grupo=?,nivel=?,aprovado_por=?,aprovado_em=? WHERE id=?",
                       (grupo, nivel, user["sub"], now_brt(), body.usuario_id))
        else:
            db.execute("UPDATE sais_usuarios SET status='bloqueado' WHERE id=?", (body.usuario_id,))
        db.commit()
        return {"ok":True,"status":"ativo" if body.aprovado else "bloqueado"}
    finally:
        db.close()

@router.post("/admin/editar")
async def editar_usuario(body: EditarUsuarioIn, user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        row = db.execute("SELECT * FROM sais_usuarios WHERE id=?", (body.usuario_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Usuário não encontrado.")
        updates = {}
        if body.nome:   updates["nome"]   = body.nome.strip()
        if body.grupo:
            if body.grupo not in GRUPOS:
                raise HTTPException(400, f"Grupo inválido: {body.grupo}")
            updates["grupo"] = body.grupo
            updates["nivel"] = GRUPOS[body.grupo]["nivel"]
        if body.nivel is not None: updates["nivel"]  = body.nivel
        if body.status:            updates["status"] = body.status
        if updates:
            sets = ", ".join(f"{k}=?" for k in updates)
            db.execute(f"UPDATE sais_usuarios SET {sets} WHERE id=?", (*updates.values(), body.usuario_id))
            db.commit()
        return {"ok":True}
    finally:
        db.close()

@router.post("/admin/alterar-senha")
async def alterar_senha(body: AlterarSenhaIn, user=Depends(requer_nivel(4))):
    validar_senha_str(body.nova_senha)
    db = get_db()
    try:
        if not db.execute("SELECT id FROM sais_usuarios WHERE id=?", (body.usuario_id,)).fetchone():
            raise HTTPException(404, "Usuário não encontrado.")
        db.execute("UPDATE sais_usuarios SET senha_hash=? WHERE id=?", (hash_senha(body.nova_senha), body.usuario_id))
        db.commit()
        return {"ok":True}
    finally:
        db.close()

@router.delete("/admin/usuario/{uid}")
async def deletar_usuario(uid: int, user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        db.execute("UPDATE sais_usuarios SET status='bloqueado' WHERE id=?", (uid,))
        db.commit()
        return {"ok":True}
    finally:
        db.close()

@router.get("/admin/grupos")
async def listar_grupos(user=Depends(requer_nivel(4))):
    return {"grupos":[{"id":k,"label":v["label"],"nivel":v["nivel"],"modulos":v["modulos"]} for k,v in GRUPOS.items()]}

@router.get("/admin/logs")
async def listar_logs(user=Depends(requer_nivel(4))):
    db = get_db()
    try:
        rows = db.execute("SELECT l.*,u.nome FROM sais_sessions_log l LEFT JOIN sais_usuarios u ON u.id=l.usuario_id ORDER BY l.criado_em DESC LIMIT 200").fetchall()
        return {"logs":[dict(r) for r in rows]}
    finally:
        db.close()
