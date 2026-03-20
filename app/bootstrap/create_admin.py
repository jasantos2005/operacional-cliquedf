#!/usr/bin/env python3
import sqlite3, hashlib, os, sys
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS prod_tecnicos (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    nome                TEXT NOT NULL,
    ixc_funcionario_id  INTEGER UNIQUE NOT NULL,
    meta_dia            INTEGER DEFAULT 8,
    meta_mes            INTEGER DEFAULT 176,
    setor               TEXT DEFAULT 'Campo',
    ativo               INTEGER DEFAULT 1,
    criado_em           TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE TABLE IF NOT EXISTS prod_os_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ixc_os_id       INTEGER UNIQUE NOT NULL,
    tecnico_id      INTEGER REFERENCES prod_tecnicos(id),
    ixc_assunto_id  INTEGER,
    categoria       TEXT,
    status          TEXT,
    data_abertura   TEXT,
    data_fechamento TEXT,
    sincronizado_em TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE TABLE IF NOT EXISTS prod_metas (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tecnico_id INTEGER,
    tipo       TEXT,
    valor      REAL,
    periodo    TEXT,
    vigente    INTEGER DEFAULT 1,
    criado_em  TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE TABLE IF NOT EXISTS prod_usuarios (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    login         TEXT UNIQUE NOT NULL,
    nome          TEXT NOT NULL,
    senha_hash    TEXT NOT NULL,
    nivel         INTEGER DEFAULT 1,
    ativo         INTEGER DEFAULT 1,
    ultimo_acesso TEXT,
    criado_em     TEXT DEFAULT (datetime('now','-3 hours'))
);
CREATE TABLE IF NOT EXISTS prod_permissoes (
    modulo    TEXT,
    nivel     INTEGER,
    permitido INTEGER DEFAULT 0,
    PRIMARY KEY (modulo, nivel)
);
CREATE INDEX IF NOT EXISTS idx_os_tecnico   ON prod_os_cache(tecnico_id);
CREATE INDEX IF NOT EXISTS idx_os_abertura  ON prod_os_cache(data_abertura);
CREATE INDEX IF NOT EXISTS idx_os_categoria ON prod_os_cache(categoria);
"""

TECNICOS = [
    {"ixc_funcionario_id": 13, "nome": "ALEXANDRE",                  "meta_dia": 8},
    {"ixc_funcionario_id": 17, "nome": "DENISON",                    "meta_dia": 8},
    {"ixc_funcionario_id": 43, "nome": "DEYVID MARQUES",             "meta_dia": 8},
    {"ixc_funcionario_id": 11, "nome": "JONATHAN",                   "meta_dia": 8},
    {"ixc_funcionario_id": 38, "nome": "JOSEILTON",                  "meta_dia": 8},
    {"ixc_funcionario_id": 47, "nome": "LEANDRO",                    "meta_dia": 8},
    {"ixc_funcionario_id": 50, "nome": "RICARDO - ILHA",             "meta_dia": 8},
    {"ixc_funcionario_id": 35, "nome": "RODRIGO SANTOS",             "meta_dia": 8},
    {"ixc_funcionario_id": 56, "nome": "ROGERIO",                    "meta_dia": 8},
    {"ixc_funcionario_id": 55, "nome": "VICTOR FERREIRA DA SILVA",   "meta_dia": 8},
    {"ixc_funcionario_id": 46, "nome": "WELLINGTON PIAÇABUÇU",       "meta_dia": 8},
]

PERMISSOES = [
    ("PROD_GERAL",   1,1,1,1),
    ("PROD_RANKING", 1,1,1,1),
    ("PROD_OS",      1,1,1,1),
    ("PROD_METAS",   0,1,1,1),
    ("PROD_TEC",     0,0,1,1),
    ("PROD_TG",      0,0,1,1),
    ("PROD_USR",     0,0,0,1),
    ("PROD_PERM",    0,0,0,1),
    ("PROD_CFG",     0,0,0,1),
]

METAS_PADRAO = [
    (None, "os_dia",        150,  "diario"),
    (None, "os_mes",        3300, "mensal"),
    (None, "eficiencia_min", 80,  "diario"),
]

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)

    for t in TECNICOS:
        conn.execute(
            "INSERT OR IGNORE INTO prod_tecnicos (ixc_funcionario_id, nome, meta_dia) VALUES (?,?,?)",
            (t["ixc_funcionario_id"], t["nome"], t["meta_dia"])
        )
    print(f"  ✅ {len(TECNICOS)} técnicos inseridos")

    for p in PERMISSOES:
        for nivel, val in enumerate(p[1:], 1):
            conn.execute(
                "INSERT OR REPLACE INTO prod_permissoes VALUES (?,?,?)",
                (p[0], nivel, val)
            )
    print(f"  ✅ Permissões configuradas")

    for m in METAS_PADRAO:
        conn.execute(
            "INSERT OR IGNORE INTO prod_metas (tecnico_id,tipo,valor,periodo) VALUES (?,?,?,?)", m
        )
    print(f"  ✅ Metas padrão inseridas")

    senha = "admin123"
    h = hashlib.sha256(senha.encode()).hexdigest()
    conn.execute(
        "INSERT OR REPLACE INTO prod_usuarios (login,nome,senha_hash,nivel) VALUES (?,?,?,99)",
        ("admin", "Ailton Santos", h)
    )
    print(f"  ✅ Usuário admin criado (senha: admin123)")

    conn.commit()
    conn.close()
    print(f"\n✅ Banco criado: {DB_PATH}")
    print(f"   Próximo: venv/bin/python app/bootstrap/cron_sync_ixc.py")

if __name__ == "__main__":
    main()
