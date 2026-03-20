#!/usr/bin/env python3
import sqlite3, pymysql, os, sys, logging
from datetime import datetime
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [SYNC] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

CATEGORIAS = {
    "servico":  {2,14,15,19,26,30,48,49,92,101,110,127,220,227,239},
    "suporte":  {5,17,20,21,27,44,47,94,102,103,104,105,107,113,184,203,240,245},
    "infra":    {3,7,16,138,142,143,145,146,148,151,152,153,154,155,156,157,158,
                 159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,
                 175,176,177,178,179,180,181,182,183,185,186,187,188,221,222,232,
                 242,243,244},
    "retirada": {6,22,39,40,89,111},
}

STATUS_MAP = {"A": "aberta", "F": "finalizada", "C": "cancelada"}

def classificar(assunto_id):
    for cat, ids in CATEGORIAS.items():
        if assunto_id in ids:
            return cat
    return "outros"

def get_ixc_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )

def sync():
    log.info("Iniciando sync...")

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    # Mapa ixc_funcionario_id → id local
    rows = db.execute("SELECT id, ixc_funcionario_id FROM prod_tecnicos WHERE ativo=1").fetchall()
    tec_map = {r["ixc_funcionario_id"]: r["id"] for r in rows}
    ids_ixc = list(tec_map.keys())

    if not ids_ixc:
        log.error("Nenhum técnico ativo no banco local.")
        return

    log.info(f"Técnicos ativos: {ids_ixc}")

    try:
        ixc = get_ixc_conn()
    except Exception as e:
        log.error(f"Falha ao conectar no MySQL IXC: {e}")
        sys.exit(1)

    placeholders = ",".join(["%s"] * len(ids_ixc))
    with ixc.cursor() as cur:
        cur.execute(f"""
            SELECT
                o.id                AS ixc_os_id,
                o.id_tecnico        AS ixc_funcionario_id,
                o.id_assunto        AS ixc_assunto_id,
                o.status            AS status_raw,
                o.data_abertura     AS data_abertura,
                o.data_fechamento   AS data_fechamento
            FROM su_oss_chamado o
            WHERE o.id_tecnico IN ({placeholders})
              AND DATE(CONVERT_TZ(o.data_abertura, '+00:00', '-03:00'))
                  = DATE(CONVERT_TZ(NOW(), '+00:00', '-03:00'))
              AND o.status IN ('A','F','C')
            ORDER BY o.data_abertura DESC
        """, ids_ixc)
        os_list = cur.fetchall()
    ixc.close()

    log.info(f"OS encontradas no IXC hoje: {len(os_list)}")

    inseridos = atualizados = 0
    cats = {}

    for o in os_list:
        ixc_os_id    = o["ixc_os_id"]
        tec_local_id = tec_map.get(o["ixc_funcionario_id"])
        categoria    = classificar(o["ixc_assunto_id"] or 0)
        status       = STATUS_MAP.get(o["status_raw"], "aberta")
        data_ab      = str(o["data_abertura"])  if o["data_abertura"]  else None
        data_fech    = str(o["data_fechamento"]) if o["data_fechamento"] else None

        cats[categoria] = cats.get(categoria, 0) + 1

        existe = db.execute(
            "SELECT id FROM prod_os_cache WHERE ixc_os_id=?", (ixc_os_id,)
        ).fetchone()

        if existe:
            db.execute("""
                UPDATE prod_os_cache
                SET status=?, data_fechamento=?,
                    sincronizado_em=datetime('now','-3 hours')
                WHERE ixc_os_id=?
            """, (status, data_fech, ixc_os_id))
            atualizados += 1
        else:
            db.execute("""
                INSERT INTO prod_os_cache
                    (ixc_os_id, tecnico_id, ixc_assunto_id, categoria,
                     status, data_abertura, data_fechamento)
                VALUES (?,?,?,?,?,?,?)
            """, (ixc_os_id, tec_local_id, o["ixc_assunto_id"],
                  categoria, status, data_ab, data_fech))
            inseridos += 1

    db.commit()
    db.close()

    log.info(f"Inseridos: {inseridos} | Atualizados: {atualizados}")
    log.info(f"Distribuição: {cats}")

if __name__ == "__main__":
    sync()
