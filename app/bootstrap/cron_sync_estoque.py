#!/usr/bin/env python3
"""
cron_sync_estoque.py
Sincroniza saldo do IXC para sais_estoque_tecnico.
Executar diariamente às 06:00:
  0 6 * * * cd /opt/.../operacional && source venv/bin/activate && python3 -m app.bootstrap.cron_sync_estoque >> /var/log/sais_estoque.log 2>&1
"""
import sqlite3, pymysql, os, sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

ALMOX_TECS = {
    12: {"tec_id": 1,  "func_id": 13, "nome": "ALEXANDRE"},
     7: {"tec_id": 2,  "func_id": 17, "nome": "DENISON"},
    15: {"tec_id": 4,  "func_id": 11, "nome": "JONATHAN"},
    38: {"tec_id": 5,  "func_id": 38, "nome": "JOSEILTON"},
    44: {"tec_id": 6,  "func_id": 47, "nome": "LEANDRO"},
    46: {"tec_id": 7,  "func_id": 50, "nome": "RICARDO - ILHA"},
    33: {"tec_id": 8,  "func_id": 35, "nome": "RODRIGO SANTOS"},
    51: {"tec_id": 9,  "func_id": 56, "nome": "ROGERIO"},
    49: {"tec_id": 10, "func_id": 55, "nome": "VICTOR FERREIRA"},
    43: {"tec_id": 11, "func_id": 46, "nome": "WELLINGTON"},
    11: {"tec_id": 12, "func_id": 19, "nome": "JOSE MARCONDES"},
}

def sincronizar():
    agora = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{agora} [ESTOQUE] Iniciando sync...")

    try:
        ixc = pymysql.connect(
            host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )
        ph = ",".join(["%s"] * len(ALMOX_TECS))
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT id_almox, id_produto, produto_descricao,
                       ROUND(saldo, 3) AS saldo, produto_unidade
                FROM estoque_produtos_almox_filial
                WHERE id_almox IN ({ph})
                  AND produto_controla_estoque = 'S'
            """, list(ALMOX_TECS.keys()))
            rows = cur.fetchall()
        ixc.close()

        db = sqlite3.connect(DB_PATH)
        total = 0
        for r in rows:
            t = ALMOX_TECS.get(r["id_almox"])
            if not t: continue
            db.execute("""
                INSERT INTO sais_estoque_tecnico
                  (tecnico_id,ixc_func_id,almox_id,id_produto,produto_nome,saldo,unidade,sincronizado_em)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(almox_id,id_produto) DO UPDATE SET
                  saldo=excluded.saldo,
                  produto_nome=excluded.produto_nome,
                  sincronizado_em=excluded.sincronizado_em
            """, (t["tec_id"], t["func_id"], r["id_almox"], r["id_produto"],
                  r["produto_descricao"], float(r["saldo"] or 0),
                  r["produto_unidade"] or "UND", agora))
            total += 1
        db.commit()
        db.close()
        print(f"{agora} [ESTOQUE] {total} produtos sincronizados")
        return total

    except Exception as e:
        print(f"{agora} [ESTOQUE] ERRO: {e}")
        import traceback; traceback.print_exc()
        return 0

if __name__ == "__main__":
    sincronizar()
