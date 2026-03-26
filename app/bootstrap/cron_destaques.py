#!/usr/bin/env python3
"""
cron_destaques.py
Verifica quem finalizou 100% das OS agendadas hoje e grava em sais_destaques.
Executar diariamente às 19:00:
  0 19 * * * cd /opt/.../operacional && source venv/bin/activate && python3 -m app.bootstrap.cron_destaques >> /var/log/sais_destaques.log 2>&1
"""
import sqlite3, pymysql, os, sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/opt/automacoes/cliquedf/operacional/.env")
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

def rodar():
    agora = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M:%S")
    hoje  = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")
    print(f"{agora} [DESTAQUES] Verificando destaques de {hoje}...")

    try:
        ixc = pymysql.connect(
            host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT",3306)),
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT
                    f.id AS func_id,
                    f.funcionario AS nome,
                    COUNT(*) AS total_os,
                    SUM(CASE WHEN c.status='F' THEN 1 ELSE 0 END) AS finalizadas
                FROM su_oss_chamado c
                JOIN funcionarios f ON f.id = c.id_tecnico
                WHERE DATE(c.data_reservada) = %s
                  AND c.id_tecnico IN ({TECS_IDS})
                GROUP BY f.id
                HAVING total_os > 0
            """, (hoje,))
            dados = cur.fetchall()
        ixc.close()

        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        salvos = 0

        for d in dados:
            total = d["total_os"] or 0
            fins  = d["finalizadas"] or 0
            if total > 0 and fins == total:
                # Busca id interno do técnico
                row = db.execute(
                    "SELECT id, nome FROM prod_tecnicos WHERE ixc_funcionario_id=?",
                    (d["func_id"],)
                ).fetchone()
                if not row:
                    continue
                db.execute("""
                    INSERT INTO sais_destaques
                      (data, tecnico_id, tecnico_nome, total_os, os_pontuais, bonus_pts)
                    VALUES (?,?,?,?,?,15)
                    ON CONFLICT(data,tecnico_id) DO UPDATE SET
                      total_os=excluded.total_os,
                      os_pontuais=excluded.os_pontuais,
                      bonus_pts=15
                """, (hoje, row["id"], row["nome"], int(total), int(fins)))
                salvos += 1
                print(f"  ⭐ {row['nome']} — {fins}/{total} OS → +15 pts")

        db.commit()
        db.close()
        print(f"{agora} [DESTAQUES] ✅ {salvos} destaques gravados")

    except Exception as e:
        print(f"{agora} [DESTAQUES] ERRO: {e}")
        import traceback; traceback.print_exc()

if __name__ == "__main__":
    rodar()
