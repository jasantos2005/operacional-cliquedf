#!/usr/bin/env python3
"""
backfill_junho.py — Reprocessa retroativamente um período de dias
que ficaram sem sync por causa do crontab quebrado.
"""
import argparse
import sqlite3
import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/automacoes/cliquedf/operacional")

import pymysql
from dotenv import load_dotenv

load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

STATUS_MAP = {
    "A": "aberta", "F": "finalizada", "C": "cancelada",
    "AG": "agendada", "RAG": "agendada",
    "EN": "execucao", "AS": "execucao", "EX": "execucao",
}


def get_ixc_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=15,
    )


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def clean_dt(v):
    if not v or str(v).startswith("0000"):
        return None
    return str(v)


def diff_min(a, b):
    if not a or not b:
        return None
    try:
        fmt = "%Y-%m-%d %H:%M:%S"
        ta = datetime.strptime(str(a)[:19], fmt)
        tb = datetime.strptime(str(b)[:19], fmt)
        diff = (tb - ta).total_seconds() / 60
        return round(diff, 1) if diff >= 0 else None
    except Exception:
        return None


def get_categoria(id_assunto):
    cat_servico  = set(int(x) for x in os.getenv("IXC_ASSUNTOS_SERVICO", "").split(",") if x.strip())
    cat_suporte  = set(int(x) for x in os.getenv("IXC_ASSUNTOS_SUPORTE", "").split(",") if x.strip())
    cat_infra    = set(int(x) for x in os.getenv("IXC_ASSUNTOS_INFRA", "").split(",") if x.strip())
    cat_retirada = set(int(x) for x in os.getenv("IXC_ASSUNTOS_RETIRADA", "").split(",") if x.strip())
    if id_assunto in cat_servico:  return "servico"
    if id_assunto in cat_suporte:  return "suporte"
    if id_assunto in cat_infra:    return "infra"
    if id_assunto in cat_retirada: return "retirada"
    return "suporte"


def sync_dia(db, dia_str, dry_run):
    ixc = get_ixc_conn()
    with ixc.cursor() as cur:
        cur.execute(f"""
            SELECT
                o.id                AS ixc_os_id,
                o.id_tecnico        AS ixc_funcionario_id,
                o.id_assunto        AS ixc_assunto_id,
                o.status             AS status_raw,
                o.data_abertura,
                o.data_fechamento,
                o.data_agenda,
                o.data_hora_assumido,
                o.data_hora_execucao
            FROM su_oss_chamado o
            WHERE DATE(o.data_reservada) = %s
              AND o.id_tecnico IN ({TECS_IDS})
        """, (dia_str,))
        rows = cur.fetchall()
    ixc.close()

    inseridos = atualizados = 0
    for o in rows:
        status = STATUS_MAP.get(o.get("status_raw") or "A", "aberta")
        categoria = get_categoria(o["ixc_assunto_id"] or 0)

        data_fechamento    = clean_dt(o.get("data_fechamento"))
        data_agenda        = clean_dt(o.get("data_agenda"))
        data_abertura      = clean_dt(o.get("data_abertura"))
        data_hora_assumido = clean_dt(o.get("data_hora_assumido"))
        data_hora_execucao = clean_dt(o.get("data_hora_execucao"))

        sla_fila_min    = diff_min(data_abertura, data_hora_assumido)
        sla_desloc_min  = diff_min(data_hora_assumido, data_hora_execucao)
        sla_exec_min    = diff_min(data_hora_execucao, data_fechamento)
        sla_tecnico_min = diff_min(data_hora_assumido, data_fechamento)

        if dry_run:
            inseridos += 1
            continue

        existing = db.execute(
            "SELECT id FROM prod_os_cache WHERE ixc_os_id=?", (o["ixc_os_id"],)
        ).fetchone()

        if existing:
            db.execute("""
                UPDATE prod_os_cache SET
                    status=?, categoria=?,
                    data_fechamento=?, data_agenda=?,
                    data_hora_assumido=?, data_hora_execucao=?,
                    sla_fila_min=?, sla_desloc_min=?,
                    sla_exec_min=?, sla_tecnico_min=?,
                    sincronizado_em=datetime('now','-3 hours')
                WHERE ixc_os_id=?
            """, (status, categoria, data_fechamento, data_agenda,
                  data_hora_assumido, data_hora_execucao,
                  sla_fila_min, sla_desloc_min, sla_exec_min, sla_tecnico_min,
                  o["ixc_os_id"]))
            atualizados += 1
        else:
            tec_row = db.execute(
                "SELECT id FROM prod_tecnicos WHERE ixc_funcionario_id=?",
                (o["ixc_funcionario_id"],)
            ).fetchone()
            tec_id = tec_row["id"] if tec_row else None

            db.execute("""
                INSERT INTO prod_os_cache
                    (ixc_os_id, tecnico_id, ixc_assunto_id, categoria, status,
                     data_abertura, data_fechamento, data_agenda,
                     data_hora_assumido, data_hora_execucao,
                     sla_fila_min, sla_desloc_min, sla_exec_min, sla_tecnico_min,
                     sincronizado_em)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','-3 hours'))
            """, (o["ixc_os_id"], tec_id, o["ixc_assunto_id"], categoria, status,
                  data_abertura, data_fechamento, data_agenda,
                  data_hora_assumido, data_hora_execucao,
                  sla_fila_min, sla_desloc_min, sla_exec_min, sla_tecnico_min))
            inseridos += 1

    if not dry_run:
        db.commit()
    return len(rows), inseridos, atualizados


def rodar_regras_dia(dia_str, dry_run):
    if dry_run:
        return
    try:
        from app.engines.regras_engine import rodar_regras
        rodar_regras(dia_str)
    except Exception as e:
        print(f"  [REGRAS] ERRO em {dia_str}: {e}")


def rodar_destaques_dia(db, dia_str, dry_run):
    ixc = get_ixc_conn()
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
        """, (dia_str,))
        dados = cur.fetchall()
    ixc.close()

    salvos = 0
    for d in dados:
        total = d["total_os"] or 0
        fins  = d["finalizadas"] or 0
        if total > 0 and fins == total:
            row = db.execute(
                "SELECT id, nome FROM prod_tecnicos WHERE ixc_funcionario_id=?",
                (d["func_id"],)
            ).fetchone()
            if not row:
                continue
            if dry_run:
                salvos += 1
                continue
            db.execute("""
                INSERT INTO sais_destaques
                  (data, tecnico_id, tecnico_nome, total_os, os_pontuais, bonus_pts)
                VALUES (?,?,?,?,?,15)
                ON CONFLICT(data,tecnico_id) DO UPDATE SET
                  total_os=excluded.total_os,
                  os_pontuais=excluded.os_pontuais,
                  bonus_pts=15
            """, (dia_str, row["id"], row["nome"], int(total), int(fins)))
            salvos += 1
    if not dry_run:
        db.commit()
    return salvos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inicio", required=True, help="YYYY-MM-DD")
    ap.add_argument("--fim", required=True, help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    d1 = datetime.strptime(args.inicio, "%Y-%m-%d").date()
    d2 = datetime.strptime(args.fim, "%Y-%m-%d").date()

    if d1 > d2:
        print("--inicio precisa ser <= --fim")
        sys.exit(1)

    db = get_db()
    dia = d1
    print(f"{'[DRY-RUN] ' if args.dry_run else ''}Backfill de {d1} até {d2}\n")

    while dia <= d2:
        dia_str = dia.strftime("%Y-%m-%d")
        total, ins, upd = sync_dia(db, dia_str, args.dry_run)
        print(f"{dia_str} — OS no IXC: {total} | inseridas: {ins} | atualizadas: {upd}")

        rodar_regras_dia(dia_str, args.dry_run)
        salvos = rodar_destaques_dia(db, dia_str, args.dry_run)
        print(f"           destaques gravados: {salvos}")

        dia += timedelta(days=1)

    db.close()
    print("\nConcluído.")


if __name__ == "__main__":
    main()
