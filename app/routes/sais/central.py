"""
SAIS — Central Operacional
OS críticas, técnicos ociosos, regiões críticas.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Query

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")


@router.get("/os-criticas")
async def get_os_criticas():
    """OS atrasadas e em risco de SLA."""
    db = get_db()
    agora = datetime.now()
    limite_4h = (agora - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura, o.data_fechamento,
            t.nome AS tecnico,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            ROUND((julianday('now') - julianday(o.data_abertura)) * 24, 1) AS horas_abertas
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status IN ('execucao', 'aberta')
          AND o.data_abertura < ?
        ORDER BY horas_abertas DESC
    """, (limite_4h,)).fetchall()
    db.close()

    return {"os_criticas": [dict(r) for r in rows], "total": len(rows)}


@router.get("/tecnicos-ociosos")
async def get_tecnicos_ociosos():
    """Técnicos sem OS há mais de 1.5h."""
    db = get_db()
    config = db.execute(
        "SELECT valor FROM sais_config WHERE chave='tecnico_ocioso_horas'"
    ).fetchone()
    horas = float(config["valor"]) if config else 1.5
    limite = (datetime.now() - timedelta(hours=horas)).strftime("%Y-%m-%d %H:%M:%S")

    tecnicos = db.execute("SELECT id, nome FROM prod_tecnicos WHERE ativo=1").fetchall()
    ociosos = []

    for t in tecnicos:
        ultima = db.execute("""
            SELECT MAX(COALESCE(data_fechamento, data_abertura)) as ultima,
                   status
            FROM prod_os_cache
            WHERE tecnico_id=?
              AND DATE(COALESCE(data_fechamento, data_abertura), '+3 hours') = ?
        """, (t["id"], hoje_brt())).fetchone()

        if ultima and ultima["ultima"] and ultima["ultima"] < limite:
            dt = datetime.strptime(str(ultima["ultima"])[:19], "%Y-%m-%d %H:%M:%S")
            h = round((datetime.now() - dt).total_seconds() / 3600, 1)
            ociosos.append({
                "tecnico_id": t["id"],
                "nome": t["nome"],
                "horas_ocioso": h,
                "ultimo_status": ultima["status"],
            })

    db.close()
    return {"tecnicos_ociosos": ociosos, "total": len(ociosos)}


@router.get("/resumo-critico")
async def get_resumo_critico():
    """Resumo rápido para o topo da Central Operacional."""
    db = get_db()
    agora = datetime.now()

    atrasadas = db.execute("""
        SELECT COUNT(*) as total FROM prod_os_cache
        WHERE status IN ('execucao','aberta')
          AND data_abertura < ?
    """, ((agora - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S"),)).fetchone()

    alertas_criticos = db.execute(
        "SELECT COUNT(*) as total FROM sais_alertas WHERE criticidade='critico' AND lido=0"
    ).fetchone()

    audits_criticas = db.execute(
        "SELECT COUNT(*) as total FROM sais_auditorias WHERE criticidade='critica' AND resolvida=0"
    ).fetchone()

    agendadas_hoje = db.execute("""
        SELECT COUNT(*) as total FROM prod_os_cache
        WHERE status='agendada'
          AND DATE(data_agenda, '+3 hours') = ?
    """, (hoje_brt(),)).fetchone()

    db.close()
    return {
        "os_atrasadas":       atrasadas["total"] if atrasadas else 0,
        "alertas_criticos":   alertas_criticos["total"] if alertas_criticos else 0,
        "audits_criticas":    audits_criticas["total"] if audits_criticas else 0,
        "agendadas_hoje":     agendadas_hoje["total"] if agendadas_hoje else 0,
    }


@router.get("/reincidencias")
async def get_reincidencias():
    """Reincidências reais: mesmo contrato (id_login), assunto de suporte, dentro de 60 dias.
    Exclui retirada, instalação e infra. Busca direto no IXC MySQL."""
    import os as _os, pymysql

    # Assuntos de suporte considerados (sem acesso / lenta / reincidência)
    ASSUNTOS_SUPORTE = (5, 20, 21, 44, 47)  # 94=[CDF] SUPORTE TECNICO excluído — é intermediário

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10,
        )
        ph = ",".join([str(x) for x in ASSUNTOS_SUPORTE])

        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT
                    o.id              AS os_id,
                    o.id_login        AS contrato_id,
                    o.id_assunto,
                    a.assunto,
                    o.data_abertura,
                    o.status,
                    f.funcionario     AS tecnico,
                    cl.razao          AS cliente_nome,
                    cnt.total         AS ocorrencias,
                    ROUND(TIMESTAMPDIFF(HOUR, o.data_abertura, NOW()), 1) AS horas_abertas
                FROM su_oss_chamado o
                JOIN (
                    SELECT id_login, id_assunto, COUNT(*) AS total
                    FROM su_oss_chamado
                    WHERE id_assunto IN ({ph})
                      AND id_login > 0
                      AND data_abertura >= DATE_SUB(NOW(), INTERVAL 60 DAY)
                      AND status != 'C'
                    GROUP BY id_login, id_assunto
                    HAVING COUNT(*) >= 2
                ) cnt ON cnt.id_login = o.id_login
                     AND cnt.id_assunto = o.id_assunto
                LEFT JOIN su_oss_assunto a  ON a.id = o.id_assunto
                LEFT JOIN funcionarios f    ON f.id = o.id_tecnico
                LEFT JOIN cliente cl        ON cl.id = o.id_cliente
                WHERE o.id_assunto IN ({ph})
                  AND o.id_login > 0
                  AND o.data_abertura >= DATE_SUB(NOW(), INTERVAL 60 DAY)
                  AND o.status != 'C'
                ORDER BY cnt.total DESC, o.data_abertura DESC
                LIMIT 100
            """)
            rows = cur.fetchall()
        ixc.close()

        # Serializar datas
        result = []
        for r in rows:
            d = dict(r)
            if hasattr(d.get("data_abertura"), "strftime"):
                d["data_abertura"] = d["data_abertura"].strftime("%Y-%m-%d %H:%M:%S")
            result.append(d)

        return {"reincidencias": result, "total": len(result)}

    except Exception as e:
        print(f"ERRO reincidencias IXC: {e}")
        import traceback; traceback.print_exc()
        return {"reincidencias": [], "total": 0, "erro": str(e)}


@router.get("/os-sem-tecnico")
async def get_os_sem_tecnico():
    """OS abertas há mais de 1h sem técnico atribuído."""
    db = get_db()
    limite = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            ROUND((julianday('now') - julianday(o.data_abertura)) * 24, 1) AS horas_abertas
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE (o.tecnico_id IS NULL)
          AND o.status IN ('aberta', 'aguardando')
          AND o.data_abertura < ?
        ORDER BY horas_abertas DESC
        LIMIT 30
    """, (limite,)).fetchall()
    db.close()

    return {"os_sem_tecnico": [dict(r) for r in rows], "total": len(rows)}


@router.get("/concentradores-criticos")
async def get_concentradores_criticos():
    """Concentradores (radpop) com mais OS abertas — requer IXC MySQL."""
    import os as _os, pymysql
    db = get_db()
    hoje = hoje_brt()

    # Busca os ixc_os_ids de OS abertas/em execução hoje
    os_abertas = db.execute("""
        SELECT ixc_os_id FROM prod_os_cache
        WHERE status IN ('aberta','execucao','aguardando')
          AND DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (hoje,)).fetchall()
    db.close()

    if not os_abertas:
        return {"concentradores": [], "total": 0}

    ids = [r["ixc_os_id"] for r in os_abertas]
    ph  = ",".join(["%s"] * len(ids))

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=8,
        )
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT
                    rp.pop AS concentrador,
                    COUNT(DISTINCT o.id) AS total_os,
                    GROUP_CONCAT(DISTINCT o.id ORDER BY o.id SEPARATOR ',') AS os_ids
                FROM su_oss_chamado o
                JOIN cliente_contrato cc ON cc.id_cliente = o.id_cliente
                JOIN radpop_radio_cliente_fibra rf ON rf.id_contrato = cc.id
                JOIN radpop rp ON rp.id = rf.id_transmissor
                WHERE o.id IN ({ph})
                  AND rp.pop IS NOT NULL AND rp.pop != ''
                GROUP BY rp.pop
                ORDER BY total_os DESC
                LIMIT 10
            """, ids)
            rows = cur.fetchall()
        ixc.close()
        return {"concentradores": rows, "total": len(rows)}
    except Exception as e:
        print(f"ERRO concentradores-criticos IXC: {e}")
        return {"concentradores": [], "total": 0, "erro": str(e)}


@router.get("/tecnicos-em-atraso")
async def get_tecnicos_em_atraso():
    """Técnicos com OS em execução parada há mais de 2h (nominal)."""
    db = get_db()
    limite_horas = 2
    limite = (datetime.now() - timedelta(hours=limite_horas)).strftime("%Y-%m-%d %H:%M:%S")

    rows = db.execute("""
        SELECT
            t.nome AS tecnico,
            t.ixc_funcionario_id AS tecnico_id,
            o.ixc_os_id,
            o.data_abertura,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            o.categoria,
            ROUND((julianday('now') - julianday(o.data_abertura)) * 24, 1) AS horas_abertas
        FROM prod_os_cache o
        JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.status = 'execucao'
          AND o.data_abertura < ?
        ORDER BY horas_abertas DESC
        LIMIT 20
    """, (limite,)).fetchall()
    db.close()

    return {"tecnicos_em_atraso": [dict(r) for r in rows], "total": len(rows)}


@router.get("/instalacao-com-suporte")
async def get_instalacao_com_suporte():
    """Contratos que tiveram instalação e abriram suporte em até 30 dias.
    Indica qualidade da instalação por técnico."""
    import os as _os, pymysql

    ASSUNTOS_INSTALACAO = (2, 3, 26, 30, 48, 101, 239)
    ASSUNTOS_SUPORTE    = (5, 20, 21, 44, 47, 94)
    ph_inst = ",".join([str(x) for x in ASSUNTOS_INSTALACAO])
    ph_sup  = ",".join([str(x) for x in ASSUNTOS_SUPORTE])

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10,
        )
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT
                    inst.id          AS os_inst_id,
                    sup.id           AS os_sup_id,
                    inst.id_login    AS contrato_id,
                    cl.razao         AS cliente_nome,
                    a_inst.assunto   AS assunto_instalacao,
                    a_sup.assunto    AS assunto_suporte,
                    f_inst.funcionario AS tecnico_instalacao,
                    f_sup.funcionario  AS tecnico_suporte,
                    inst.data_fechamento AS data_instalacao,
                    sup.data_abertura   AS data_suporte,
                    DATEDIFF(sup.data_abertura, inst.data_fechamento) AS dias_entre
                FROM su_oss_chamado inst
                JOIN su_oss_chamado sup
                    ON  sup.id_login   = inst.id_login
                    AND sup.id_assunto IN ({ph_sup})
                    AND sup.data_abertura > inst.data_fechamento
                    AND sup.data_abertura <= DATE_ADD(inst.data_fechamento, INTERVAL 30 DAY)
                    AND sup.status != 'C'
                LEFT JOIN su_oss_assunto a_inst ON a_inst.id = inst.id_assunto
                LEFT JOIN su_oss_assunto a_sup  ON a_sup.id  = sup.id_assunto
                LEFT JOIN funcionarios f_inst   ON f_inst.id = inst.id_tecnico
                LEFT JOIN funcionarios f_sup    ON f_sup.id  = sup.id_tecnico
                LEFT JOIN cliente cl            ON cl.id     = inst.id_cliente
                WHERE inst.id_assunto IN ({ph_inst})
                  AND inst.status = 'F'
                  AND inst.data_fechamento >= DATE_SUB(NOW(), INTERVAL 90 DAY)
                  AND inst.id_login > 0
                ORDER BY dias_entre ASC
                LIMIT 50
            """)
            rows = cur.fetchall()
        ixc.close()

        result = []
        for r in rows:
            d = dict(r)
            for campo in ("data_instalacao", "data_suporte"):
                if hasattr(d.get(campo), "strftime"):
                    d[campo] = d[campo].strftime("%Y-%m-%d %H:%M:%S")
            result.append(d)

        return {"casos": result, "total": len(result)}

    except Exception as e:
        print(f"ERRO instalacao-com-suporte IXC: {e}")
        import traceback; traceback.print_exc()
        return {"casos": [], "total": 0, "erro": str(e)}

@router.get("/sla")
async def get_sla_dashboard():
    """Dashboard de SLA: já estouradas + prestes a estourar + ranking técnicos."""
    import os as _os, pymysql
    from datetime import datetime as _dt, timedelta as _td
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    TECS_IDS = "13,17,11,38,47,50,35,56,55,46,19"

    # Mapa de categoria por assunto (do banco local)
    import sqlite3
    db = sqlite3.connect("/opt/automacoes/cliquedf/operacional/prod_local.db")
    db.row_factory = sqlite3.Row
    cat_map = {}
    for r in db.execute("SELECT ixc_os_id, categoria FROM prod_os_cache").fetchall():
        cat_map[r["ixc_os_id"]] = r["categoria"]
    db.close()

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT",3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )

        with ixc.cursor() as cur:
            # Já estouradas
            cur.execute(f"""
                SELECT c.id, f.funcionario AS tecnico, f.id AS func_id,
                       a.assunto, c.status, c.status_sla,
                       c.data_abertura, c.data_prazo_limite,
                       TIMESTAMPDIFF(HOUR, c.data_prazo_limite, NOW()) AS horas_atraso,
                       TIMESTAMPDIFF(DAY, c.data_abertura, NOW()) AS dias_aberto,
                       (SELECT COUNT(*) FROM su_oss_chamado_mensagem m WHERE m.id_chamado=c.id) AS interacoes
                FROM su_oss_chamado c
                LEFT JOIN funcionarios f ON f.id=c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id=c.id_assunto
                WHERE c.id_tecnico IN ({TECS_IDS})
                  AND c.status NOT IN ('F','C')
                  AND c.data_prazo_limite < NOW()
                  AND c.data_prazo_limite > '2020-01-01'
                ORDER BY c.data_prazo_limite ASC
                LIMIT 50
            """)
            estouradas = cur.fetchall()

            # Prestes a estourar (próximas 4h)
            cur.execute(f"""
                SELECT c.id, f.funcionario AS tecnico, f.id AS func_id,
                       a.assunto, c.status, c.data_abertura, c.data_prazo_limite,
                       TIMESTAMPDIFF(MINUTE, NOW(), c.data_prazo_limite) AS min_restantes,
                       TIMESTAMPDIFF(DAY, c.data_abertura, NOW()) AS dias_aberto,
                       (SELECT COUNT(*) FROM su_oss_chamado_mensagem m WHERE m.id_chamado=c.id) AS interacoes
                FROM su_oss_chamado c
                LEFT JOIN funcionarios f ON f.id=c.id_tecnico
                LEFT JOIN su_oss_assunto a ON a.id=c.id_assunto
                WHERE c.id_tecnico IN ({TECS_IDS})
                  AND c.status NOT IN ('F','C')
                  AND c.data_prazo_limite > NOW()
                  AND c.data_prazo_limite <= DATE_ADD(NOW(), INTERVAL 4 HOUR)
                ORDER BY c.data_prazo_limite ASC
                LIMIT 20
            """)
            prestes = cur.fetchall()

        ixc.close()

        # Ranking de técnicos com mais estouros
        ranking_tec = {}
        for r in estouradas:
            tec = r["tecnico"] or "Desconhecido"
            if tec not in ranking_tec:
                ranking_tec[tec] = {"nome": tec, "total": 0, "critico": 0}
            ranking_tec[tec]["total"] += 1
            if int(r["horas_atraso"] or 0) >= 24:
                ranking_tec[tec]["critico"] += 1

        ranking_final = sorted(ranking_tec.values(), key=lambda x: -x["total"])

        def fmt_os(r, tipo="estourada"):
            cat = cat_map.get(r["id"], "outros")
            base = {
                "os_id":     r["id"],
                "tecnico":   r["tecnico"] or "—",
                "assunto":   r["assunto"] or "—",
                "status":    r["status"] or "—",
                "categoria": cat,
                "abertura":  str(r["data_abertura"])[:16] if r["data_abertura"] else "—",
                "prazo":     str(r["data_prazo_limite"])[:16] if r["data_prazo_limite"] else "—",
                "interacoes": int(r.get("interacoes") or 0),
            }
            if tipo == "estourada":
                base["horas_atraso"] = int(r["horas_atraso"] or 0)
                base["dias_aberto"]  = int(r["dias_aberto"] or 0)
            else:
                base["min_restantes"] = int(r["min_restantes"] or 0)
            return base

        return {
            "resumo": {
                "total_estouradas": len(estouradas),
                "total_prestes":    len(prestes),
                "criticas":         sum(1 for r in estouradas if int(r["horas_atraso"] or 0) >= 24),
            },
            "estouradas": [fmt_os(r, "estourada") for r in estouradas],
            "prestes":    [fmt_os(r, "prestes")   for r in prestes],
            "ranking":    ranking_final,
        }

    except Exception as e:
        print(f"ERRO sla: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "estouradas": [], "prestes": [], "ranking": []}
