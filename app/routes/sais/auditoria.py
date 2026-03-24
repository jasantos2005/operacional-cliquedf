"""SAIS — Auditoria Operacional"""
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


@router.get("/ocorrencias")
async def get_ocorrencias(
    tipo: Optional[str] = Query(None),
    criticidade: Optional[str] = Query(None),
    tecnico_id: Optional[int] = Query(None),
    resolvida: Optional[int] = Query(0),
    limit: int = Query(50),
):
    """Lista de ocorrências de auditoria com filtros."""
    db = get_db()
    conds = [f"resolvida={resolvida}"]
    if tipo:        conds.append(f"a.tipo='{tipo}'")
    if criticidade: conds.append(f"a.criticidade='{criticidade}'")
    if tecnico_id:  conds.append(f"a.tecnico_id={tecnico_id}")
    where = "WHERE " + " AND ".join(conds)

    rows = db.execute(f"""
        SELECT
            a.*,
            t.nome AS tecnico_nome,
            COALESCE(ass.assunto, '') AS assunto_nome
        FROM sais_auditorias a
        LEFT JOIN prod_tecnicos t ON t.id = a.tecnico_id
        LEFT JOIN prod_os_cache o ON o.ixc_os_id = a.os_id
        LEFT JOIN prod_assuntos ass ON ass.id = o.ixc_assunto_id
        {where}
        ORDER BY
            CASE criticidade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                             WHEN 'media' THEN 3 ELSE 4 END,
            criado_em DESC
        LIMIT ?
    """, (limit,)).fetchall()
    db.close()

    return {"ocorrencias": [dict(r) for r in rows], "total": len(rows)}


@router.get("/resumo")
async def get_resumo_auditoria():
    """Resumo de auditorias para o dashboard."""
    db = get_db()
    rows = db.execute("""
        SELECT tipo, criticidade, COUNT(*) as total
        FROM sais_auditorias
        WHERE resolvida=0
        GROUP BY tipo, criticidade
        ORDER BY CASE criticidade WHEN 'critica' THEN 1 WHEN 'alta' THEN 2
                                  WHEN 'media' THEN 3 ELSE 4 END
    """).fetchall()

    totais = db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN criticidade='critica' THEN 1 ELSE 0 END) AS criticas,
            SUM(CASE WHEN criticidade='alta'    THEN 1 ELSE 0 END) AS altas,
            SUM(CASE WHEN criticidade='media'   THEN 1 ELSE 0 END) AS medias,
            SUM(CASE WHEN resolvida=1           THEN 1 ELSE 0 END) AS resolvidas
        FROM sais_auditorias
    """).fetchone()

    db.close()
    return {"por_tipo": [dict(r) for r in rows], "totais": dict(totais)}


@router.post("/ocorrencias/{audit_id}/resolver")
async def resolver_ocorrencia(audit_id: int):
    """Marca ocorrência como resolvida."""
    db = get_db()
    db.execute("UPDATE sais_auditorias SET resolvida=1 WHERE id=?", (audit_id,))
    db.commit()
    db.close()
    return {"ok": True}


@router.get("/score-risco")
async def get_score_risco():
    """Score de risco por técnico baseado em auditorias."""
    db = get_db()
    rows = db.execute("""
        SELECT
            t.id, t.nome,
            COUNT(a.id) AS total_auditorias,
            SUM(CASE WHEN a.criticidade='critica' THEN 4
                     WHEN a.criticidade='alta'    THEN 2
                     WHEN a.criticidade='media'   THEN 1
                     ELSE 0 END) AS score_risco
        FROM prod_tecnicos t
        LEFT JOIN sais_auditorias a ON a.tecnico_id = t.id AND a.resolvida=0
        WHERE t.ativo=1
        GROUP BY t.id, t.nome
        ORDER BY score_risco DESC
    """).fetchall()
    db.close()
    return {"score_risco": [dict(r) for r in rows]}


@router.get("/estoque")
async def get_auditoria_estoque():
    """Auditoria de estoque técnico — saldo atual por técnico com divergências."""
    import os as _os, pymysql
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    # Mapeamento almoxarifado → técnico (baseado no IXC)
    ALMOX_TECS = {
        12: {"nome": "ALEXANDRE",            "func_id": 13},
         7: {"nome": "DENISON",              "func_id": 17},
        15: {"nome": "JONATHAN",             "func_id": 11},
        38: {"nome": "JOSEILTON",            "func_id": 38},
        44: {"nome": "LEANDRO",              "func_id": 47},
        46: {"nome": "RICARDO - ILHA",       "func_id": 50},
        33: {"nome": "RODRIGO SANTOS",       "func_id": 35},
        51: {"nome": "ROGERIO",              "func_id": 56},
        49: {"nome": "VICTOR FERREIRA",      "func_id": 55},
        43: {"nome": "WELLINGTON PIAÇABUÇU", "func_id": 46},
        11: {"nome": "JOSE MARCONDES",       "func_id": 19},
    }

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )

        almox_ids = list(ALMOX_TECS.keys())
        ph = ",".join(["%s"] * len(almox_ids))

        with ixc.cursor() as cur:
            # Estoque atual por almoxarifado
            cur.execute(f"""
                SELECT e.id_almox, e.id_produto, e.produto_descricao,
                       ROUND(e.saldo, 2) AS saldo,
                       e.produto_controla_estoque
                FROM estoque_produtos_almox_filial e
                WHERE e.id_almox IN ({ph})
                  AND e.produto_controla_estoque = 'S'
                  AND e.saldo != 0
                ORDER BY e.id_almox, e.saldo ASC
            """, almox_ids)
            rows = cur.fetchall()

            # Movimentos recentes (últimos 30 dias) vinculados a OS
            cur.execute(f"""
                SELECT mp.id_almox, mp.id_produto, mp.descricao AS produto,
                       mp.qtde_saida, mp.data, mp.id_oss_chamado,
                       mp.status_comodato
                FROM movimento_produtos mp
                JOIN su_oss_chamado o ON o.id = mp.id_oss_chamado
                WHERE mp.id_almox IN ({ph})
                  AND mp.id_oss_chamado > 0
                  AND mp.data >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY mp.data DESC
                LIMIT 200
            """, almox_ids)
            movs = cur.fetchall()

        ixc.close()

        # Agrupa por técnico
        tecs = {}
        for almox_id, tec in ALMOX_TECS.items():
            key = tec["func_id"]
            if key not in tecs:
                tecs[key] = {
                    "nome":       tec["nome"],
                    "func_id":    tec["func_id"],
                    "almox_id":   almox_id,
                    "itens":      [],
                    "divergencias": 0,
                    "itens_negativos": [],
                    "total_itens": 0,
                }

        for r in rows:
            almox_id = r["id_almox"]
            if almox_id not in ALMOX_TECS:
                continue
            func_id = ALMOX_TECS[almox_id]["func_id"]
            if func_id not in tecs:
                continue
            item = {
                "produto": r["produto_descricao"],
                "id_produto": r["id_produto"],
                "saldo": float(r["saldo"]),
            }
            tecs[func_id]["itens"].append(item)
            tecs[func_id]["total_itens"] += 1
            if float(r["saldo"]) < 0:
                tecs[func_id]["divergencias"] += 1
                tecs[func_id]["itens_negativos"].append(item)

        # Movimentos recentes por técnico
        movs_por_tec = {}
        for m in movs:
            almox_id = m["id_almox"]
            if almox_id not in ALMOX_TECS:
                continue
            func_id = ALMOX_TECS[almox_id]["func_id"]
            if func_id not in movs_por_tec:
                movs_por_tec[func_id] = []
            movs_por_tec[func_id].append({
                "produto":     m["produto"],
                "qtde_saida":  float(m["qtde_saida"] or 0),
                "data":        str(m["data"])[:10],
                "os_id":       m["id_oss_chamado"],
                "comodato":    m["status_comodato"] or "",
            })

        resultado = []
        total_divergencias = 0
        total_negativos = 0

        for func_id, t in tecs.items():
            divs = t["divergencias"]
            total_divergencias += (1 if divs > 0 else 0)
            total_negativos += divs
            resultado.append({
                "nome":             t["nome"],
                "func_id":          t["func_id"],
                "almox_id":         t["almox_id"],
                "total_itens":      t["total_itens"],
                "divergencias":     divs,
                "precisao":         round((t["total_itens"] - divs) / t["total_itens"] * 100) if t["total_itens"] > 0 else 100,
                "itens_negativos":  t["itens_negativos"][:10],
                "movimentos_recentes": movs_por_tec.get(func_id, [])[:5],
            })

        resultado.sort(key=lambda x: -x["divergencias"])

        return {
            "resumo": {
                "tecnicos_com_divergencia": total_divergencias,
                "itens_negativos": total_negativos,
                "total_tecnicos": len(resultado),
                "precisao_geral": round(
                    sum(t["precisao"] for t in resultado) / len(resultado)
                ) if resultado else 100,
            },
            "tecnicos": resultado,
        }

    except Exception as e:
        print(f"ERRO estoque: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "resumo": {}, "tecnicos": []}


@router.get("/estoque-cadastro")
async def get_estoque_cadastro(
    tecnico_id:  Optional[int] = Query(None),
    busca:       Optional[str] = Query(None),
):
    """Lista estoque técnico do SAIS com comparação ao IXC."""
    import os as _os, pymysql
    from datetime import datetime as _dt
    db = get_db()

    where = "1=1"
    params = []
    if tecnico_id:
        where += " AND e.tecnico_id=?"
        params.append(tecnico_id)
    if busca:
        where += " AND e.produto_nome LIKE ?"
        params.append(f"%{busca}%")

    rows = db.execute(f"""
        SELECT e.id, e.tecnico_id, e.ixc_func_id, e.almox_id,
               e.id_produto, e.produto_nome, e.saldo, e.unidade,
               e.sincronizado_em, t.nome AS tecnico_nome
        FROM sais_estoque_tecnico e
        JOIN prod_tecnicos t ON t.id = e.tecnico_id
        WHERE {where}
        ORDER BY t.nome, e.produto_nome
    """, params).fetchall()

    # Resumo por técnico
    resumo = db.execute("""
        SELECT e.tecnico_id, t.nome,
               COUNT(*) AS total_itens,
               SUM(CASE WHEN e.saldo < 0 THEN 1 ELSE 0 END) AS negativos,
               MAX(e.sincronizado_em) AS ultima_sync
        FROM sais_estoque_tecnico e
        JOIN prod_tecnicos t ON t.id = e.tecnico_id
        GROUP BY e.tecnico_id
        ORDER BY negativos DESC, t.nome
    """).fetchall()

    # KPIs
    kpis = db.execute("""
        SELECT COUNT(DISTINCT tecnico_id) AS tecnicos,
               COUNT(*) AS total_itens,
               SUM(CASE WHEN saldo < 0 THEN 1 ELSE 0 END) AS negativos,
               MAX(sincronizado_em) AS ultima_sync
        FROM sais_estoque_tecnico
    """).fetchone()

    db.close()

    return {
        "kpis": dict(kpis) if kpis else {},
        "resumo_tecnicos": [dict(r) for r in resumo],
        "itens": [dict(r) for r in rows],
    }


@router.post("/estoque-sync")
async def sync_estoque_ixc():
    """Sincroniza saldo do IXC para sais_estoque_tecnico."""
    import os as _os, pymysql
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    ALMOX_TECS = {
        12: {"tec_id": 1,  "func_id": 13},
         7: {"tec_id": 2,  "func_id": 17},
        15: {"tec_id": 4,  "func_id": 11},
        38: {"tec_id": 5,  "func_id": 38},
        44: {"tec_id": 6,  "func_id": 47},
        46: {"tec_id": 7,  "func_id": 50},
        33: {"tec_id": 8,  "func_id": 35},
        51: {"tec_id": 9,  "func_id": 56},
        49: {"tec_id": 10, "func_id": 55},
        43: {"tec_id": 11, "func_id": 46},
        11: {"tec_id": 12, "func_id": 19},
    }

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT",3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )
        ph = ",".join(["%s"]*len(ALMOX_TECS))
        with ixc.cursor() as cur:
            cur.execute(f"""
                SELECT id_almox, id_produto, produto_descricao,
                       ROUND(saldo,3) AS saldo, produto_unidade
                FROM estoque_produtos_almox_filial
                WHERE id_almox IN ({ph})
                  AND produto_controla_estoque = 'S'
            """, list(ALMOX_TECS.keys()))
            rows = cur.fetchall()
        ixc.close()

        db = get_db()
        total = 0
        for r in rows:
            t = ALMOX_TECS.get(r["id_almox"])
            if not t: continue
            db.execute("""
                INSERT INTO sais_estoque_tecnico
                  (tecnico_id,ixc_func_id,almox_id,id_produto,produto_nome,saldo,unidade,sincronizado_em)
                VALUES (?,?,?,?,?,?,?,datetime('now','-3 hours'))
                ON CONFLICT(almox_id,id_produto) DO UPDATE SET
                  saldo=excluded.saldo,
                  produto_nome=excluded.produto_nome,
                  sincronizado_em=excluded.sincronizado_em
            """, (t["tec_id"],t["func_id"],r["id_almox"],r["id_produto"],
                  r["produto_descricao"],float(r["saldo"] or 0),r["produto_unidade"] or "UND"))
            total += 1
        db.commit()
        db.close()
        return {"ok": True, "sincronizados": total}

    except Exception as e:
        print(f"ERRO sync estoque: {e}")
        return {"ok": False, "erro": str(e)}


@router.get("/estoque-historico/{tecnico_id}")
async def get_estoque_historico(tecnico_id: int):
    """Histórico de ajustes de estoque de um técnico."""
    db = get_db()
    rows = db.execute("""
        SELECT a.*, t.nome AS tecnico_nome
        FROM sais_estoque_ajustes a
        JOIN prod_tecnicos t ON t.id = a.tecnico_id
        WHERE a.tecnico_id = ?
        ORDER BY a.criado_em DESC
        LIMIT 50
    """, (tecnico_id,)).fetchall()
    db.close()
    return {"historico": [dict(r) for r in rows]}


@router.patch("/estoque-ajuste")
async def ajustar_estoque(
    tecnico_id:   int = Query(...),
    id_produto:   int = Query(...),
    qtd_nova:     float = Query(...),
    tipo:         Optional[str] = Query("auditoria"),
    obs:          Optional[str] = Query(None),
    criado_por:   Optional[str] = Query(None),
):
    """Registra ajuste manual no estoque do técnico."""
    db = get_db()
    row = db.execute(
        "SELECT saldo, produto_nome FROM sais_estoque_tecnico WHERE tecnico_id=? AND id_produto=?",
        (tecnico_id, id_produto)
    ).fetchone()
    if not row:
        db.close()
        return {"erro": "Item não encontrado"}

    qtd_ant = row["saldo"]
    db.execute(
        "UPDATE sais_estoque_tecnico SET saldo=?, sincronizado_em=datetime('now','-3 hours') WHERE tecnico_id=? AND id_produto=?",
        (qtd_nova, tecnico_id, id_produto)
    )
    db.execute("""
        INSERT INTO sais_estoque_ajustes
          (tecnico_id,id_produto,produto_nome,qtd_anterior,qtd_nova,tipo,obs,criado_por)
        VALUES (?,?,?,?,?,?,?,?)
    """, (tecnico_id, id_produto, row["produto_nome"], qtd_ant, qtd_nova,
          tipo or "auditoria", obs, criado_por))
    db.commit()
    db.close()
    return {"ok": True, "qtd_anterior": qtd_ant, "qtd_nova": qtd_nova}


@router.get("/estoque-divergencias")
async def get_estoque_divergencias(
    data_base: Optional[str] = Query(None),
):
    """
    Auditoria real de estoque:
    saldo_base (data_base) + entradas IXC - saidas IXC = saldo_esperado
    divergencia = saldo_esperado - saldo_atual_IXC
    """
    import os as _os, pymysql
    from datetime import datetime as _dt, timedelta as _td
    from dotenv import load_dotenv
    load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

    hoje = (_dt.now() + _td(hours=-3)).strftime("%Y-%m-%d")
    data_base = data_base or hoje

    ALMOX_TECS = {
        12: {"tec_id": 1,  "nome": "ALEXANDRE"},
         7: {"tec_id": 2,  "nome": "DENISON"},
        15: {"tec_id": 4,  "nome": "JONATHAN"},
        38: {"tec_id": 5,  "nome": "JOSEILTON"},
        44: {"tec_id": 6,  "nome": "LEANDRO"},
        46: {"tec_id": 7,  "nome": "RICARDO - ILHA"},
        33: {"tec_id": 8,  "nome": "RODRIGO SANTOS"},
        51: {"tec_id": 9,  "nome": "ROGERIO"},
        49: {"tec_id": 10, "nome": "VICTOR FERREIRA"},
        43: {"tec_id": 11, "nome": "WELLINGTON"},
        11: {"tec_id": 12, "nome": "JOSE MARCONDES"},
    }

    db = get_db()

    # Saldo base do SAIS (importado na data_base)
    base_rows = db.execute("""
        SELECT e.tecnico_id, e.almox_id, e.id_produto, e.produto_nome,
               e.saldo AS saldo_base, e.unidade, t.nome AS tecnico_nome
        FROM sais_estoque_tecnico e
        JOIN prod_tecnicos t ON t.id = e.tecnico_id
        WHERE e.saldo != 0
    """).fetchall()
    db.close()

    # Monta dicionário base: (almox_id, id_produto) → saldo_base
    base = {}
    for r in base_rows:
        base[(r["almox_id"], r["id_produto"])] = {
            "tec_id": r["tecnico_id"],
            "tec_nome": r["tecnico_nome"],
            "produto": r["produto_nome"],
            "unidade": r["unidade"],
            "saldo_base": float(r["saldo_base"] or 0),
        }

    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=10
        )

        ph = ",".join(["%s"] * len(ALMOX_TECS))
        almox_ids = list(ALMOX_TECS.keys())

        with ixc.cursor() as cur:
            # Movimentos desde a data_base
            cur.execute(f"""
                SELECT id_almox, id_produto, tipo,
                       ROUND(quantidade, 3) AS entrada,
                       ROUND(qtde_saida, 3) AS saida,
                       id_oss_chamado, data, descricao
                FROM movimento_produtos
                WHERE id_almox IN ({ph})
                  AND data >= %s
                  AND tipo IN ('E','S')
                ORDER BY data ASC
            """, almox_ids + [data_base])
            movimentos = cur.fetchall()

            # Saldo atual do IXC
            cur.execute(f"""
                SELECT id_almox, id_produto, ROUND(saldo, 3) AS saldo_atual
                FROM estoque_produtos_almox_filial
                WHERE id_almox IN ({ph})
                  AND produto_controla_estoque = 'S'
            """, almox_ids)
            saldo_atual_ixc = {(r["id_almox"], r["id_produto"]): float(r["saldo_atual"] or 0)
                               for r in cur.fetchall()}

        ixc.close()

        # Calcula movimentação desde data_base por (almox, produto)
        movs = {}
        for m in movimentos:
            key = (m["id_almox"], m["id_produto"])
            if key not in movs:
                movs[key] = {"entradas": 0, "saidas": 0, "os_list": []}
            if m["tipo"] == "E":
                movs[key]["entradas"] += float(m["entrada"] or 0)
            elif m["tipo"] == "S":
                movs[key]["saidas"] += float(m["saida"] or 0)
                if m["id_oss_chamado"]:
                    movs[key]["os_list"].append({
                        "os_id": m["id_oss_chamado"],
                        "data": str(m["data"])[:10],
                        "qtde": float(m["saida"] or 0),
                        "produto": str(m["descricao"] or "")[:40],
                    })

        # Calcula divergências
        divergencias = []
        ok_count = 0
        total_itens = 0

        for key, b in base.items():
            almox_id, prod_id = key
            if almox_id not in ALMOX_TECS:
                continue
            total_itens += 1
            mov = movs.get(key, {"entradas": 0, "saidas": 0, "os_list": []})
            saldo_esp = b["saldo_base"] + mov["entradas"] - mov["saidas"]
            saldo_ixc = saldo_atual_ixc.get(key, 0)
            diverg = round(saldo_esp - saldo_ixc, 3)

            if abs(diverg) > 0.001:
                divergencias.append({
                    "tecnico":      b["tec_nome"],
                    "tecnico_id":   b["tec_id"],
                    "produto":      b["produto"],
                    "id_produto":   prod_id,
                    "almox_id":     almox_id,
                    "unidade":      b["unidade"],
                    "saldo_base":   b["saldo_base"],
                    "entradas":     mov["entradas"],
                    "saidas":       mov["saidas"],
                    "saldo_esperado": round(saldo_esp, 3),
                    "saldo_ixc":    saldo_ixc,
                    "divergencia":  diverg,
                    "tipo":         "falta" if diverg > 0 else "excesso",
                    "os_list":      mov["os_list"][-5:],
                })
            else:
                ok_count += 1

        divergencias.sort(key=lambda x: -abs(x["divergencia"]))

        # Resumo por técnico
        por_tec = {}
        for d in divergencias:
            t = d["tecnico"]
            if t not in por_tec:
                por_tec[t] = {"tecnico": t, "tecnico_id": d["tecnico_id"],
                               "divergencias": 0, "total_falta": 0, "total_excesso": 0}
            por_tec[t]["divergencias"] += 1
            if d["tipo"] == "falta":
                por_tec[t]["total_falta"] += 1
            else:
                por_tec[t]["total_excesso"] += 1

        return {
            "data_base": data_base,
            "data_auditoria": hoje,
            "resumo": {
                "total_itens":    total_itens,
                "com_divergencia": len(divergencias),
                "ok":             ok_count,
                "precisao":       round(ok_count / total_itens * 100) if total_itens > 0 else 100,
            },
            "por_tecnico": sorted(por_tec.values(), key=lambda x: -x["divergencias"]),
            "divergencias": divergencias[:50],
        }

    except Exception as e:
        print(f"ERRO auditoria estoque: {e}")
        import traceback; traceback.print_exc()
        return {"erro": str(e), "divergencias": [], "resumo": {}}
