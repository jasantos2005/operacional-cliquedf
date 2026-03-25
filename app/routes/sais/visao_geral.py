"""
SAIS — Visão Geral
Endpoints para o dashboard principal.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Query

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")


@router.get("/resumo")
async def get_resumo(data: Optional[str] = Query(None)):
    """KPIs principais do dia para o dashboard."""
    db = get_db()
    data = str(data) if data else hoje_brt()

    resumo = dict(db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas,
            SUM(CASE WHEN status='aguardando' THEN 1 ELSE 0 END) AS aguardando,
            SUM(CASE WHEN categoria='servico'  THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN categoria='suporte'  THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN categoria='infra'    THEN 1 ELSE 0 END) AS infra,
            SUM(CASE WHEN categoria='retirada' THEN 1 ELSE 0 END) AS retiradas
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (data,)).fetchone())

    meta_row = db.execute(
        "SELECT valor FROM sais_config WHERE chave='meta_dia'"
    ).fetchone()
    meta = int(meta_row["valor"]) if meta_row else 150

    fins  = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    efic  = round(fins / total * 100, 1) if total > 0 else 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    pontos_row = db.execute("""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS pontos
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p
            ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo = 1
        WHERE o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = ?
    """, (data,)).fetchone()
    total_pontos = pontos_row["pontos"] or 0 if pontos_row else 0

    tecs_ativos = db.execute("""
        SELECT COUNT(DISTINCT tecnico_id) as total
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') = ?
    """, (data,)).fetchone()

    alertas = db.execute(
        "SELECT COUNT(*) as total FROM sais_alertas WHERE lido=0"
    ).fetchone()

    audits = db.execute(
        "SELECT COUNT(*) as total FROM sais_auditorias WHERE criticidade IN ('critica','alta') AND resolvida=0"
    ).fetchone()

    db.close()

    return {
        "data": data,
        "resumo": resumo,
        "meta_dia": meta,
        "eficiencia": efic,
        "meta_percentual": pct_meta,
        "tecnicos_ativos": tecs_ativos["total"] if tecs_ativos else 0,
        "alertas_pendentes":   alertas["total"] if alertas else 0,
        "auditorias_criticas": audits["total"] if audits else 0,
        "total_pontos":        total_pontos,
    }


@router.get("/eventos-recentes")
async def get_eventos_recentes(limit: int = Query(20)):
    """Ultimas OS finalizadas e eventos do dia."""
    db = get_db()
    data = hoje_brt()

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura, o.data_fechamento,
            t.nome AS tecnico,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
        ORDER BY COALESCE(o.data_fechamento, o.data_abertura) DESC
        LIMIT ?
    """, (data, limit)).fetchall()
    db.close()

    return {"eventos": [dict(r) for r in rows]}


@router.get("/os-finalizadas")
async def get_os_finalizadas(
    data:        Optional[str] = Query(None),
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
    tecnico_id:  Optional[str] = Query(None),
    categoria:   Optional[str] = Query(None),
):
    """Lista de OS finalizadas com suporte a período (data_inicio/data_fim).
    Retrocompatível com ?data= (1 dia).
    Filtros opcionais: tecnico_id (ixc_funcionario_id, aceita múltiplos separados por vírgula),
    categoria (servico|suporte|infra|retirada).
    """
    db = get_db()
    hoje = hoje_brt()

    # Resolve período
    if data_inicio or data_fim:
        di = str(data_inicio) if data_inicio else hoje
        df = str(data_fim)    if data_fim    else hoje
    else:
        dia = str(data) if data else hoje
        di = df = dia

    # Monta WHERE dinâmico
    where  = [
        "o.status = 'finalizada'",
        "DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') BETWEEN ? AND ?",
    ]
    params = [di, df]

    if tecnico_id:
        ixc_ids = [int(x) for x in tecnico_id.split(",") if x.strip().isdigit()]
        if ixc_ids:
            ph = ",".join("?" * len(ixc_ids))
            tec_rows = db.execute(
                f"SELECT id FROM prod_tecnicos WHERE ixc_funcionario_id IN ({ph})", ixc_ids
            ).fetchall()
            tec_internos = [r["id"] for r in tec_rows]
            if tec_internos:
                ph2 = ",".join("?" * len(tec_internos))
                where.append(f"o.tecnico_id IN ({ph2})")
                params.extend(tec_internos)
            else:
                where.append("1=0")

    if categoria:
        cats = [c.strip() for c in categoria.split(",") if c.strip()]
        if cats:
            ph = ",".join("?" * len(cats))
            where.append(f"o.categoria IN ({ph})")
            params.extend(cats)

    where_sql = " AND ".join(where)

    rows = db.execute(f"""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura, o.data_fechamento,
            t.nome AS tecnico_nome,
            t.ixc_funcionario_id AS tecnico_id,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS nome_assunto,
            p.pontos_final, p.pontos_base,
            p.pen_foto, p.pen_app, p.pen_produto, p.pen_descricao,
            p.bonus_tempo, p.bonus_fibra,
            p.total_fotos, p.tem_produto, p.tem_comodato, p.tem_app,
            p.metros_fibra, p.minutos_exec, p.len_descricao,
            p.pendencias, p.aprovada,
            s.horas_sla
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t      ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a      ON a.id = o.ixc_assunto_id
        LEFT JOIN sais_os_pontuacao p  ON p.os_id = o.ixc_os_id
        LEFT JOIN sais_sla_config s    ON s.assunto_id = o.ixc_assunto_id
        WHERE {where_sql}
        ORDER BY COALESCE(o.data_fechamento, o.data_abertura) DESC
    """, params).fetchall()
    db.close()

    result = []
    for r in rows:
        d = dict(r)
        tempo_min = None
        if d.get("data_abertura") and d.get("data_fechamento"):
            try:
                from datetime import datetime as dt
                fmt = "%Y-%m-%d %H:%M:%S"
                ab = dt.strptime(d["data_abertura"][:19], fmt)
                fe = dt.strptime(d["data_fechamento"][:19], fmt)
                tempo_min = round((fe - ab).total_seconds() / 60)
            except:
                pass

        horas_sla  = d.get("horas_sla") or 4.0
        pct_sla    = round(tempo_min / (horas_sla * 60) * 100) if tempo_min else None
        status_sla = None
        if pct_sla is not None:
            status_sla = "no_prazo" if pct_sla <= 80 else "em_risco" if pct_sla <= 100 else "estourado"

        pends = [p for p in (d.get("pendencias") or "").split(" | ") if p]

        result.append({
            "os_id":        d["ixc_os_id"],
            "tecnico_nome": d["tecnico_nome"] or "---",
            "tecnico_id":   d["tecnico_id"],
            "nome_assunto": d["nome_assunto"] or "---",
            "categoria":    d["categoria"] or "---",
            "data_fechamento": d["data_fechamento"],
            "tempo_min":    tempo_min,
            "sla": {
                "horas_previstas": horas_sla,
                "pct":             pct_sla,
                "status":          status_sla,
            },
            "pontuacao": {
                "calculado":      d["pontos_final"] is not None,
                "pontos_base":    d["pontos_base"]   or 0,
                "pontos_final":   d["pontos_final"]  or 0,
                "aproveitamento": round(d["pontos_final"] / d["pontos_base"] * 100)
                                  if d["pontos_base"] and d["pontos_final"] is not None else 0,
                "pen_foto":      d["pen_foto"]      or 0,
                "pen_app":       d["pen_app"]       or 0,
                "pen_produto":   d["pen_produto"]   or 0,
                "pen_descricao": d["pen_descricao"] or 0,
                "bonus_tempo":   d["bonus_tempo"]   or 0,
                "bonus_fibra":   d["bonus_fibra"]   or 0,
            },
            "evidencias": {
                "total_fotos":   d["total_fotos"]  or 0,
                "tem_produto":   bool(d["tem_produto"]),
                "tem_comodato":  bool(d["tem_comodato"]),
                "tem_app":       bool(d["tem_app"]),
                "metros_fibra":  d["metros_fibra"]  or 0,
                "minutos_exec":  d["minutos_exec"]  or 0,
                "len_descricao": d["len_descricao"] or 0,
                "aprovada":      bool(d["aprovada"]),
            },
            "pendencias": pends,
        })

    return {"data_inicio": di, "data_fim": df, "total": len(result), "os": result}


@router.get("/filtros-opcoes")
async def get_filtros_opcoes(
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
):
    """Retorna opcoes para os filtros ordenadas por movimentacao no periodo."""
    import os as _os, pymysql
    hoje = hoje_brt()
    di = str(data_inicio) if data_inicio else hoje
    df = str(data_fim)    if data_fim    else hoje
    db = get_db()

    tecs_mov = db.execute("""
        SELECT t.ixc_funcionario_id AS id, t.nome, COUNT(o.id) AS total
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o ON o.tecnico_id = t.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours') BETWEEN ? AND ?
        WHERE t.ativo = 1
        GROUP BY t.id ORDER BY total DESC, t.nome
    """, (di, df)).fetchall()

    assuntos_mov = db.execute("""
        SELECT a.id, a.assunto, COUNT(o.id) AS total
        FROM prod_assuntos a
        LEFT JOIN prod_os_cache o ON o.ixc_assunto_id = a.id
            AND DATE(COALESCE(o.data_fechamento,o.data_agenda,o.data_abertura),'+3 hours') BETWEEN ? AND ?
        GROUP BY a.id ORDER BY total DESC, a.assunto LIMIT 100
    """, (di, df)).fetchall()

    cats_mov = db.execute("""
        SELECT categoria, COUNT(*) AS total FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento,data_agenda,data_abertura),'+3 hours') BETWEEN ? AND ?
          AND categoria IS NOT NULL
        GROUP BY categoria ORDER BY total DESC
    """, (di, df)).fetchall()
    cats_dict = {r["categoria"]: r["total"] for r in cats_mov}
    db.close()

    categorias = sorted([
        {"id": "servico",  "nome": "Servico",  "total": cats_dict.get("servico",  0)},
        {"id": "suporte",  "nome": "Suporte",  "total": cats_dict.get("suporte",  0)},
        {"id": "infra",    "nome": "Infra",    "total": cats_dict.get("infra",    0)},
        {"id": "retirada", "nome": "Retirada", "total": cats_dict.get("retirada", 0)},
    ], key=lambda x: -x["total"])
    # Restaura nomes com acento para exibicao
    nomes_cat = {"servico": "Servico", "suporte": "Suporte", "infra": "Infra", "retirada": "Retirada"}

    bairros = []
    cidades = []
    concentradores = []
    try:
        ixc = pymysql.connect(
            host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
            user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
            database=_os.getenv("DB_NAME"),
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=8,
        )
        with ixc.cursor() as cur:
            cur.execute("""
                SELECT cc.bairro, COUNT(DISTINCT o.id) AS total
                FROM su_oss_chamado o
                JOIN cliente_contrato cc ON cc.id_cliente = o.id_cliente
                WHERE DATE(CONVERT_TZ(o.data_abertura,'+00:00','-03:00')) BETWEEN %s AND %s
                  AND cc.bairro IS NOT NULL AND cc.bairro != ''
                GROUP BY cc.bairro ORDER BY total DESC, cc.bairro LIMIT 80
            """, (di, df))
            bairros = [{"nome": r["bairro"], "total": r["total"]} for r in cur.fetchall() if r["bairro"]]

            cur.execute("""
                SELECT cd.nome AS cidade_nome, COUNT(DISTINCT o.id) AS total
                FROM su_oss_chamado o
                JOIN cliente_contrato cc ON cc.id_cliente = o.id_cliente
                JOIN cidade cd ON cd.id = cc.cidade
                WHERE DATE(CONVERT_TZ(o.data_abertura,'+00:00','-03:00')) BETWEEN %s AND %s
                  AND cd.nome IS NOT NULL AND cd.nome != ''
                GROUP BY cd.nome ORDER BY total DESC, cd.nome LIMIT 60
            """, (di, df))
            cidades = [{"nome": r["cidade_nome"], "total": r["total"]} for r in cur.fetchall() if r["cidade_nome"]]

            cur.execute("""
                SELECT rp.pop AS nome, COUNT(DISTINCT o.id) AS total
                FROM su_oss_chamado o
                JOIN cliente_contrato cc ON cc.id_cliente = o.id_cliente
                JOIN radpop_radio_cliente_fibra rf ON rf.id_contrato = cc.id
                JOIN radpop rp ON rp.id = rf.id_transmissor
                WHERE DATE(CONVERT_TZ(o.data_abertura,'+00:00','-03:00')) BETWEEN %s AND %s
                  AND rp.pop IS NOT NULL AND rp.pop != ''
                GROUP BY rp.pop ORDER BY total DESC LIMIT 30
            """, (di, df))
            concentradores = [{"nome": r["nome"], "total": r["total"]} for r in cur.fetchall() if r["nome"]]
        ixc.close()
    except Exception as e:
        print(f"ERRO filtros-opcoes IXC: {e}")
        import traceback; traceback.print_exc()

    return {
        "periodo":    {"data_inicio": di, "data_fim": df},
        "tecnicos":   [{"id": t["id"], "nome": t["nome"], "total": t["total"]} for t in tecs_mov],
        "categorias": categorias,
        "assuntos":   [{"id": a["id"], "nome": a["assunto"], "total": a["total"]} for a in assuntos_mov],
        "bairros":    bairros,
        "cidades":    cidades,
        "concentradores": concentradores,
    }


def dias_uteis_periodo(di: str, df: str) -> int:
    """Conta dias úteis (seg a sáb, excluindo domingos) entre di e df inclusive."""
    from datetime import date as _date, timedelta as _td
    try:
        d_ini = _date.fromisoformat(di)
        d_fim = _date.fromisoformat(df)
        total = 0
        d = d_ini
        while d <= d_fim:
            if d.weekday() != 6:  # 6 = domingo
                total += 1
            d += _td(days=1)
        return max(total, 1)
    except:
        return 1


@router.get("/resumo-filtrado")
async def get_resumo_filtrado(
    data_inicio:  Optional[str] = Query(None),
    data_fim:     Optional[str] = Query(None),
    tecnico_id:   Optional[str] = Query(None),
    categoria:    Optional[str] = Query(None),
    bairro:       Optional[str] = Query(None),
    assunto_id:   Optional[str] = Query(None),
    cidade:       Optional[str] = Query(None),
    concentrador: Optional[str] = Query(None),
):
    """Resumo do dashboard com filtros aplicados."""
    import os as _os, pymysql
    db = get_db()
    hoje = hoje_brt()

    # Converte TODOS os parametros Query para string
    di           = str(data_inicio)  if data_inicio  else hoje
    df           = str(data_fim)     if data_fim     else hoje
    tecnico_id   = str(tecnico_id)   if tecnico_id   else None
    categoria    = str(categoria)    if categoria    else None
    bairro       = str(bairro)       if bairro       else None
    assunto_id   = str(assunto_id)   if assunto_id   else None
    cidade       = str(cidade)       if cidade       else None
    concentrador = str(concentrador) if concentrador else None

    # Helper: busca os_ids no IXC com tratamento de erro
    def _busca_ixc(query, lista):
        try:
            conn = pymysql.connect(
                host=_os.getenv("DB_HOST"), port=int(_os.getenv("DB_PORT", 3306)),
                user=_os.getenv("DB_USER"), password=_os.getenv("DB_PASS"),
                database=_os.getenv("DB_NAME"),
                cursorclass=pymysql.cursors.DictCursor, connect_timeout=8
            )
            with conn.cursor() as cur:
                cur.execute(query, lista)
                ids = [r["id"] for r in cur.fetchall()]
            conn.close()
            return ids  # lista vazia = sem resultados (sem erro)
        except Exception as e:
            print(f"ERRO IXC filtro: {e}")
            return None  # None = erro de conexao

    # Monta clausulas WHERE SQLite
    where  = ["DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') BETWEEN ? AND ?"]
    params = [di, df]

    # Tecnico
    if tecnico_id:
        ixc_ids = [int(x) for x in tecnico_id.split(",") if x.strip().isdigit()]
        if ixc_ids:
            ph = ",".join("?" * len(ixc_ids))
            rows = db.execute(
                f"SELECT id FROM prod_tecnicos WHERE ixc_funcionario_id IN ({ph})", ixc_ids
            ).fetchall()
            tec_ids_internos = [r["id"] for r in rows]
            if tec_ids_internos:
                ph2 = ",".join("?" * len(tec_ids_internos))
                where.append(f"tecnico_id IN ({ph2})")
                params.extend(tec_ids_internos)

    # Categoria
    if categoria:
        cats = [c.strip() for c in categoria.split(",") if c.strip()]
        if cats:
            ph = ",".join("?" * len(cats))
            where.append(f"categoria IN ({ph})")
            params.extend(cats)

    # Assunto
    if assunto_id:
        aids = [int(x) for x in assunto_id.split(",") if x.strip().isdigit()]
        if aids:
            ph = ",".join("?" * len(aids))
            where.append(f"ixc_assunto_id IN ({ph})")
            params.extend(aids)

    # Cidade -- via cliente_contrato -> cidade (nome real)
    if cidade:
        cids = [c.strip() for c in cidade.split(",") if c.strip()]
        ph   = ",".join(["%s"] * len(cids))
        ids  = _busca_ixc(
            "SELECT DISTINCT o.id AS id FROM su_oss_chamado o "
            "JOIN cliente_contrato cc ON cc.id_cliente=o.id_cliente "
            "JOIN cidade cd ON cd.id=cc.cidade "
            f"WHERE cd.nome IN ({ph}) AND o.data_abertura>=DATE_SUB(NOW(),INTERVAL 90 DAY)",
            cids
        )
        if ids:
            where.append(f"ixc_os_id IN ({','.join(['?']*len(ids))})")
            params.extend(ids)
        elif ids is not None:
            where.append("1=0")

    # Bairro -- via cliente_contrato
    if bairro:
        blist = [b.strip() for b in bairro.split(",") if b.strip()]
        ph    = ",".join(["%s"] * len(blist))
        ids   = _busca_ixc(
            "SELECT DISTINCT o.id AS id FROM su_oss_chamado o "
            "JOIN cliente_contrato cc ON cc.id_cliente=o.id_cliente "
            f"WHERE cc.bairro IN ({ph}) AND o.data_abertura>=DATE_SUB(NOW(),INTERVAL 90 DAY)",
            blist
        )
        if ids:
            where.append(f"ixc_os_id IN ({','.join(['?']*len(ids))})")
            params.extend(ids)
        elif ids is not None:
            where.append("1=0")

    # Concentrador -- via radpop
    if concentrador:
        clist = [c.strip() for c in concentrador.split(",") if c.strip()]
        ph    = ",".join(["%s"] * len(clist))
        ids   = _busca_ixc(
            "SELECT DISTINCT o.id AS id FROM su_oss_chamado o "
            "JOIN cliente_contrato cc ON cc.id_cliente=o.id_cliente "
            "JOIN radpop_radio_cliente_fibra rf ON rf.id_contrato=cc.id "
            "JOIN radpop rp ON rp.id=rf.id_transmissor "
            f"WHERE rp.pop IN ({ph}) AND o.data_abertura>=DATE_SUB(NOW(),INTERVAL 90 DAY)",
            clist
        )
        if ids:
            where.append(f"ixc_os_id IN ({','.join(['?']*len(ids))})")
            params.extend(ids)
        elif ids is not None:
            where.append("1=0")

    # Monta SQL final
    where_sql = " AND ".join(where)

    resumo = dict(db.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas,
            SUM(CASE WHEN categoria='servico'  THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN categoria='suporte'  THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN categoria='infra'    THEN 1 ELSE 0 END) AS infra,
            SUM(CASE WHEN categoria='retirada' THEN 1 ELSE 0 END) AS retiradas
        FROM prod_os_cache
        WHERE {where_sql}
    """, params).fetchone())

    meta_row = db.execute("SELECT valor FROM sais_config WHERE chave='meta_dia'").fetchone()
    meta     = int(meta_row["valor"]) if meta_row else 150
    fins     = resumo["finalizadas"] or 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0

    ranking_rows = db.execute(f"""
        SELECT
            t.nome, t.ixc_funcionario_id AS tecnico_id,
            t.meta_dia AS meta_tecnico,
            COUNT(*) AS total_os,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            COALESCE(SUM(p.pontos_final), 0) AS pontos_total
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN sais_os_pontuacao p ON p.os_id = o.ixc_os_id
        WHERE {where_sql}
        GROUP BY o.tecnico_id
        ORDER BY finalizadas DESC
        LIMIT 20
    """, params).fetchall()

    pontos_row = db.execute(f"""
        SELECT SUM(COALESCE(p.pontuacao, 0)) AS pontos
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id AND p.ativo=1
        WHERE o.status='finalizada' AND {where_sql}
    """, params).fetchone()

    tecs_ativos = db.execute(f"""
        SELECT COUNT(DISTINCT tecnico_id) as total
        FROM prod_os_cache WHERE {where_sql}
    """, params).fetchone()

    db.close()

    import calendar as _cal
    from datetime import date as _date
    _d1 = _date.fromisoformat(di)
    _d2 = _date.fromisoformat(df) if True else None
    _mes_inicio = _date(_d1.year, _d1.month, 1)
    _mes_fim    = _date(_d1.year, _d1.month, _cal.monthrange(_d1.year, _d1.month)[1])
    _mes_inteiro = (_d1 == _mes_inicio and _d2 == _mes_fim)
    dias_uteis   = dias_uteis_periodo(di, df)
    tecs_ativos_n = tecs_ativos["total"] if tecs_ativos else 1
    # Meta da equipe = 80pts × técnicos ativos × dias úteis (mês inteiro = 1760 × tecs)
    meta_periodo = (1760 * tecs_ativos_n) if _mes_inteiro else (dias_uteis * 80 * tecs_ativos_n)
    total_pts   = pontos_row["pontos"] or 0 if pontos_row else 0
    pct_pts     = round(total_pts / meta_periodo * 100) if meta_periodo > 0 else 0

    return {
        "data_inicio":    di,
        "data_fim":       df,
        "resumo":         resumo,
        "meta_dia":       meta,
        "meta_periodo":   meta_periodo,
        "dias_uteis":     dias_uteis,
        "meta_percentual": pct_pts,
        "tecnicos_ativos": tecs_ativos["total"] if tecs_ativos else 0,
        "total_pontos":   total_pts,
        "ranking": [{
            "nome":       r["nome"] or "---",
            "tecnico_id": r["tecnico_id"],
            "total_os":   r["total_os"],
            "finalizadas": r["finalizadas"],
            "pontos":     r["pontos_total"] or 0,
            "pct_meta":   round((r["pontos_total"] or 0) / (dias_uteis_periodo(di, df) * (r["meta_tecnico"] or 80)) * 100),
            "score":      r["pontos_total"] or 0,
            "eficiencia": round((r["finalizadas"] or 0) / r["total_os"] * 100) if r["total_os"] > 0 else 0,
        } for r in ranking_rows],
    }


@router.get("/alertas")
async def get_alertas(limit: int = Query(20), apenas_nao_lidos: bool = Query(False)):
    """Alertas do sistema."""
    db = get_db()
    where = "WHERE lido=0" if apenas_nao_lidos else ""
    rows = db.execute(f"""
        SELECT a.*, t.nome AS tecnico_nome
        FROM sais_alertas a
        LEFT JOIN prod_tecnicos t ON t.id = a.tecnico_id
        {where}
        ORDER BY
            CASE criticidade WHEN 'critico' THEN 1 WHEN 'aviso' THEN 2 ELSE 3 END,
            criado_em DESC
        LIMIT ?
    """, (limit,)).fetchall()
    db.close()
    return {"alertas": [dict(r) for r in rows]}


@router.post("/alertas/{alerta_id}/lido")
async def marcar_lido(alerta_id: int):
    """Marca alerta como lido."""
    db = get_db()
    db.execute("UPDATE sais_alertas SET lido=1 WHERE id=?", (alerta_id,))
    db.commit()
    db.close()
    return {"ok": True}


@router.get("/tecnico/{tecnico_id}")
async def get_modal_tecnico(
    tecnico_id:  int,
    data:        Optional[str] = Query(None),
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
):
    """Dados completos para o modal do tecnico com suporte a periodo."""
    db = get_db()
    hoje = hoje_brt()
    di = str(data_inicio) if data_inicio else (str(data) if data else hoje)
    df = str(data_fim)    if data_fim    else (str(data) if data else hoje)

    tecnico = db.execute(
        "SELECT * FROM prod_tecnicos WHERE id=?", (tecnico_id,)
    ).fetchone()
    if not tecnico:
        db.close()
        return {"erro": "Tecnico nao encontrado"}

    os_hoje = dict(db.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status IN ('execucao','aberta') THEN 1 ELSE 0 END) AS em_campo,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND DATE(COALESCE(data_fechamento, data_agenda, data_abertura), '+3 hours') BETWEEN ? AND ?
    """, (tecnico_id, di, df)).fetchone())

    from app.engines.score_engine import calcular_pontos_tecnico as calcular_score_tecnico
    score = calcular_score_tecnico(tecnico_id, di)

    os_recentes = db.execute("""
        SELECT o.ixc_os_id, o.status, o.categoria,
               o.data_abertura, o.data_fechamento, o.data_agenda,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.tecnico_id=?
          AND DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') BETWEEN ? AND ?
        ORDER BY COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura) DESC
        LIMIT 20
    """, (tecnico_id, di, df)).fetchall()

    amanha = (datetime.strptime(df, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    agenda = db.execute("""
        SELECT o.ixc_os_id, o.data_agenda, o.status,
               COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.tecnico_id=?
          AND o.status = 'agendada'
          AND DATE(o.data_agenda, '+3 hours') BETWEEN ? AND ?
        ORDER BY o.data_agenda
    """, (tecnico_id, df, amanha)).fetchall()

    auditorias = db.execute("""
        SELECT tipo, subtipo, criticidade, descricao, criado_em
        FROM sais_auditorias
        WHERE tecnico_id=? AND resolvida=0
        ORDER BY criado_em DESC LIMIT 10
    """, (tecnico_id,)).fetchall()

    from app.engines.score_engine import historico_tecnico
    historico = historico_tecnico(tecnico_id, dias=7)

    db.close()

    return {
        "tecnico": dict(tecnico),
        "hoje": os_hoje,
        "score": score,
        "os_recentes": [dict(r) for r in os_recentes],
        "agenda": [dict(r) for r in agenda],
        "auditorias": [dict(r) for r in auditorias],
        "historico_7d": historico,
    }


@router.get("/os/{os_id}")
async def get_modal_os(os_id: int):
    """Dados completos para o modal de uma OS."""
    db = get_db()

    os = db.execute("""
        SELECT
            o.*,
            t.nome AS tecnico_nome,
            t.meta_dia AS tecnico_meta,
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS nome_assunto
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE o.ixc_os_id = ?
    """, (os_id,)).fetchone()

    if not os:
        db.close()
        return {"erro": "OS nao encontrada"}

    os_dict = dict(os)

    from app.engines.sla_engine import calcular_sla
    sla = calcular_sla(os_dict)

    auditorias = db.execute("""
        SELECT tipo, subtipo, criticidade, descricao, valor_detectado, valor_esperado, criado_em
        FROM sais_auditorias
        WHERE os_id = ?
        ORDER BY criado_em DESC
    """, (os_id,)).fetchall()

    db.close()

    return {
        "os": os_dict,
        "sla": sla,
        "auditorias": [dict(r) for r in auditorias],
    }


@router.post("/sync")
async def sync_manual(background_tasks: BackgroundTasks):
    """Executa sync completo: OS + pontuação do dia."""
    import subprocess, sys
    try:
        venv_python = sys.executable
        subprocess.Popen(
            [venv_python, "-m", "app.bootstrap.cron_sync_ixc", "--full"],
            cwd="/opt/automacoes/cliquedf/operacional"
        )
        return {"ok": True, "msg": "Sync iniciado em background"}
    except Exception as e:
        return {"ok": False, "erro": str(e)}
