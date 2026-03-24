"""
SAIS — Endpoints de Pontuação por Regras
/api/sais/pontuacao/*

Registrar no main.py:
    from app.routes.sais import pontuacao
    app.include_router(pontuacao.router, prefix="/api/sais/pontuacao", tags=["SAIS Pontuação"])
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Query, BackgroundTasks

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def hoje_brt():
    return (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")


@router.get("/os/{os_id}")
async def get_pontuacao_os(os_id: int):
    """Pontuação detalhada de uma OS — usado no modal da OS."""
    db = get_db()
    row = db.execute(
        "SELECT * FROM sais_os_pontuacao WHERE os_id=?", (os_id,)
    ).fetchone()
    db.close()

    if not row:
        return {
            "os_id": os_id,
            "calculado": False,
            "mensagem": "Pontuação não calculada ainda. Aguarde o próximo ciclo (15 min)."
        }

    r = dict(row)
    pendencias = [p for p in (r.get("pendencias") or "").split(" | ") if p]

    return {
        "calculado":    True,
        "os_id":        r["os_id"],
        "cliente_nome": r["cliente_nome"],
        "tecnico_nome": r["tecnico_nome"],
        "assunto":      r["nome_assunto"],
        "pontos_base":  r["pontos_base"],
        "pontos_final": r["pontos_final"],
        "aprovada":     bool(r["aprovada"]),
        "detalhes": {
            "pen_foto":      r["pen_foto"],
            "pen_app":       r["pen_app"],
            "pen_produto":   r["pen_produto"],
            "pen_descricao": r["pen_descricao"],
            "bonus_tempo":   r["bonus_tempo"],
            "bonus_fibra":   r["bonus_fibra"],
        },
        "evidencias": {
            "total_fotos":   r["total_fotos"],
            "tem_produto":   bool(r["tem_produto"]),
            "tem_comodato":  bool(r["tem_comodato"]),
            "tem_app":       bool(r["tem_app"]),
            "metros_fibra":  r["metros_fibra"],
            "minutos_exec":  r["minutos_exec"],
            "len_descricao": r["len_descricao"],
        },
        "pendencias":   pendencias,
        "calculado_em": r["calculado_em"],
    }


@router.get("/tecnico/{tecnico_id}")
async def get_pontuacao_tecnico(
    tecnico_id: int,
    data: Optional[str] = Query(None)
):
    """Pontuação detalhada de um técnico no dia — com breakdown por OS."""
    db = get_db()
    data = data or hoje_brt()

    rows = db.execute("""
        SELECT p.*, o.data_fechamento
        FROM sais_os_pontuacao p
        LEFT JOIN prod_os_cache o ON o.ixc_os_id = p.os_id
        WHERE p.tecnico_id = ?
          AND DATE(o.data_fechamento, '+3 hours') = ?
        ORDER BY p.pontos_final DESC
    """, (tecnico_id, data)).fetchall()

    meta_row = db.execute("""
        SELECT valor FROM prod_metas
        WHERE tecnico_id=? AND tipo='pontos_dia' AND vigente=1 LIMIT 1
    """, (tecnico_id,)).fetchone()
    meta = int(meta_row["valor"]) if meta_row else 80

    db.close()

    total_pontos  = sum(r["pontos_final"] for r in rows)
    total_os      = len(rows)
    os_perfeitas  = sum(1 for r in rows if not r["pendencias"])
    pct_meta      = round(total_pontos / meta * 100, 1) if meta > 0 else 0
    pct_qualidade = round(os_perfeitas / total_os * 100) if total_os > 0 else 0

    return {
        "tecnico_id":    tecnico_id,
        "data":          data,
        "total_pontos":  total_pontos,
        "meta_dia":      meta,
        "pct_meta":      pct_meta,
        "total_os":      total_os,
        "os_perfeitas":  os_perfeitas,
        "pct_qualidade": pct_qualidade,
        "os_list": [{
            "os_id":        r["os_id"],
            "assunto":      r["nome_assunto"],
            "cliente":      r["cliente_nome"],
            "pontos_base":  r["pontos_base"],
            "pontos_final": r["pontos_final"],
            "pendencias":   [p for p in (r["pendencias"] or "").split(" | ") if p],
            "aprovada":     bool(r["aprovada"]),
        } for r in rows],
    }


@router.get("/ranking")
async def get_ranking_pontuacao(
    data:        Optional[str] = Query(None),
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
):
    """Ranking de técnicos por pontuação real — suporta período."""
    import calendar as _cal
    from datetime import date as _date, timedelta as _td
    db = get_db()
    hoje = hoje_brt()
    di = str(data_inicio) if data_inicio else (str(data) if data else hoje)
    df = str(data_fim)    if data_fim    else (str(data) if data else hoje)
    try:
        _d1 = _date.fromisoformat(di)
        _d2 = _date.fromisoformat(df)
        _mes_inicio = _date(_d1.year, _d1.month, 1)
        _mes_fim    = _date(_d1.year, _d1.month, _cal.monthrange(_d1.year, _d1.month)[1])
        _mes_inteiro = (_d1 == _mes_inicio and _d2 == _mes_fim)
        dias_uteis = sum(1 for i in range((_d2-_d1).days+1) if (_d1+_td(i)).weekday()!=6)
        dias_uteis = max(dias_uteis, 1)
    except:
        _mes_inteiro = False
        dias_uteis = 1
    rows = db.execute("""
        SELECT
            p.tecnico_id,
            t.nome,
            t.meta_dia,
            t.meta_mes,
            SUM(p.pontos_final) AS total_pontos,
            SUM(p.pontos_base)  AS pontos_max,
            COUNT(p.os_id)      AS total_os,
            SUM(CASE WHEN p.pendencias = '' THEN 1 ELSE 0 END) AS os_perfeitas,
            SUM(CASE WHEN p.aprovada = 1 THEN 1 ELSE 0 END)      AS aprovadas
        FROM sais_os_pontuacao p
        LEFT JOIN prod_tecnicos t ON t.ixc_funcionario_id = p.tecnico_id
        LEFT JOIN prod_os_cache o ON o.ixc_os_id = p.os_id
        WHERE DATE(o.data_fechamento, '+3 hours') BETWEEN ? AND ?
        GROUP BY p.tecnico_id
        ORDER BY total_pontos DESC
    """, (di, df)).fetchall()
    ranking = []
    for i, r in enumerate(rows):
        total_os     = r["total_os"] or 1
        meta_dia     = r["meta_dia"] or 80
        meta_mes     = r["meta_mes"] or 1760
        meta_periodo = meta_mes if _mes_inteiro else dias_uteis * meta_dia
        pts          = r["total_pontos"] or 0
        pts_max      = r["pontos_max"] or 1
        ranking.append({
            "posicao":        i + 1,
            "tecnico_id":     r["tecnico_id"],
            "nome":           r["nome"] or "—",
            "total_os":       r["total_os"],
            "total_pontos":   pts,
            "pontos_max":     pts_max,
            "aproveitamento": round(pts / pts_max * 100) if pts_max > 0 else 0,
            "os_perfeitas":   r["os_perfeitas"],
            "pct_qualidade":  round(r["os_perfeitas"] / total_os * 100),
            "meta_dia":       meta_dia,
            "meta_periodo":   meta_periodo,
            "pct_meta":       round(pts / meta_periodo * 100, 1) if meta_periodo > 0 else 0,
        })
    db.close()
    return {"data_inicio": di, "data_fim": df, "ranking": ranking}

@router.get("/resumo-dia")
async def get_resumo_dia(data: Optional[str] = Query(None)):
    """Resumo de pontuação da equipe no dia."""
    db = get_db()
    data = data or hoje_brt()

    row = db.execute("""
        SELECT
            COUNT(p.os_id)                                      AS total_os,
            SUM(p.pontos_final)                                 AS total_pontos,
            SUM(p.pontos_base)                                  AS pontos_max,
            SUM(CASE WHEN p.pendencias = '' THEN 1 ELSE 0 END) AS os_perfeitas,
            SUM(CASE WHEN p.pen_foto < 0     THEN 1 ELSE 0 END) AS pen_foto_count,
            SUM(CASE WHEN p.pen_app < 0      THEN 1 ELSE 0 END) AS pen_app_count,
            SUM(CASE WHEN p.pen_produto < 0  THEN 1 ELSE 0 END) AS pen_prod_count,
            SUM(CASE WHEN p.pen_descricao < 0 THEN 1 ELSE 0 END) AS pen_desc_count
        FROM sais_os_pontuacao p
        LEFT JOIN prod_os_cache o ON o.ixc_os_id = p.os_id
        WHERE DATE(o.data_fechamento, '+3 hours') = ?
    """, (data,)).fetchone()

    db.close()
    r = dict(row)
    pts     = r["total_pontos"] or 0
    pts_max = r["pontos_max"]   or 1

    return {
        "data":            data,
        "total_os":        r["total_os"]     or 0,
        "total_pontos":    pts,
        "pontos_max":      pts_max,
        "aproveitamento":  round(pts / pts_max * 100) if pts_max > 0 else 0,
        "os_perfeitas":    r["os_perfeitas"] or 0,
        "pendencias": {
            "sem_foto":    r["pen_foto_count"]  or 0,
            "sem_app":     r["pen_app_count"]   or 0,
            "sem_produto": r["pen_prod_count"]  or 0,
            "desc_curta":  r["pen_desc_count"]  or 0,
        }
    }


@router.post("/calcular-agora")
async def calcular_agora(background_tasks: BackgroundTasks,
                          data: Optional[str] = Query(None)):
    """Força recálculo imediato das pontuações (roda em background)."""
    from app.engines.regras_engine import rodar_regras
    background_tasks.add_task(rodar_regras, data)
    return {"mensagem": "Cálculo iniciado em background", "data": data or hoje_brt()}
