"""SAIS — Produtividade"""
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


@router.get("/ranking")
async def get_ranking(data: Optional[str] = Query(None), limit: int = Query(20)):
    """Ranking de produtividade do dia."""
    from app.engines.score_engine import ranking_dia, calcular_pontos_tecnico
    data = data or hoje_brt()
    ranking = ranking_dia(data, limit)
    return {"data": data, "ranking": ranking}


@router.get("/tecnico/{tecnico_id}")
async def get_produtividade_tecnico(
    tecnico_id: int,
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
):
    """Produtividade detalhada de um técnico."""
    from app.engines.score_engine import historico_tecnico, calcular_score_tecnico
    db = get_db()
    data = hoje_brt()

    tecnico = db.execute(
        "SELECT id, nome, meta_dia FROM prod_tecnicos WHERE id=?", (tecnico_id,)
    ).fetchone()
    if not tecnico:
        db.close()
        return {"erro": "Técnico não encontrado"}

    # Score do dia
    score_hoje = calcular_score_tecnico(tecnico_id, data)

    # Histórico 30 dias
    historico = historico_tecnico(tecnico_id, dias=30)

    # Por categoria no período
    data_ini = data_inicio or (datetime.strptime(data, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    data_fim = data_fim or data

    cats = db.execute("""
        SELECT categoria, COUNT(*) as total,
               SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) as finalizadas
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND DATE(COALESCE(data_fechamento, data_abertura), '+3 hours') BETWEEN ? AND ?
        GROUP BY categoria
    """, (tecnico_id, data_ini, data_fim)).fetchall()

    # Tempo médio por categoria
    tempo_medio = db.execute("""
        SELECT categoria,
               ROUND(AVG((julianday(data_fechamento) - julianday(data_abertura)) * 24), 2) AS horas_media
        FROM prod_os_cache
        WHERE tecnico_id=?
          AND status='finalizada'
          AND data_fechamento IS NOT NULL
          AND data_fechamento NOT LIKE '0000%'
          AND DATE(data_fechamento, '+3 hours') BETWEEN ? AND ?
        GROUP BY categoria
    """, (tecnico_id, data_ini, data_fim)).fetchall()

    db.close()

    # Pontuação do dia
    pontos_hoje = calcular_pontos_tecnico(tecnico_id, data)

    return {
        "tecnico": dict(tecnico),
        "score_hoje": score_hoje,
        "pontos_hoje": pontos_hoje,
        "historico_30d": historico,
        "por_categoria": [dict(r) for r in cats],
        "tempo_medio": [dict(r) for r in tempo_medio],
    }


@router.get("/por-assunto")
async def get_produtividade_assunto(data: Optional[str] = Query(None)):
    """Produtividade agrupada por tipo de OS."""
    db = get_db()
    data = data or hoje_brt()

    rows = db.execute("""
        SELECT
            COALESCE(a.assunto, 'Assunto ' || o.ixc_assunto_id) AS assunto,
            o.categoria,
            COUNT(*) AS total,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            ROUND(AVG(CASE WHEN o.status='finalizada' AND o.data_fechamento NOT LIKE '0000%'
                THEN (julianday(o.data_fechamento) - julianday(o.data_abertura)) * 60
                END), 0) AS minutos_medio
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
        GROUP BY o.ixc_assunto_id
        ORDER BY total DESC
    """, (data,)).fetchall()
    db.close()

    return {"data": data, "por_assunto": [dict(r) for r in rows]}


@router.get("/auditoria/{tecnico_id}")
async def get_auditoria_tecnico(
    tecnico_id: int,
    data_inicio: Optional[str] = Query(None),
    data_fim:    Optional[str] = Query(None),
):
    """Auditoria individual do técnico — cada OS com pontuação detalhada."""
    db = get_db()
    hoje = hoje_brt()
    di = str(data_inicio) if data_inicio else hoje
    df = str(data_fim)    if data_fim    else hoje

    tecnico = db.execute(
        "SELECT id, nome, ixc_funcionario_id, meta_dia FROM prod_tecnicos WHERE id=?",
        (tecnico_id,)
    ).fetchone()
    if not tecnico:
        db.close()
        return {"erro": "Técnico não encontrado"}

    # Dias úteis (seg-sáb) no período
    from datetime import date as _date, timedelta as _td
    try:
        d1 = _date.fromisoformat(di)
        d2 = _date.fromisoformat(df)
        dias_uteis = sum(1 for i in range((d2-d1).days+1) if (d1+_td(i)).weekday()!=6)
        dias_uteis = max(dias_uteis, 1)
    except:
        dias_uteis = 1

    meta_periodo = dias_uteis * (tecnico["meta_dia"] or 80)

    # OS do período
    os_rows = db.execute("""
        SELECT
            o.ixc_os_id, o.status, o.categoria,
            o.data_abertura, o.data_fechamento,
            COALESCE(a.assunto, 'Assunto '||o.ixc_assunto_id) AS nome_assunto,
            o.ixc_assunto_id,
            p.pontos_base, p.pontos_final,
            p.pen_foto, p.pen_app, p.pen_produto, p.pen_descricao,
            p.bonus_tempo, p.bonus_fibra,
            p.total_fotos, p.tem_produto, p.tem_comodato, p.tem_app,
            p.pendencias, p.aprovada,
            ap.pontuacao AS pontuacao_assunto
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos a ON a.id = o.ixc_assunto_id
        LEFT JOIN sais_os_pontuacao p ON p.os_id = o.ixc_os_id
        LEFT JOIN prod_assuntos_pontuacao ap ON ap.id_assunto_ixc = o.ixc_assunto_id AND ap.ativo=1
        WHERE o.tecnico_id = ?
          AND o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') BETWEEN ? AND ?
        ORDER BY o.data_fechamento DESC
    """, (tecnico_id, di, df)).fetchall()

    db.close()

    # Monta lista de OS com penalidades detalhadas
    os_list = []
    total_pontos_ganhos  = 0
    total_pontos_perdidos = 0
    total_pontos_bonus   = 0
    sem_regra = 0

    for r in os_rows:
        d = dict(r)

        # Penalidades em lista legível
        pens = []
        if (d["pen_foto"] or 0) < 0:
            pens.append({"motivo": "Sem foto(s)", "valor": d["pen_foto"]})
        if (d["pen_app"] or 0) < 0:
            pens.append({"motivo": "Sem deslocamento/execução no app", "valor": d["pen_app"]})
        if (d["pen_produto"] or 0) < 0:
            pens.append({"motivo": "Produto não registrado", "valor": d["pen_produto"]})
        if (d["pen_descricao"] or 0) < 0:
            pens.append({"motivo": "Descrição insuficiente", "valor": d["pen_descricao"]})

        bonus = []
        if (d["bonus_tempo"] or 0) > 0:
            bonus.append({"motivo": "Bônus tempo", "valor": d["bonus_tempo"]})
        elif (d["bonus_tempo"] or 0) < 0:
            pens.append({"motivo": "Tempo muito curto", "valor": d["bonus_tempo"]})
        if (d["bonus_fibra"] or 0) > 0:
            bonus.append({"motivo": "Bônus fibra", "valor": d["bonus_fibra"]})

        calculado = d["pontos_final"] is not None
        if not calculado:
            sem_regra += 1

        pontos_final  = d["pontos_final"] or 0
        pontos_base   = d["pontos_base"]  or 0
        perdidos      = pontos_base - pontos_final
        bonus_total   = sum(b["valor"] for b in bonus)

        total_pontos_ganhos   += pontos_final
        total_pontos_perdidos += max(perdidos, 0)
        total_pontos_bonus    += bonus_total

        os_list.append({
            "os_id":          d["ixc_os_id"],
            "data":           (d["data_fechamento"] or "")[:10],
            "categoria":      d["categoria"] or "---",
            "nome_assunto":   d["nome_assunto"],
            "assunto_id":     d["ixc_assunto_id"],
            "pontuacao_assunto": d["pontuacao_assunto"] or 0,
            "calculado":      calculado,
            "pontos_base":    pontos_base,
            "pontos_final":   pontos_final,
            "perdidos":       max(perdidos, 0),
            "penalidades":    pens,
            "bonus":          bonus,
            "evidencias": {
                "fotos":      d["total_fotos"] or 0,
                "tem_app":    bool(d["tem_app"]),
                "tem_produto":bool(d["tem_produto"]),
                "tem_comodato":bool(d["tem_comodato"]),
                "aprovada":   bool(d["aprovada"]),
            },
        })

    pct_meta = round(total_pontos_ganhos / meta_periodo * 100) if meta_periodo > 0 else 0

    return {
        "tecnico":       dict(tecnico),
        "periodo":       {"data_inicio": di, "data_fim": df, "dias_uteis": dias_uteis},
        "resumo": {
            "total_os":        len(os_list),
            "pontos_ganhos":   total_pontos_ganhos,
            "pontos_perdidos": total_pontos_perdidos,
            "bonus_total":     total_pontos_bonus,
            "sem_regra":       sem_regra,
            "meta_periodo":    meta_periodo,
            "pct_meta":        pct_meta,
        },
        "os": os_list,
    }


@router.patch("/auditoria/os/{os_id}")
async def editar_pontuacao_os(
    os_id: int,
    pontos_override: Optional[int]  = Query(None),
    obs_manual:      Optional[str]  = Query(None),
    revisada:        Optional[int]  = Query(None),
    revisado_por:    Optional[str]  = Query(None),
):
    """Edição manual de pontuação de uma OS na auditoria."""
    db = get_db()

    # Verifica se OS existe na pontuacao
    row = db.execute("SELECT os_id FROM sais_os_pontuacao WHERE os_id=?", (os_id,)).fetchone()
    if not row:
        # Cria registro mínimo se não existir
        os_row = db.execute("SELECT id, tecnico_id, ixc_assunto_id FROM prod_os_cache WHERE ixc_os_id=?", (os_id,)).fetchone()
        if not os_row:
            db.close()
            return {"erro": "OS não encontrada"}
        db.execute(
            "INSERT OR IGNORE INTO sais_os_pontuacao (os_id, tecnico_id, assunto_id) VALUES (?,?,?)",
            (os_id, os_row["tecnico_id"], os_row["ixc_assunto_id"])
        )
        db.commit()

    agora = (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d %H:%M:%S")
    campos = []
    valores = []

    if pontos_override is not None:
        campos.append("pontos_override=?")
        valores.append(pontos_override)
        # Atualiza pontos_final com override
        campos.append("pontos_final=?")
        valores.append(pontos_override)

    if obs_manual is not None:
        campos.append("obs_manual=?")
        valores.append(obs_manual)

    if revisada is not None:
        campos.append("revisada=?")
        valores.append(revisada)
        campos.append("revisado_em=?")
        valores.append(agora)

    if revisado_por is not None:
        campos.append("revisado_por=?")
        valores.append(revisado_por)

    if not campos:
        db.close()
        return {"erro": "Nenhum campo para atualizar"}

    valores.append(os_id)
    db.execute(f"UPDATE sais_os_pontuacao SET {', '.join(campos)} WHERE os_id=?", valores)
    db.commit()

    updated = dict(db.execute(
        "SELECT os_id, pontos_final, pontos_override, obs_manual, revisada, revisado_por, revisado_em FROM sais_os_pontuacao WHERE os_id=?",
        (os_id,)
    ).fetchone())
    db.close()
    return {"ok": True, "os": updated}
