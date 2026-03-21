"""
Dashboard V2 - Sistema de Filtros Avançados
Baseado em operacional.iatechhub.cloud
"""
from fastapi import APIRouter, Query
from typing import Optional, List
import sqlite3
from datetime import datetime, date

router = APIRouter()
DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def build_filtro_sql(
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    status: Optional[List[str]] = None,
    categorias: Optional[List[str]] = None
) -> str:
    """Constrói cláusula WHERE dinâmica baseada nos filtros"""
    condicoes = []
    
    # Filtro de data
    if data_inicio and data_fim:
        condicoes.append(f"DATE(data_fechamento) BETWEEN '{data_inicio}' AND '{data_fim}'")
    elif data_inicio:
        condicoes.append(f"DATE(data_fechamento) >= '{data_inicio}'")
    elif data_fim:
        condicoes.append(f"DATE(data_fechamento) <= '{data_fim}'")
    
    # Filtro de status
    if status and len(status) > 0:
        status_list = "','".join(status)
        condicoes.append(f"status IN ('{status_list}')")
    
    # Filtro de categoria
    if categorias and len(categorias) > 0:
        cat_list = "','".join(categorias)
        condicoes.append(f"categoria IN ('{cat_list}')")
    
    return " AND ".join(condicoes) if condicoes else "1=1"


@router.get("/resumo")
async def get_resumo_filtrado(
    data_inicio: Optional[str] = Query(None, description="YYYY-MM-DD"),
    data_fim: Optional[str] = Query(None, description="YYYY-MM-DD"),
    status: Optional[str] = Query(None, description="finalizada,agendada,execucao"),
    categorias: Optional[str] = Query(None, description="servico,suporte,infra")
):
    """
    Resumo geral com filtros dinâmicos
    
    Exemplo:
    /api/dashboard/v2/resumo?data_inicio=2026-03-20&status=finalizada,execucao
    """
    db = get_db()
    
    # Processar filtros
    status_list = status.split(',') if status else None
    cat_list = categorias.split(',') if categorias else None
    filtro_where = build_filtro_sql(data_inicio, data_fim, status_list, cat_list)
    
    # Buscar dados
    resumo = dict(db.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status='agendada' THEN 1 ELSE 0 END) AS agendadas,
            SUM(CASE WHEN status='aguardando' THEN 1 ELSE 0 END) AS aguardando,
            SUM(CASE WHEN status='execucao' OR status='aberta' THEN 1 ELSE 0 END) AS execucao,
            SUM(CASE WHEN categoria='servico' THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN categoria='suporte' THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN categoria='infra' THEN 1 ELSE 0 END) AS infra,
            SUM(CASE WHEN categoria='retirada' THEN 1 ELSE 0 END) AS retiradas,
            SUM(CASE WHEN categoria NOT IN ('servico','suporte','infra','retirada') THEN 1 ELSE 0 END) AS outros
        FROM prod_os_cache
        WHERE {filtro_where}
    """).fetchone())
    
    # Meta do dia
    meta_row = db.execute("""
        SELECT valor FROM prod_metas
        WHERE tipo='os_dia' AND tecnico_id IS NULL AND vigente=1
        LIMIT 1
    """).fetchone()
    
    db.close()
    
    meta = int(meta_row["valor"]) if meta_row else 150
    fins = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    
    return {
        "resumo": resumo,
        "meta_dia": meta,
        "eficiencia": round(fins / total * 100, 1) if total > 0 else 0,
        "meta_percentual": round(fins / meta * 100) if meta > 0 else 0,
        "filtros_aplicados": {
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "status": status_list,
            "categorias": cat_list
        }
    }


@router.get("/categoria/{categoria}/tecnicos")
async def get_tecnicos_por_categoria(
    categoria: str,
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    status: Optional[str] = Query(None)
):
    """
    Detalha quais técnicos trabalharam em uma categoria
    
    Exemplo:
    /api/dashboard/v2/categoria/servico/tecnicos
    
    Retorna: LEANDRO (12 OS), JOSEILTON (2 OS), etc.
    """
    db = get_db()
    
    status_list = status.split(',') if status else None
    filtro_where = build_filtro_sql(data_inicio, data_fim, status_list, [categoria])
    
    rows = db.execute(f"""
        SELECT
            t.id,
            t.nome,
            COUNT(o.id) AS total_os,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas
        FROM prod_tecnicos t
        INNER JOIN prod_os_cache o ON o.tecnico_id = t.id
        WHERE {filtro_where}
        GROUP BY t.id, t.nome
        ORDER BY total_os DESC
    """).fetchall()
    
    db.close()
    
    tecnicos = []
    for r in rows:
        tecnicos.append({
            "id": r["id"],
            "nome": r["nome"],
            "total_os": r["total_os"],
            "finalizadas": r["finalizadas"],
            "percentual_finalizacao": round(r["finalizadas"] / r["total_os"] * 100, 1) if r["total_os"] > 0 else 0
        })
    
    return {
        "categoria": categoria,
        "total_tecnicos": len(tecnicos),
        "total_os": sum(t["total_os"] for t in tecnicos),
        "tecnicos": tecnicos
    }


@router.get("/tecnico/{tecnico_id}/os")
async def get_os_do_tecnico(
    tecnico_id: int,
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    categoria: Optional[str] = Query(None)
):
    """
    Lista todas as OS de um técnico específico
    
    Exemplo:
    /api/dashboard/v2/tecnico/1/os?categoria=servico
    
    Retorna as 12 OS de serviço do LEANDRO
    """
    db = get_db()
    
    # Buscar nome do técnico
    tecnico = dict(db.execute("""
        SELECT id, nome, ixc_funcionario_id, meta_dia
        FROM prod_tecnicos
        WHERE id = ?
    """, (tecnico_id,)).fetchone() or {})
    
    if not tecnico:
        db.close()
        return {"erro": "Técnico não encontrado"}
    
    # Filtros
    status_list = status.split(',') if status else None
    cat_list = [categoria] if categoria else None
    filtro_base = build_filtro_sql(data_inicio, data_fim, status_list, cat_list)
    filtro_where = f"tecnico_id = {tecnico_id} AND ({filtro_base})"
    
    # Buscar OS
    rows = db.execute(f"""
        SELECT
            ixc_os_id,
            ixc_assunto_id,
            categoria,
            status,
            data_abertura,
            data_fechamento
        FROM prod_os_cache
        WHERE {filtro_where}
        ORDER BY data_fechamento DESC
    """).fetchall()
    
    db.close()
    
    os_list = []
    for r in rows:
        os_list.append({
            "os_id": r["ixc_os_id"],
            "assunto_id": r["ixc_assunto_id"],
            "categoria": r["categoria"],
            "status": r["status"],
            "data_abertura": r["data_abertura"],
            "data_fechamento": r["data_fechamento"]
        })
    
    return {
        "tecnico": tecnico,
        "total_os": len(os_list),
        "os_list": os_list,
        "filtros_aplicados": {
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "status": status_list,
            "categoria": categoria
        }
    }


@router.get("/alertas")
async def get_alertas(
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None)
):
    """Alertas dinâmicos baseados nas métricas"""
    db = get_db()
    
    filtro_where = build_filtro_sql(data_inicio, data_fim, None, None)
    
    resumo = dict(db.execute(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN status='execucao' OR status='aberta' THEN 1 ELSE 0 END) AS execucao
        FROM prod_os_cache
        WHERE {filtro_where}
    """).fetchone())
    
    meta_row = db.execute("""
        SELECT valor FROM prod_metas
        WHERE tipo='os_dia' AND tecnico_id IS NULL AND vigente=1
        LIMIT 1
    """).fetchone()
    
    db.close()
    
    meta = int(meta_row["valor"]) if meta_row else 150
    fins = resumo["finalizadas"] or 0
    total = resumo["total"] or 0
    execucao = resumo["execucao"] or 0
    pct_meta = round(fins / meta * 100) if meta > 0 else 0
    
    alertas = []
    
    if pct_meta < 50:
        alertas.append({"tipo": "erro", "mensagem": f"Meta em risco ({pct_meta}%)", "cor": "red"})
    elif pct_meta < 80:
        alertas.append({"tipo": "aviso", "mensagem": f"Abaixo da meta ({pct_meta}%)", "cor": "amber"})
    
    if execucao > 10:
        alertas.append({"tipo": "aviso", "mensagem": f"{execucao} OS em execução", "cor": "amber"})
    
    if total > 0 and fins == total:
        alertas.append({"tipo": "sucesso", "mensagem": "100% finalizadas!", "cor": "green"})
    
    if pct_meta >= 100:
        alertas.append({"tipo": "sucesso", "mensagem": "Meta do dia atingida!", "cor": "green"})
    
    return {"alertas": alertas, "meta_percentual": pct_meta}


@router.get("/top-tecnicos")
async def get_top_tecnicos(
    data_inicio: Optional[str] = Query(None),
    data_fim: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(6)
):
    """
    Top técnicos com base nos filtros aplicados
    
    Exemplo:
    /api/dashboard/v2/top-tecnicos?data_inicio=2026-03-20&status=finalizada&limit=6
    """
    db = get_db()
    
    status_list = status.split(',') if status else None
    filtro_where = build_filtro_sql(data_inicio, data_fim, status_list, None)
    
    rows = db.execute(f"""
        SELECT
            t.id,
            t.nome,
            t.meta_dia,
            COUNT(o.id) AS total,
            SUM(CASE WHEN o.status='finalizada' THEN 1 ELSE 0 END) AS finalizadas,
            SUM(CASE WHEN o.categoria='servico' THEN 1 ELSE 0 END) AS servicos,
            SUM(CASE WHEN o.categoria='suporte' THEN 1 ELSE 0 END) AS suportes,
            SUM(CASE WHEN o.categoria='infra' THEN 1 ELSE 0 END) AS infra
        FROM prod_tecnicos t
        LEFT JOIN prod_os_cache o 
            ON o.tecnico_id = t.id 
            AND ({filtro_where})
        WHERE t.ativo = 1
        GROUP BY t.id, t.nome, t.meta_dia
        ORDER BY finalizadas DESC
        LIMIT {limit}
    """).fetchall()
    
    db.close()
    
    tecnicos = []
    for i, r in enumerate(rows):
        total = r["total"] or 0
        fins = r["finalizadas"] or 0
        score = (r["servicos"] or 0) * 3 + (r["suportes"] or 0) * 2 + (r["infra"] or 0) * 2
        efic = round(fins / total * 100, 1) if total > 0 else 0.0
        
        # Definir classe de posição
        if i == 0:
            classe = "p1"
        elif i == 1:
            classe = "p2"
        elif i == 2:
            classe = "p3"
        else:
            classe = "pn"
        
        tecnicos.append({
            "posicao": i + 1,
            "id": r["id"],
            "nome": r["nome"],
            "os_finalizadas": fins,
            "os_total": total,
            "eficiencia": efic,
            "score": score,
            "classe_posicao": classe
        })
    
    return {
        "total_tecnicos": len(tecnicos),
        "tecnicos": tecnicos
    }
