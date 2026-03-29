"""
SAIS — Motor de SLA v2
3 métricas de SLA reais:
  sla_fila     = abertura → assumido     (tempo no backoffice/fila)
  sla_desloc   = assumido → execucao     (deslocamento do técnico)
  sla_exec     = execucao → fechamento   (tempo de execução na OS)
  sla_tecnico  = assumido → fechamento   (tempo TOTAL com o técnico)
"""
import sqlite3
from datetime import datetime
from typing import Optional

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _parse_dt(val) -> Optional[datetime]:
    if not val or str(val).startswith("0000"):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val)[:19], fmt)
        except ValueError:
            continue
    return None


def _diff_min(a, b) -> Optional[float]:
    ta, tb = _parse_dt(a), _parse_dt(b)
    if not ta or not tb:
        return None
    diff = (tb - ta).total_seconds() / 60
    return round(diff, 1) if diff >= 0 else None


def _status_sla(realizado_min: float, previsto_h: float) -> str:
    if realizado_min is None:
        return "pendente"
    pct = realizado_min / (previsto_h * 60) * 100
    if pct > 100:   return "estourado"
    if pct > 80:    return "em_risco"
    return "no_prazo"


def get_sla_config(assunto_id: int) -> dict:
    db = get_db()
    row = db.execute(
        "SELECT horas_sla, horas_minimas, horas_maximas FROM sais_sla_config WHERE assunto_id=?",
        (assunto_id,)
    ).fetchone()
    db.close()
    if row:
        return {
            "horas_sla":      row["horas_sla"],
            "horas_minimas":  row["horas_minimas"],
            "horas_maximas":  row["horas_maximas"],
        }
    return {"horas_sla": 4.0, "horas_minimas": 0.17, "horas_maximas": 8.0}


def calcular_sla(os: dict) -> dict:
    """
    Recebe dict de uma OS e retorna análise completa de SLA com 3 métricas.

    sla_fila    = abertura → assumido      (fila/backoffice)
    sla_desloc  = assumido → execucao      (deslocamento)
    sla_exec    = execucao → fechamento    (execução na OS)
    sla_tecnico = assumido → fechamento    (tempo total com técnico)
    sla_total   = abertura → fechamento    (tempo total da OS — legado)
    """
    assunto_id = os.get("ixc_assunto_id") or os.get("assunto_id") or 0
    config     = get_sla_config(assunto_id)
    horas_sla  = config["horas_sla"]
    horas_min  = config["horas_minimas"]
    horas_max  = config["horas_maximas"]

    dab  = os.get("data_abertura")
    dfe  = os.get("data_fechamento")
    das  = os.get("data_hora_assumido")
    dex  = os.get("data_hora_execucao")
    status = os.get("status", "")

    # ── SLA pré-calculado no cache ───────────────────────
    sla_fila_min    = os.get("sla_fila_min")    or _diff_min(dab, das)
    sla_desloc_min  = os.get("sla_desloc_min")  or _diff_min(das, dex)
    sla_exec_min    = os.get("sla_exec_min")    or _diff_min(dex, dfe)
    sla_tecnico_min = os.get("sla_tecnico_min") or _diff_min(das, dfe)
    sla_total_min   = _diff_min(dab, dfe)

    # ── SLA principal: usa tempo do técnico se disponível ─
    # Caso técnico não tenha assumido ainda, usa tempo total
    sla_principal_min = sla_tecnico_min if sla_tecnico_min is not None else sla_total_min

    # ── Status baseado no SLA do técnico ─────────────────
    status_sla = _status_sla(sla_principal_min, horas_sla)

    # ── OS ainda aberta: calcula tempo decorrido ──────────
    horas_restantes = None
    if status in ("agendada", "aberta", "execucao") and dab:
        agora = datetime.now()
        dt_ref = _parse_dt(das) or _parse_dt(dab)
        if dt_ref:
            decorrido_h   = (agora - dt_ref).total_seconds() / 3600
            horas_restantes = max(0, horas_sla - decorrido_h)
            pct_decorrido   = decorrido_h / horas_sla * 100
            if pct_decorrido > 100:   status_sla = "estourado"
            elif pct_decorrido > 80:  status_sla = "em_risco"
            else:                     status_sla = "no_prazo"

    # ── Flags de anomalia ─────────────────────────────────
    sla_tec_h = sla_tecnico_min / 60 if sla_tecnico_min else None
    abaixo_minimo = bool(sla_tec_h and sla_tec_h < horas_min)
    acima_maximo  = bool(sla_tec_h and sla_tec_h > horas_max)

    return {
        # Métricas principais
        "sla_fila_min":     sla_fila_min,
        "sla_desloc_min":   sla_desloc_min,
        "sla_exec_min":     sla_exec_min,
        "sla_tecnico_min":  sla_tecnico_min,
        "sla_total_min":    sla_total_min,

        # Classificação
        "status_sla":          status_sla,
        "horas_previstas":     horas_sla,
        "horas_restantes":     round(horas_restantes, 2) if horas_restantes else None,
        "percentual_consumido": round(
            (sla_principal_min / (horas_sla * 60) * 100) if sla_principal_min else 0, 1
        ),

        # Legado (compatibilidade)
        "horas_realizadas":   round(sla_tecnico_min / 60, 2) if sla_tecnico_min else None,
        "minutos_realizados": round(sla_tecnico_min) if sla_tecnico_min else None,
        "abaixo_minimo":      abaixo_minimo,
        "acima_maximo":       acima_maximo,

        # Flags de disponibilidade
        "tem_assumido":  das is not None,
        "tem_execucao":  dex is not None,
    }


def resumo_sla_dia(data: str = None) -> dict:
    """Resumo de SLA do dia com as 3 métricas."""
    db   = get_db()
    data = data or datetime.now().strftime("%Y-%m-%d")

    rows = db.execute("""
        SELECT
            o.ixc_os_id, o.ixc_assunto_id, o.status,
            o.data_abertura, o.data_fechamento, o.data_agenda,
            o.data_hora_assumido, o.data_hora_execucao,
            o.sla_fila_min, o.sla_desloc_min,
            o.sla_exec_min, o.sla_tecnico_min,
            t.nome AS tecnico_nome
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura), '+3 hours') = ?
    """, (data,)).fetchall()
    db.close()

    total = no_prazo = em_risco = estourado = 0
    os_estouradas = []
    os_em_risco   = []

    # Médias das 3 métricas
    filas = []; deslocamentos = []; execucoes = []; tecnicos = []

    for r in rows:
        d   = dict(r)
        sla = calcular_sla(d)
        s   = sla["status_sla"]
        total += 1

        if s == "no_prazo":    no_prazo  += 1
        elif s == "em_risco":  em_risco  += 1
        elif s == "estourado": estourado += 1

        if d["sla_fila_min"]    is not None: filas.append(d["sla_fila_min"])
        if d["sla_desloc_min"]  is not None: deslocamentos.append(d["sla_desloc_min"])
        if d["sla_exec_min"]    is not None: execucoes.append(d["sla_exec_min"])
        if d["sla_tecnico_min"] is not None: tecnicos.append(d["sla_tecnico_min"])

        if s == "estourado":
            os_estouradas.append({
                "os_id":            r["ixc_os_id"],
                "tecnico":          r["tecnico_nome"],
                "sla_tecnico_min":  sla["sla_tecnico_min"],
                "sla_total_min":    sla["sla_total_min"],
                "horas_previstas":  sla["horas_previstas"],
            })
        elif s == "em_risco":
            os_em_risco.append({
                "os_id":                r["ixc_os_id"],
                "tecnico":              r["tecnico_nome"],
                "percentual_consumido": sla["percentual_consumido"],
                "horas_restantes":      sla["horas_restantes"],
            })

    def media(lst, limite_max=480):
        # Exclui outliers acima do limite (padrão 8h = 480min)
        filtrado = [x for x in lst if x is not None and 0 < x <= limite_max]
        return round(sum(filtrado) / len(filtrado), 1) if filtrado else None

    return {
        "data":    data,
        "total":   total,
        "no_prazo":  no_prazo,
        "em_risco":  em_risco,
        "estourado": estourado,
        "percentual_no_prazo": round(no_prazo / total * 100, 1) if total > 0 else 0,

        # Médias reais (excluindo outliers > 8h)
        "media_sla_fila_min":    media(filas),
        "media_sla_desloc_min":  media(deslocamentos),
        "media_sla_exec_min":    media(execucoes),
        "media_sla_tecnico_min": media(tecnicos),

        # Cobertura dos dados
        "pct_com_assumido": round(len([x for x in filas if x]) / total * 100) if total else 0,
        "pct_com_execucao": round(len([x for x in execucoes if x]) / total * 100) if total else 0,

        "os_estouradas": os_estouradas[:10],
        "os_em_risco":   os_em_risco[:10],
    }
