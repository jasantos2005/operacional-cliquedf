"""
SAIS — Motor de SLA
Calcula SLA previsto vs realizado por OS e assunto.
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


def get_sla_config(assunto_id: int) -> dict:
    db = get_db()
    row = db.execute(
        "SELECT horas_sla, horas_minimas, horas_maximas FROM sais_sla_config WHERE assunto_id=?",
        (assunto_id,)
    ).fetchone()
    db.close()
    if row:
        return {"horas_sla": row["horas_sla"], "horas_minimas": row["horas_minimas"], "horas_maximas": row["horas_maximas"]}
    return {"horas_sla": 4.0, "horas_minimas": 0.17, "horas_maximas": 8.0}


def calcular_sla(os: dict) -> dict:
    """
    Recebe dict de uma OS e retorna análise de SLA.

    Retorna:
    {
        horas_previstas, horas_realizadas, horas_restantes,
        status_sla: 'no_prazo'|'em_risco'|'estourado'|'pendente',
        percentual_consumido, minutos_realizados,
        abaixo_minimo, acima_maximo
    }
    """
    assunto_id = os.get("ixc_assunto_id") or os.get("assunto_id") or 0
    config = get_sla_config(assunto_id)
    horas_sla = config["horas_sla"]
    horas_min = config["horas_minimas"]
    horas_max = config["horas_maximas"]

    dt_abertura  = _parse_dt(os.get("data_abertura"))
    dt_fechamento = _parse_dt(os.get("data_fechamento"))
    dt_agenda    = _parse_dt(os.get("data_agenda"))
    status       = os.get("status", "")

    horas_realizadas = None
    horas_restantes  = None
    status_sla       = "pendente"
    percentual       = 0
    abaixo_minimo    = False
    acima_maximo     = False

    if status == "finalizada" and dt_abertura and dt_fechamento:
        delta = dt_fechamento - dt_abertura
        horas_realizadas = delta.total_seconds() / 3600
        percentual = round(horas_realizadas / horas_sla * 100, 1) if horas_sla > 0 else 0

        if horas_realizadas > horas_sla:
            status_sla = "estourado"
        elif horas_realizadas > (horas_sla * 0.8):
            status_sla = "em_risco"
        else:
            status_sla = "no_prazo"

        abaixo_minimo = horas_realizadas < horas_min
        acima_maximo  = horas_realizadas > horas_max

    elif status in ("agendada", "aberta", "execucao") and dt_abertura:
        agora = datetime.now()
        delta = agora - dt_abertura
        horas_decorridas = delta.total_seconds() / 3600
        horas_restantes  = max(0, horas_sla - horas_decorridas)
        percentual = round(horas_decorridas / horas_sla * 100, 1) if horas_sla > 0 else 0

        if horas_decorridas > horas_sla:
            status_sla = "estourado"
        elif horas_decorridas > (horas_sla * 0.8):
            status_sla = "em_risco"
        else:
            status_sla = "no_prazo"

    minutos = round(horas_realizadas * 60) if horas_realizadas is not None else None

    return {
        "horas_previstas":     horas_sla,
        "horas_realizadas":    round(horas_realizadas, 2) if horas_realizadas is not None else None,
        "horas_restantes":     round(horas_restantes, 2) if horas_restantes is not None else None,
        "minutos_realizados":  minutos,
        "status_sla":          status_sla,
        "percentual_consumido": percentual,
        "abaixo_minimo":       abaixo_minimo,
        "acima_maximo":        acima_maximo,
    }


def resumo_sla_dia(data: str = None) -> dict:
    """Resumo de SLA do dia para o dashboard."""
    db = get_db()
    data = data or datetime.now().strftime("%Y-%m-%d")

    rows = db.execute("""
        SELECT o.ixc_os_id, o.ixc_assunto_id, o.status,
               o.data_abertura, o.data_fechamento, o.data_agenda,
               t.nome as tecnico_nome
        FROM prod_os_cache o
        LEFT JOIN prod_tecnicos t ON t.id = o.tecnico_id
        WHERE DATE(COALESCE(o.data_fechamento, o.data_agenda, o.data_abertura)) = ?
    """, (data,)).fetchall()
    db.close()

    total = no_prazo = em_risco = estourado = 0
    os_estouradas = []
    os_em_risco   = []

    for r in rows:
        sla = calcular_sla(dict(r))
        s = sla["status_sla"]
        if s == "no_prazo":    no_prazo  += 1
        elif s == "em_risco":  em_risco  += 1
        elif s == "estourado": estourado += 1
        total += 1

        if s == "estourado":
            os_estouradas.append({
                "os_id": r["ixc_os_id"],
                "tecnico": r["tecnico_nome"],
                "horas_realizadas": sla["horas_realizadas"],
                "horas_previstas":  sla["horas_previstas"],
            })
        elif s == "em_risco":
            os_em_risco.append({
                "os_id": r["ixc_os_id"],
                "tecnico": r["tecnico_nome"],
                "percentual_consumido": sla["percentual_consumido"],
                "horas_restantes": sla["horas_restantes"],
            })

    pct_no_prazo = round(no_prazo / total * 100, 1) if total > 0 else 0

    return {
        "data": data,
        "total": total,
        "no_prazo": no_prazo,
        "em_risco": em_risco,
        "estourado": estourado,
        "percentual_no_prazo": pct_no_prazo,
        "os_estouradas": os_estouradas[:10],
        "os_em_risco": os_em_risco[:10],
    }
