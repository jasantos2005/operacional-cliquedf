"""
SAIS — Motor de Auditoria Automática
Detecta irregularidades nas OS e registra em sais_auditorias.

Executar via cron a cada 15 minutos:
    python3 -m app.engines.audit_engine
"""
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Optional

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"
log = logging.getLogger("AUDIT")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [AUDIT] %(message)s",
                    datefmt="%H:%M:%S")


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


def _ja_existe(db, os_id: int, subtipo: str) -> bool:
    """Evita duplicar auditorias para a mesma OS + subtipo."""
    row = db.execute(
        "SELECT id FROM sais_auditorias WHERE os_id=? AND subtipo=? AND resolvida=0",
        (os_id, subtipo)
    ).fetchone()
    return row is not None


def _registrar(db, os_id, tecnico_id, tipo, subtipo, criticidade, descricao,
               valor_detectado=None, valor_esperado=None):
    if _ja_existe(db, os_id, subtipo):
        return False
    db.execute("""
        INSERT INTO sais_auditorias
            (os_id, tecnico_id, tipo, subtipo, criticidade,
             descricao, valor_detectado, valor_esperado)
        VALUES (?,?,?,?,?,?,?,?)
    """, (os_id, tecnico_id, tipo, subtipo, criticidade,
          descricao, valor_detectado, valor_esperado))
    return True


# ═══════════════════════════════════════════════════════
# REGRAS DE AUDITORIA
# ═══════════════════════════════════════════════════════

def auditar_tempo(db, os: dict) -> int:
    """
    Verifica tempo de execução da OS.
    Retorna número de auditorias registradas.
    """
    if os["status"] != "finalizada":
        return 0

    dt_ab   = _parse_dt(os["data_abertura"])
    dt_fech = _parse_dt(os["data_fechamento"])
    if not dt_ab or not dt_fech:
        return 0

    delta_h = (dt_fech - dt_ab).total_seconds() / 3600
    count = 0

    # Tempo abaixo do mínimo (menos de 10 minutos)
    if delta_h < 0.17:
        minutos = round(delta_h * 60, 1)
        ok = _registrar(db, os["ixc_os_id"], os["tecnico_id"],
                        "tempo", "tempo_curto", "alta",
                        f"OS finalizada em {minutos} minutos (mínimo esperado: 10 min)",
                        valor_detectado=f"{minutos} min", valor_esperado="≥ 10 min")
        if ok: count += 1

    # Tempo acima do máximo (mais de 6 horas)
    elif delta_h > 6.0:
        horas = round(delta_h, 1)
        ok = _registrar(db, os["ixc_os_id"], os["tecnico_id"],
                        "tempo", "tempo_longo", "media",
                        f"OS com {horas}h de duração (máximo esperado: 6h)",
                        valor_detectado=f"{horas}h", valor_esperado="≤ 6h")
        if ok: count += 1

    return count


def auditar_sla(db, os: dict) -> int:
    """Verifica SLA da OS."""
    if os["status"] != "finalizada":
        return 0

    dt_ab   = _parse_dt(os["data_abertura"])
    dt_fech = _parse_dt(os["data_fechamento"])
    if not dt_ab or not dt_fech:
        return 0

    # Buscar SLA configurado para o assunto
    sla_row = db.execute(
        "SELECT horas_sla FROM sais_sla_config WHERE assunto_id=?",
        (os["ixc_assunto_id"],)
    ).fetchone()
    horas_sla = sla_row["horas_sla"] if sla_row else 4.0

    delta_h = (dt_fech - dt_ab).total_seconds() / 3600
    count = 0

    if delta_h > horas_sla:
        ok = _registrar(db, os["ixc_os_id"], os["tecnico_id"],
                        "sla", "sla_estourado", "critica",
                        f"SLA estourado: {round(delta_h,1)}h realizado vs {horas_sla}h previsto",
                        valor_detectado=f"{round(delta_h,1)}h",
                        valor_esperado=f"≤ {horas_sla}h")
        if ok: count += 1

    return count


def auditar_comportamento(db, tecnicos: list) -> int:
    """
    Detecta padrões suspeitos por técnico:
    - Muitas OS rápidas em sequência
    - Ritmo anormalmente alto
    """
    hoje = datetime.now().strftime("%Y-%m-%d")
    count = 0

    for tec in tecnicos:
        tec_id = tec["id"]

        # Buscar OS finalizadas hoje com menos de 15 min cada
        rapidas = db.execute("""
            SELECT COUNT(*) as total
            FROM prod_os_cache
            WHERE tecnico_id = ?
              AND status = 'finalizada'
              AND DATE(data_fechamento) = ?
              AND (
                julianday(data_fechamento) - julianday(data_abertura)
              ) * 24 < 0.25
        """, (tec_id, hoje)).fetchone()

        if rapidas and rapidas["total"] >= 5:
            # Verificar se já existe auditoria para hoje
            existe = db.execute("""
                SELECT id FROM sais_auditorias
                WHERE tecnico_id=? AND subtipo='muitas_rapidas'
                  AND DATE(criado_em)=? AND resolvida=0
            """, (tec_id, hoje)).fetchone()

            if not existe:
                db.execute("""
                    INSERT INTO sais_auditorias
                        (os_id, tecnico_id, tipo, subtipo, criticidade, descricao, valor_detectado)
                    VALUES (NULL,?,?,?,?,?,?)
                """, (tec_id, "comportamento", "muitas_rapidas", "alta",
                      f"Técnico com {rapidas['total']} OS finalizadas em menos de 15 min hoje",
                      f"{rapidas['total']} OS"))
                count += 1

    return count


def auditar_os_sem_fechamento(db) -> int:
    """OS em execução há mais de 8 horas sem fechamento."""
    agora = datetime.now()
    limite = agora - timedelta(hours=8)
    count = 0

    rows = db.execute("""
        SELECT ixc_os_id, tecnico_id, data_abertura
        FROM prod_os_cache
        WHERE status IN ('execucao', 'aberta')
          AND data_abertura IS NOT NULL
          AND data_abertura < ?
    """, (limite.strftime("%Y-%m-%d %H:%M:%S"),)).fetchall()

    for r in rows:
        dt_ab = _parse_dt(r["data_abertura"])
        if dt_ab:
            horas = (agora - dt_ab).total_seconds() / 3600
            ok = _registrar(db, r["ixc_os_id"], r["tecnico_id"],
                            "execucao", "sem_fechamento", "alta",
                            f"OS aberta há {round(horas,1)}h sem fechamento",
                            valor_detectado=f"{round(horas,1)}h", valor_esperado="< 8h")
            if ok: count += 1

    return count


# ═══════════════════════════════════════════════════════
# EXECUÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════════

def rodar_auditoria():
    log.info("Iniciando auditoria...")
    db = get_db()

    hoje = datetime.now().strftime("%Y-%m-%d")

    # Buscar OS do dia
    os_list = db.execute("""
        SELECT ixc_os_id, tecnico_id, ixc_assunto_id, status,
               data_abertura, data_fechamento, data_agenda
        FROM prod_os_cache
        WHERE DATE(COALESCE(data_fechamento, data_agenda, data_abertura)) = ?
    """, (hoje,)).fetchall()

    # Buscar técnicos ativos
    tecnicos = db.execute(
        "SELECT id, nome FROM prod_tecnicos WHERE ativo=1"
    ).fetchall()

    total_tempo    = 0
    total_sla      = 0
    total_comp     = 0
    total_semfech  = 0

    for os in os_list:
        os_dict = dict(os)
        total_tempo   += auditar_tempo(db, os_dict)
        total_sla     += auditar_sla(db, os_dict)

    total_comp    = auditar_comportamento(db, [dict(t) for t in tecnicos])
    total_semfech = auditar_os_sem_fechamento(db)

    db.commit()

    total = total_tempo + total_sla + total_comp + total_semfech

    # Log de execução
    db.execute("""
        INSERT INTO sais_automacoes_log (automacao, status, itens_afetados, detalhe)
        VALUES ('audit_engine', 'ok', ?, ?)
    """, (total, f"tempo:{total_tempo} sla:{total_sla} comp:{total_comp} sem_fech:{total_semfech}"))
    db.commit()
    db.close()

    log.info(f"Auditoria concluída: {total} ocorrências | "
             f"tempo:{total_tempo} sla:{total_sla} comp:{total_comp} sem_fech:{total_semfech}")
    return total


if __name__ == "__main__":
    rodar_auditoria()
