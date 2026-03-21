"""
SAIS — Cron de Auditoria + Eventos
Executar a cada 15 minutos via crontab:

    */15 * * * * cd /opt/automacoes/cliquedf/operacional && \
        source venv/bin/activate && \
        python3 -m app.bootstrap.cron_auditoria >> /var/log/sais_auditoria.log 2>&1
"""
import sys
import os
sys.path.insert(0, "/opt/automacoes/cliquedf/operacional")

from app.engines.audit_engine import rodar_auditoria
from app.engines.event_engine import detectar_e_registrar
import logging

log = logging.getLogger("CRON_AUDIT")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [CRON] %(message)s",
                    datefmt="%H:%M:%S")

if __name__ == "__main__":
    log.info("=== Iniciando ciclo SAIS ===")
    try:
        total_audit = rodar_auditoria()
        log.info(f"Auditoria: {total_audit} ocorrências")
    except Exception as e:
        log.error(f"Erro na auditoria: {e}")

    try:
        result = detectar_e_registrar()
        log.info(f"Eventos: {result}")
    except Exception as e:
        log.error(f"Erro nos eventos: {e}")

    log.info("=== Ciclo SAIS concluído ===")
