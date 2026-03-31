#!/usr/bin/env python3
"""
SAIS — Relatório HTML de Qualidade com Diagnóstico Técnico
Envia como documento no Telegram.

Uso:
  venv/bin/python -m app.bootstrap.cron_qualidade_html

Crontab (18:50 BRT = 21:50 UTC, seg-sex):
  50 21 * * 1-5 cd /opt/automacoes/cliquedf/operacional && venv/bin/python -m app.bootstrap.cron_qualidade_html >> /var/log/sais_telegram.log 2>&1
"""

import os, sys, requests, unicodedata, pymysql
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TECNICOS_IDS = "13,55,56,38,35,17,50,11,47,46,43,19"

ASSUNTOS_INST   = "2,14,15,49,227,239,30,48"
ASSUNTOS_SUP    = "5,20,21,44,47,94,103,105,107,240"

def limpar(texto):
    if not texto: return ""
    return str(texto).replace("<","&lt;").replace(">","&gt;")

def gerar_html():
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT",3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"), cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10, charset="utf8mb4"
    )

    data_30d = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                f_inst.funcionario          AS tecnico_inst,
                f_sup.funcionario           AS tecnico_sup,
                cli.razao                   AS cliente,
                cc.id                       AS contrato_id,
                c_sup.id                    AS os_sup_id,
                c_inst.id                   AS os_inst_id,
                a_inst.assunto              AS servico_inst,
                a_sup.assunto               AS motivo_suporte,
                c_inst.data_fechamento      AS data_inst,
                c_sup.data_abertura         AS data_suporte,
                c_sup.mensagem_resposta     AS diag_suporte,
                c_inst.mensagem_resposta    AS diag_inst,
                DATEDIFF(c_sup.data_abertura, c_inst.data_fechamento) AS dias_apos,
                c_sup.status                AS status_sup
            FROM su_oss_chamado c_inst
            JOIN su_oss_chamado c_sup
                ON c_sup.id_cliente = c_inst.id_cliente
                AND c_sup.id_assunto IN ({ASSUNTOS_SUP})
                AND c_sup.data_abertura > c_inst.data_fechamento
                AND c_sup.data_abertura <= DATE_ADD(c_inst.data_fechamento, INTERVAL 30 DAY)
                AND c_sup.id != c_inst.id
            JOIN cliente cli             ON cli.id = c_inst.id_cliente
            JOIN cliente_contrato cc     ON cc.id_cliente = c_inst.id_cliente AND cc.status='A'
            JOIN funcionarios f_inst     ON f_inst.id = c_inst.id_tecnico
            JOIN funcionarios f_sup      ON f_sup.id = c_sup.id_tecnico
            JOIN su_oss_assunto a_inst   ON a_inst.id = c_inst.id_assunto
            JOIN su_oss_assunto a_sup    ON a_sup.id = c_sup.id_assunto
            WHERE c_inst.id_assunto IN ({ASSUNTOS_INST})
              AND c_inst.status = 'F'
              AND c_inst.data_fechamento >= %s
              AND c_inst.id_tecnico IN ({TECNICOS_IDS})
            GROUP BY c_inst.id, c_sup.id
            ORDER BY c_inst.data_fechamento DESC
        """, (data_30d,))
        retornos = cur.fetchall()
    conn.close()

    hoje = datetime.now().strftime("%d/%m/%Y")
    total = len(retornos)

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<title>Qualidade — Cliquedf</title>
<style>
  body{{font-family:Arial,sans-serif;background:#f4f7f6;padding:12px;margin:0}}
  .header{{background:#c62828;color:white;padding:14px 18px;border-radius:8px 8px 0 0;margin-bottom:0}}
  .header h2{{margin:0;font-size:15px}}
  .header p{{margin:4px 0 0;font-size:11px;opacity:.85}}
  .container{{background:white;border-radius:0 0 8px 8px;box-shadow:0 2px 8px rgba(0,0,0,.12);overflow:hidden}}
  table{{width:100%;border-collapse:collapse;font-size:11px}}
  thead th{{background:#fafafa;padding:9px 8px;border-bottom:2px solid #e0e0e0;text-align:left;font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.04em}}
  tbody td{{padding:9px 8px;border-bottom:1px solid #f0f0f0;vertical-align:top}}
  tbody tr:hover td{{background:#fef9f9}}
  .tec{{font-weight:700;font-size:12px;color:#c62828}}
  .tec-sup{{font-size:10px;color:#666;margin-top:2px}}
  .cliente{{font-weight:600;font-size:11px}}
  .ids{{font-size:10px;color:#888;margin-top:3px}}
  .os-link{{color:#1976d2;font-weight:700}}
  .datas{{font-size:11px;text-align:center}}
  .badge{{display:inline-block;background:#ffebee;color:#c62828;padding:2px 7px;border-radius:10px;font-weight:700;font-size:11px}}
  .badge.ok{{background:#e8f5e9;color:#2e7d32}}
  .badge.med{{background:#fff3e0;color:#e65100}}
  .motivo{{font-weight:700;font-size:11px;color:#333}}
  .diag{{font-size:10px;color:#666;margin-top:5px;background:#fafafa;padding:6px 8px;border-left:3px solid #ef9a9a;border-radius:0 4px 4px 0;white-space:pre-wrap;line-height:1.5}}
  .diag b{{color:#c62828}}
  .status-f{{color:#2e7d32;font-weight:700}}
  .status-a{{color:#e65100;font-weight:700}}
</style>
</head>
<body>
<div class="header">
  <h2>⚠️ Qualidade Pós-Instalação — Cliquedf</h2>
  <p>📅 {hoje} · {total} retornos nos últimos 30 dias · Inclui diagnóstico do técnico de suporte</p>
</div>
<div class="container">
<table>
  <thead>
    <tr>
      <th>Técnico Inst.</th>
      <th>Cliente / OS</th>
      <th>Datas</th>
      <th>Dias</th>
      <th>Motivo · Diagnóstico do Suporte</th>
    </tr>
  </thead>
  <tbody>
"""

    for r in retornos:
        dias = r["dias_apos"] or 0
        badge_cls = "badge" if dias <= 7 else "badge med" if dias <= 15 else "badge ok"
        status_cls = "status-f" if r["status_sup"]=="F" else "status-a"
        status_txt = "✅ Fechada" if r["status_sup"]=="F" else "🔄 Aberta"

        diag = limpar(r["diag_suporte"] or "Sem diagnóstico preenchido")
        motivo = limpar(r["motivo_suporte"] or "—")
        servico_inst = limpar(r["servico_inst"] or "—")
        cliente = limpar(r["cliente"] or "—")
        tec_inst = limpar(r["tecnico_inst"] or "—")
        tec_sup  = limpar(r["tecnico_sup"] or "—")

        data_i = r["data_inst"].strftime("%d/%m") if r["data_inst"] else "—"
        data_s = r["data_suporte"].strftime("%d/%m") if r["data_suporte"] else "—"

        html += f"""    <tr>
      <td>
        <div class="tec">{tec_inst}</div>
        <div class="tec-sup">Sup: {tec_sup}</div>
      </td>
      <td>
        <div class="cliente">{cliente[:22]}</div>
        <div class="ids">
          CT: {r['contrato_id']} |
          <span class="os-link">OS inst: #{r['os_inst_id']}</span><br>
          <span class="os-link">OS sup: #{r['os_sup_id']}</span>
        </div>
      </td>
      <td class="datas">{data_i}<br>↓<br>{data_s}</td>
      <td style="text-align:center">
        <span class="{badge_cls}">{dias}d</span><br>
        <span class="{status_cls}" style="font-size:10px">{status_txt}</span>
      </td>
      <td>
        <div class="motivo">📋 Serviço inst: {servico_inst[:30]}</div>
        <div class="motivo" style="margin-top:4px">⚠️ Retorno: {motivo}</div>
        <div class="diag"><b>Diagnóstico:</b><br>{diag[:800]}</div>
      </td>
    </tr>
"""

    html += """  </tbody>
</table>
</div>
</body>
</html>"""

    return html, total


def main():
    print("Gerando relatório de qualidade HTML...")
    try:
        html, total = gerar_html()
    except Exception as e:
        print(f"Erro ao gerar HTML: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    path = "/tmp/qualidade_cliquedf.html"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML gerado: {total} retornos")

    hoje = datetime.now().strftime('%d/%m/%Y')
    caption = (
        f"🌅 Bom dia! Que todos tenham um ótimo dia de trabalho.\n\n"
        f"📊 Nossa automação analisou as instalações dos últimos 30 dias "
        f"e <b>{total}</b> tiveram retorno de suporte.\n\n"
        f"📋 Analisem as causas no relatório abaixo para que possamos "
        f"melhorar nos meses à frente.\n\n"
        f"📅 {hoje} · Qualidade Pós-Instalação — Cliquedf"
    )

    with open(path, "rb") as doc:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendDocument",
            data={"chat_id": CHAT_ID, "caption": caption},
            files={"document": ("qualidade_cliquedf.html", doc, "text/html")},
            timeout=30
        )

    if r.status_code == 200:
        print("✅ Relatório enviado com sucesso!")
    else:
        print(f"❌ Erro Telegram: {r.status_code} — {r.text}")
        sys.exit(1)


if __name__ == "__main__":
    main()
