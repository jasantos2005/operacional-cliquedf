"""
SAIS — Motor de Regras de Pontuação por OS v1.0
Consulta IXC em tempo real, aplica regras por assunto e salva no SQLite.

Executar via cron a cada 15 minutos (junto com cron_auditoria.py):
    python3 -m app.engines.regras_engine

Tabela criada: sais_os_pontuacao
"""
import sqlite3
import pymysql
import logging
import os
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv("/opt/automacoes/cliquedf/operacional/.env")

DB_PATH = "/opt/automacoes/cliquedf/operacional/prod_local.db"
log = logging.getLogger("REGRAS")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [REGRAS] %(message)s", datefmt="%H:%M:%S")

# ═══════════════════════════════════════════════════
# IDs de fibra/cabo (produtos que contam como fibra)
# ═══════════════════════════════════════════════════
IDS_FIBRA = [
    899,884,873,869,867,865,840,837,808,779,775,764,761,723,680,672,
    621,620,570,569,563,510,495,493,478,477,476,475,474,433,369,368,
    367,366,333,332,327,227,310,302,300,262,224,172,168,167,166,163,
    162,161,135,118,117,112
]

# ═══════════════════════════════════════════════════
# REGRAS POR ASSUNTO
# Formato: id_assunto → {
#   nome, pontos_base,
#   pen_foto, pen_app, pen_produto, pen_descricao,
#   min_descricao, min_tempo_bonus, bonus_tempo,
#   exige_comodato, exige_fibra, min_fibra_bonus
# }
# ═══════════════════════════════════════════════════
REGRAS = {
    113: {"nome": "TROCA DE EQUIPAMENTOS",                  "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto": -5, "pen_desc": -5, "min_desc": 0,  "exige_comodato": True,  "exige_fibra": False, "min_fotos": 2, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    17:  {"nome": "TROCA DE SENHA",                         "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    16:  {"nome": "MANUTENÇÃO",                             "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 10, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 40, "bonus_tempo": -10,"min_fibra_bonus": 0},
    21:  {"nome": "INTERNET LENTA",                         "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 10, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    94:  {"nome": "[CDF] SUPORTE TECNICO",                  "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 30, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    226: {"nome": "[DF] VERIFICAR CONEXÃO",                 "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 10, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    20:  {"nome": "SEM ACESSO",                             "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 30, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    240: {"nome": "FIBRA ROMPIDA",                          "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto": -20,"pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": True,  "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    239: {"nome": "INSTALAR COMODATO_UP2025",               "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto": -5, "pen_desc":  0, "min_desc": 0,  "exige_comodato": True,  "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    227: {"nome": "[OPC] ATIVAÇÃO FIBRA - NOVO",            "base": 20, "pen_foto": -5, "pen_app": -5, "pen_produto": -10,"pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": True,  "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 10, "min_fibra_bonus": 150},
    19:  {"nome": "MUDANÇA DE ENDEREÇO",                    "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    15:  {"nome": "REATIVAÇÃO",                             "base": 20, "pen_foto": -5, "pen_app": -5, "pen_produto": -10,"pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": True,  "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 10, "min_fibra_bonus": 150},
    18:  {"nome": "MUDANÇA DE EQUIPAMENTO DE CÔMODO",       "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 1, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    22:  {"nome": "RECOLHIMENTO DE EQUIPAMENTO",            "base": 10, "pen_foto":  0, "pen_app":  0, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    30:  {"nome": "INSTALAR COMODATO",                      "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto": -5, "pen_desc":  0, "min_desc": 0,  "exige_comodato": True,  "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    39:  {"nome": "RETIRADA DE EQUIPAMENTO",                "base": 10, "pen_foto":  0, "pen_app":  0, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    40:  {"nome": "RETIRADA DA FIBRA NA CTO",               "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 1, "min_tempo_bonus": 40, "bonus_tempo": 5,  "min_fibra_bonus": 0},
    48:  {"nome": "INSTALAR ROTEADOR COMPRADO NA LOJA",     "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 1, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    49:  {"nome": "ATIVACAO FIBRA",                         "base": 20, "pen_foto": -5, "pen_app": -5, "pen_produto": -10,"pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": True,  "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 10, "min_fibra_bonus": 150},
    72:  {"nome": "VIABILIDADE",                            "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    76:  {"nome": "FEEDBACK_NEGATIVO - SUPORTE",            "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    89:  {"nome": "RETIRAR FIBRA",                          "base": 10, "pen_foto":  0, "pen_app":  0, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 40, "bonus_tempo": 5,  "min_fibra_bonus": 0},
    91:  {"nome": "VENDA DE CABOS",                         "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto": -5, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 1, "min_tempo_bonus": 40, "bonus_tempo": 5,  "min_fibra_bonus": 0},
    104: {"nome": "INSTALAR APP NA RESIDENCIA",             "base": 10, "pen_foto":  0, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    105: {"nome": "VERIFICAR ITTV",                         "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 30, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    110: {"nome": "MUDANÇA DE TITULARIDADE (TECNICO)",      "base": 20, "pen_foto": -5, "pen_app": -5, "pen_produto": -10,"pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": True,  "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 10, "min_fibra_bonus": 150},
    111: {"nome": "RECOLHIMENTO EQUIPAMENTO (M.TIT.)",      "base": 10, "pen_foto":  0, "pen_app":  0, "pen_produto":  0, "pen_desc":  0, "min_desc": 0,  "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    154: {"nome": "CONSTRUÇÃO - CTO",                       "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto": -10,"pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 60, "bonus_tempo": 20, "min_fibra_bonus": 0},
    178: {"nome": "MANUTENÇÃO - TROCA DE SPLITTER",         "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto": -10,"pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 60, "bonus_tempo": 20, "min_fibra_bonus": 0},
    220: {"nome": "ATIVAÇÃO PLAYHUB (TECNICO)",             "base": 10, "pen_foto": -5, "pen_app": -5, "pen_produto":  0, "pen_desc": -5, "min_desc": 15, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    161: {"nome": "MANUTENÇÃO DE REDES",                   "base": 20, "pen_foto":  0, "pen_app":  0, "pen_produto":  0, "pen_desc": -5, "min_desc": 10, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 0,  "bonus_tempo": 0,  "min_fibra_bonus": 0},
    232: {"nome": "[INFRA] ATIVAÇÃO FIBRA",                 "base": 20, "pen_foto":  0, "pen_app": -5, "pen_produto": -10,"pen_desc": -5, "min_desc": 20, "exige_comodato": False, "exige_fibra": False, "min_fotos": 0, "min_tempo_bonus": 60, "bonus_tempo": 20, "min_fibra_bonus": 0},
}


# ═══════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════
def limpar_html(texto):
    if not texto:
        return ""
    return re.sub(re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});'), '', str(texto)).strip()


def get_ixc():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def criar_tabela():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS sais_os_pontuacao (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            os_id           INTEGER UNIQUE NOT NULL,
            tecnico_id      INTEGER,
            assunto_id      INTEGER,
            nome_assunto    TEXT,
            cliente_nome    TEXT,
            tecnico_nome    TEXT,
            pontos_base     INTEGER DEFAULT 0,
            pontos_final    INTEGER DEFAULT 0,
            pen_foto        INTEGER DEFAULT 0,
            pen_app         INTEGER DEFAULT 0,
            pen_produto     INTEGER DEFAULT 0,
            pen_descricao   INTEGER DEFAULT 0,
            bonus_tempo     INTEGER DEFAULT 0,
            bonus_fibra     INTEGER DEFAULT 0,
            total_fotos     INTEGER DEFAULT 0,
            tem_produto     INTEGER DEFAULT 0,
            tem_comodato    INTEGER DEFAULT 0,
            tem_app         INTEGER DEFAULT 0,
            metros_fibra    REAL DEFAULT 0,
            minutos_exec    REAL DEFAULT 0,
            len_descricao   INTEGER DEFAULT 0,
            pendencias      TEXT DEFAULT '',
            aprovada        INTEGER DEFAULT 0,
            calculado_em    TEXT DEFAULT (datetime('now','-3 hours'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_osp_os ON sais_os_pontuacao(os_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_osp_tec ON sais_os_pontuacao(tecnico_id)")
    db.commit()
    db.close()


# ═══════════════════════════════════════════════════
# CÁLCULO DE PONTUAÇÃO POR OS
# ═══════════════════════════════════════════════════
def calcular_pontuacao_os(os: dict, fotos: int, tem_produto: bool,
                           tem_comodato: bool, metros_fibra: float,
                           cliente_nome: str, tecnico_nome: str) -> dict:
    """Aplica as regras e retorna dict com pontuação detalhada."""
    assunto_id = os["id_assunto"]
    regra = REGRAS.get(assunto_id)

    if not regra:
        return None  # Assunto sem regra definida

    txt = limpar_html(os.get("mensagem_resposta", ""))
    len_desc = len(txt)

    # Tempo de execução em minutos
    minutos_exec = 0
    dt_assumido  = os.get("data_hora_assumido")
    dt_exec      = os.get("data_hora_execucao")
    dt_final     = os.get("data_hora_execucao") or os.get("data_fechamento")
    dt_inicio    = os.get("data_hora_assumido") or os.get("data_inicio")
    if dt_inicio and dt_final and str(dt_inicio) != "0000-00-00 00:00:00":
        try:
            delta = dt_final - dt_inicio
            minutos_exec = delta.total_seconds() / 60
        except:
            minutos_exec = 0

    # App ligado = data_hora_execucao preenchida
    tem_app = bool(dt_exec and str(dt_exec) != "0000-00-00 00:00:00")

    # Aprovada = data_hora_analise preenchida
    dt_analise = os.get("data_hora_analise")
    aprovada   = bool(dt_analise and str(dt_analise) != "0000-00-00 00:00:00")

    # ── Penalidades ──────────────────────────────
    pen_foto     = 0
    pen_app      = 0
    pen_produto  = 0
    pen_desc     = 0
    bonus_tempo  = 0
    bonus_fibra  = 0
    pendencias   = []

    # Fotos
    min_fotos = regra.get("min_fotos", 0)
    if regra["pen_foto"] < 0 and fotos < max(min_fotos, 1):
        pen_foto = regra["pen_foto"]
        pendencias.append(f"Fotos insuficientes ({fotos}/{max(min_fotos,1)})")

    # App (deslocamento + execução)
    if regra["pen_app"] < 0 and not tem_app:
        pen_app = regra["pen_app"]
        pendencias.append("Sem deslocamento/execução no app")

    # Produto/comodato
    if regra.get("exige_comodato") and not tem_comodato:
        pen_produto = regra["pen_produto"]
        pendencias.append("Comodato não registrado")
    elif regra["pen_produto"] < 0 and not tem_produto:
        pen_produto = regra["pen_produto"]
        pendencias.append("Produto não registrado")

    # Descrição mínima
    min_desc = regra.get("min_desc", 0)
    if regra["pen_desc"] < 0 and min_desc > 0 and len_desc < min_desc:
        pen_desc = regra["pen_desc"]
        pendencias.append(f"Descrição muito curta ({len_desc}/{min_desc} chars)")

    # Bônus por tempo
    min_tempo = regra.get("min_tempo_bonus", 0)
    if min_tempo > 0 and regra.get("bonus_tempo", 0) != 0:
        if regra["bonus_tempo"] > 0 and minutos_exec >= min_tempo:
            bonus_tempo = regra["bonus_tempo"]
        elif regra["bonus_tempo"] < 0 and minutos_exec < min_tempo:
            bonus_tempo = regra["bonus_tempo"]
            pendencias.append(f"Tempo muito curto ({int(minutos_exec)}min < {min_tempo}min)")

    # Bônus por metragem de fibra
    min_fibra = regra.get("min_fibra_bonus", 0)
    if min_fibra > 0 and metros_fibra >= min_fibra:
        bonus_fibra = regra.get("bonus_tempo", 10)  # mesmo campo de bônus

    # ── Pontuação final ──────────────────────────
    pontos_final = (regra["base"] + pen_foto + pen_app +
                    pen_produto + pen_desc + bonus_tempo + bonus_fibra)
    pontos_final = max(0, pontos_final)  # não pode ser negativo

    return {
        "os_id":         os["id"],
        "tecnico_id":    os["id_tecnico"],
        "assunto_id":    assunto_id,
        "nome_assunto":  regra["nome"],
        "cliente_nome":  cliente_nome,
        "tecnico_nome":  tecnico_nome,
        "pontos_base":   regra["base"],
        "pontos_final":  pontos_final,
        "pen_foto":      pen_foto,
        "pen_app":       pen_app,
        "pen_produto":   pen_produto,
        "pen_descricao": pen_desc,
        "bonus_tempo":   bonus_tempo,
        "bonus_fibra":   bonus_fibra,
        "total_fotos":   fotos,
        "tem_produto":   int(tem_produto),
        "tem_comodato":  int(tem_comodato),
        "tem_app":       int(tem_app),
        "metros_fibra":  round(metros_fibra, 2),
        "minutos_exec":  round(minutos_exec, 1),
        "len_descricao": len_desc,
        "pendencias":    " | ".join(pendencias),
        "aprovada":      int(aprovada),
        "calculado_em":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ═══════════════════════════════════════════════════
# EXECUÇÃO PRINCIPAL
# ═══════════════════════════════════════════════════
def rodar_regras(data: str = None):
    criar_tabela()
    data = data or (datetime.now() + timedelta(hours=-3)).strftime("%Y-%m-%d")
    log.info(f"Calculando pontuação para {data}...")

    # Buscar OS finalizadas do dia no SQLite
    db = get_db()
    ids_tecnicos = {r["ixc_funcionario_id"]: r["id"]
                    for r in db.execute("SELECT id, ixc_funcionario_id FROM prod_tecnicos WHERE ativo=1").fetchall()}
    db.close()

    if not ids_tecnicos:
        log.error("Nenhum técnico ativo")
        return

    tec_ids_ixc = ",".join(str(i) for i in ids_tecnicos.keys())
    assunto_ids = ",".join(str(i) for i in REGRAS.keys())
    fibra_ids   = ",".join(str(i) for i in IDS_FIBRA)

    try:
        ixc = get_ixc()
        with ixc.cursor() as c:
            # OS finalizadas do dia com regra definida
            c.execute(f"""
                SELECT o.id, o.id_assunto, o.id_tecnico, o.id_cliente,
                       o.mensagem_resposta, o.data_hora_analise,
                       o.data_hora_execucao, o.data_hora_assumido,
                       o.data_inicio, o.data_fechamento, o.data_final
                FROM su_oss_chamado o
                WHERE o.id_tecnico IN ({tec_ids_ixc})
                  AND o.id_assunto IN ({assunto_ids})
                  AND o.status = 'F'
                  AND DATE(o.data_fechamento) = %s
            """, (data,))
            os_list = c.fetchall()

            if not os_list:
                log.info(f"Nenhuma OS finalizada em {data} com regra definida")
                ixc.close()
                return

            os_ids = ",".join(str(o["id"]) for o in os_list)

            # Fotos por OS
            c.execute(f"SELECT id_oss_chamado, COUNT(*) as total FROM su_oss_chamado_arquivos WHERE id_oss_chamado IN ({os_ids}) GROUP BY id_oss_chamado")
            fotos_map = {r["id_oss_chamado"]: r["total"] for r in c.fetchall()}

            # Produtos por OS
            c.execute(f"SELECT id_oss_chamado FROM movimento_produtos WHERE id_oss_chamado IN ({os_ids}) GROUP BY id_oss_chamado")
            prods_map = {r["id_oss_chamado"] for r in c.fetchall()}

            # Comodato por OS
            c.execute(f"SELECT id_oss_chamado FROM movimento_produtos WHERE id_oss_chamado IN ({os_ids}) AND status_comodato='E' GROUP BY id_oss_chamado")
            comodato_map = {r["id_oss_chamado"] for r in c.fetchall()}

            # Metragem de fibra por OS
            c.execute(f"""
                SELECT id_oss_chamado, SUM(quantidade) as metros
                FROM movimento_produtos
                WHERE id_oss_chamado IN ({os_ids})
                  AND id_produto IN ({fibra_ids})
                GROUP BY id_oss_chamado
            """)
            fibra_map = {r["id_oss_chamado"]: float(r["metros"] or 0) for r in c.fetchall()}

            # Nomes dos clientes
            cli_ids = ",".join(str(o["id_cliente"]) for o in os_list if o["id_cliente"])
            clientes_map = {}
            if cli_ids:
                c.execute(f"SELECT id, razao FROM cliente WHERE id IN ({cli_ids})")
                clientes_map = {r["id"]: r["razao"] for r in c.fetchall()}

            # Nomes dos técnicos
            func_ids = ",".join(str(o["id_tecnico"]) for o in os_list)
            c.execute(f"SELECT id, funcionario FROM funcionarios WHERE id IN ({func_ids})")
            func_map = {r["id"]: r["funcionario"] for r in c.fetchall()}

        ixc.close()

    except Exception as e:
        log.error(f"Erro ao consultar IXC: {e}")
        return

    # Calcular e salvar
    db = get_db()
    calculadas = salvas = erros = 0

    for os in os_list:
        try:
            os_id      = os["id"]
            fotos      = fotos_map.get(os_id, 0)
            tem_prod   = os_id in prods_map
            tem_comod  = os_id in comodato_map
            metros     = fibra_map.get(os_id, 0)
            cli_nome   = clientes_map.get(os["id_cliente"], "—")
            tec_nome   = func_map.get(os["id_tecnico"], "—")

            resultado = calcular_pontuacao_os(
                os, fotos, tem_prod, tem_comod, metros, cli_nome, tec_nome
            )
            if not resultado:
                continue

            calculadas += 1

            # Salvar/atualizar no SQLite
            db.execute("""
                INSERT OR REPLACE INTO sais_os_pontuacao
                    (os_id, tecnico_id, assunto_id, nome_assunto,
                     cliente_nome, tecnico_nome,
                     pontos_base, pontos_final,
                     pen_foto, pen_app, pen_produto, pen_descricao,
                     bonus_tempo, bonus_fibra,
                     total_fotos, tem_produto, tem_comodato, tem_app,
                     metros_fibra, minutos_exec, len_descricao,
                     pendencias, aprovada, calculado_em)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                resultado["os_id"],        resultado["tecnico_id"],
                resultado["assunto_id"],   resultado["nome_assunto"],
                resultado["cliente_nome"], resultado["tecnico_nome"],
                resultado["pontos_base"],  resultado["pontos_final"],
                resultado["pen_foto"],     resultado["pen_app"],
                resultado["pen_produto"],  resultado["pen_descricao"],
                resultado["bonus_tempo"],  resultado["bonus_fibra"],
                resultado["total_fotos"],  resultado["tem_produto"],
                resultado["tem_comodato"], resultado["tem_app"],
                resultado["metros_fibra"], resultado["minutos_exec"],
                resultado["len_descricao"],resultado["pendencias"],
                resultado["aprovada"],     resultado["calculado_em"],
            ))
            salvas += 1

        except Exception as e:
            log.error(f"Erro OS #{os.get('id')}: {e}")
            erros += 1

    db.commit()

    # Log de execução
    db.execute("""
        INSERT INTO sais_automacoes_log (automacao, status, itens_afetados, detalhe)
        VALUES ('regras_engine', 'ok', ?, ?)
    """, (salvas, f"calculadas:{calculadas} salvas:{salvas} erros:{erros}"))
    db.commit()
    db.close()

    log.info(f"Concluído: {calculadas} calculadas | {salvas} salvas | {erros} erros")
    return {"calculadas": calculadas, "salvas": salvas, "erros": erros}


if __name__ == "__main__":
    rodar_regras()
