"""
SAIS — Implementação completa do sistema de pontuação por assunto.
Executar no servidor:
    cd /opt/automacoes/cliquedf/operacional
    source venv/bin/activate
    python3 /tmp/setup_pontuacao.py
"""
import sqlite3
from datetime import datetime

DB = "/opt/automacoes/cliquedf/operacional/prod_local.db"

# ═══════════════════════════════════════════════════
# DADOS DA PLANILHA ASSUNTOS.xlsx
# ═══════════════════════════════════════════════════
# (id_assunto_ixc, descricao, tipo, pontuacao, ativo)
ASSUNTOS_PONTUACAO = [
    (2,   'INSTALAÇÃO FTTH',                                     'serviço',   20, 1),
    (3,   'INSTALACAO EM REDE DE FIBRA',                         'infra',     20, 1),
    (5,   'INTERNET LENTA - 5',                                  'suporte',   10, 1),
    (6,   'RECOLHER ONU E PTO',                                  'retiradas', 10, 1),
    (7,   'MANUTENÇÃO',                                          'infra',     20, 1),
    (14,  'MIGRAÇAO DE TECNOLOGIA',                              'serviço',   20, 1),
    (15,  'REATIVAÇÃO',                                          'serviço',   20, 1),
    (16,  'MANUTENÇÃO',                                          'infra',     20, 1),
    (17,  'TROCA DE SENHA',                                      'suporte',   10, 1),
    (18,  'MUDANÇA DE EQUIPAMENTO DE CÔMODO',                    'serviço',   20, 1),
    (19,  'MUDANÇA DE ENDEREÇO',                                 'serviço',   20, 1),
    (20,  'SEM ACESSO',                                          'suporte',   10, 1),
    (21,  'INTERNET LENTA',                                      'suporte',   10, 1),
    (22,  'RECOLHIMENTO DE EQUIPAMENTO',                         'retiradas', 10, 1),
    (26,  'INSTALAÇÃO UTP',                                      'serviço',   20, 1),
    (27,  'CONFIGURAR ROTEADOR',                                 'suporte',   10, 1),
    (30,  'INSTALAR COMODATO',                                   'serviço',   10, 1),
    (39,  'RETIRADA DE EQUIPAMENTO',                             'retiradas', 10, 1),
    (40,  'RETIRADA DA FIBRA NA CTO',                            'retiradas', 10, 1),
    (44,  'REINCIDENCIA/SEM ACESSO',                             'suporte',   10, 1),
    (47,  'REINCIDENCIA/INTERNET LENTA',                         'suporte',   10, 1),
    (48,  'INSTALAR ROTEADOR COMPRADO NA LOJA',                  'serviço',   20, 1),
    (49,  'ATIVACAO FIBRA',                                      'serviço',   20, 1),
    (89,  'RETIRAR FIBRA',                                       'retiradas', 10, 1),
    (92,  'MUDANÇA DE TITULARIDADE',                             'serviço',   20, 1),
    (94,  '[CDF] SUPORTE  TECNICO',                              'suporte',   10, 1),
    (101, 'INSTALAR ROKU TV COMPRADO DA LOJA',                   'serviço',   10, 1),
    (102, 'INSTALAÇÃO ITTV  REMOTO',                             'suporte',   10, 1),
    (103, 'REICIDENCI/VERIFICAR INTERNET',                       'suporte',   10, 1),
    (104, 'INSTALAR APP NA RESIDENCIA',                          'suporte',   10, 1),
    (105, 'VERIFICAR ITTV',                                      'suporte',   10, 1),
    (107, 'REINCIDENCIA/ITTV',                                   'suporte',   10, 1),
    (110, 'MUDANÇA DE TITULARIDADE ( TECNICO)',                   'serviço',   20, 1),
    (111, 'RECOLHILMENTO DE EQUIPAMENTO( M. TITULARIDADE)',       'retiradas', 10, 1),
    (113, 'TROCA DE EQUIPAMENTOS',                               'suporte',   10, 1),
    (127, 'RECOLHER EQUIPAMENTO (ITTV)',                         'retiradas', 10, 1),
    (138, 'LANÇAMENTO DE FIBRA',                                 'infra',     20, 1),
    (142, 'MELHORIA DE SINAL CTO - REPROVADA',                   'infra',     20, 1),
    (143, 'MELHORIA DE SINAL CTO - CORRIGIDA',                   'infra',     20, 1),
    (145, 'MANUTENÇÃO DE REDES',                                 'infra',     20, 1),
    (146, 'MANUTENÇÃO DE REDES - ANALISE',                       'infra',     20, 1),
    (148, 'MANUTENÇÃO DE REDES - REINCIDENCIA',                  'infra',     20, 1),
    (151, 'CONSTRUÇÃO - INSTALAÇÃO DE FERRAGENS',                'infra',     20, 1),
    (152, 'CONSTRUÇÃO - ATIVAÇÃO DE FIBRA',                      'infra',     20, 1),
    (153, 'CONSTRUÇÃO - INSTALAÇÃO DE CAIXA DE DISTRIBUIÇÃO(CEO)','infra',    20, 1),
    (154, 'CONSTRUÇÃO - INSTALAÇÃO DE CAIXA DE ATENDIMENTOS(CTO)','infra',    20, 1),
    (155, 'CONSTRUÇÃO - REDE FTTH',                              'infra',     20, 1),
    (156, 'CONSTRUÇÃO - BACKBONE',                               'infra',     20, 1),
    (157, 'CONSTRUÇÃO - INSTALAÇÃO DE BATERIAS',                 'infra',     20, 1),
    (158, 'CONSTRUÇÃO - EQUIPAMENTOS',                           'infra',     20, 1),
    (159, 'MANUTENÇÃO - PREVENTIVA',                             'infra',     20, 1),
    (160, 'MANUTENÇÃO DE REDES - REINCIDENCIA',                  'infra',     20, 1),
    (161, 'MANUTENÇÃO DE REDES',                                 'infra',     20, 1),
    (162, 'MANUTENÇÃO -  REDE BACKBONE',                         'infra',     20, 1),
    (163, 'MANUTENÇÃO  - TROCA DE CTO',                          'infra',     20, 1),
    (164, 'MANUTENÇÃO  - CTO COM POTENCIA ALTA',                 'infra',     20, 1),
    (165, 'MANUTENÇÃO  - CTO QUEIMADA',                          'infra',     20, 1),
    (166, 'MANUTENÇÃO - ROMPIMENTO DE FTTH',                     'infra',     20, 1),
    (167, 'MANUTENÇÃO - POP OFFLINE',                            'infra',     20, 1),
    (168, 'MANUTENÇÃO - TROCA NOBREAK',                          'infra',     20, 1),
    (169, 'MANUTENÇÃO - REMANEJAMENTO DE CTO',                   'infra',     20, 1),
    (170, 'MANUTENÇÃO - ROMPIMENTO DE RAMAL',                    'infra',     20, 1),
    (171, 'MANUTENÇÃO - TROCA FONTE XPS',                        'infra',     20, 1),
    (172, 'MANUTENÇÃO - TROCA DE EQUIPAMENTO',                   'infra',     20, 1),
    (173, 'MANUTENÇÃO - RAMAL EPON OFFLINE',                     'infra',     20, 1),
    (174, 'MANUTENÇÃO - TROCA DE CEO',                           'infra',     20, 1),
    (175, 'MANUTENÇÃO - RAMAL GPON OFFLINE',                     'infra',     20, 1),
    (176, 'MANUTENÇÃO - TROCA DE DIO 48 FO',                     'infra',     20, 1),
    (177, 'MANUTENÇÃO - TROCA DE BANCO DE BATERIAS EXTACIONÁRIAS','infra',    20, 1),
    (178, 'MANUTENÇÃO - TROCA DE SPLITTER',                      'infra',     20, 1),
    (179, 'MANUTENÇÃO - TROCA DE OLT',                           'infra',     20, 1),
    (180, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 36FO',            'infra',     20, 1),
    (181, 'MANUTENÇÃO - TROCA DE DIO',                           'infra',     20, 1),
    (182, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 48FO',            'infra',     20, 1),
    (183, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 72FO',            'infra',     20, 1),
    (184, 'TROCA DE ADAPTADORES ÓPTICOS',                        'suporte',   10, 1),
    (185, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 144FO',           'infra',     20, 1),
    (186, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 12FO',            'infra',     20, 1),
    (187, 'MANUTENÇÃO - ROMPIMENTO DE BACKBONE 06FO',            'infra',     20, 1),
    (188, 'MANUTENÇÃO  - ROMPIMENTO BACKBONE',                   'infra',     20, 1),
    (203, 'TROCAR EQUIPAMENTO ALTERAÇÃO DE CONTRATO',            'suporte',   10, 1),
    (220, 'ATIVAÇÃO PLAYHUB (TECNICO)',                          'serviço',   10, 1),
    (221, '(ESTRUTURA) - BAIXA DE MATERIAL',                     'infra',     20, 1),
    (222, 'MANUTENÇÃO - TROCA DE RETIFICADORA',                  'infra',     20, 1),
    (227, '[OPC] ATIVAÇÃO FIBRA - NOVO',                         'serviço',   20, 1),
    (232, '[INFRA] ATIVAÇÃO FIBRA',                              'infra',     20, 1),
    (239, 'INSTALAR COMODATO_UP2025',                            'serviço',   20, 1),
    (240, 'FIBRA ROMPIDA',                                       'suporte',   20, 1),
    (242, 'MELHORIA DE BACKBONE (REMOÇÃO DE ATENUAÇÃO DE FIBRA)','infra',     20, 1),
    (243, 'ANCORAGEM ( TRANSFERENCIA DE BACKBONE )',              'infra',     20, 1),
    (244, 'MANUTENÇÃO - CORREÇÃO DE CTO',                        'infra',     20, 1),
    (245, 'FIBRA BAIXA',                                         'suporte',   10, 1),
]

def main():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")

    # 1. Criar tabela de pontuação
    print("1. Criando tabela prod_assuntos_pontuacao...")
    db.execute("""
        CREATE TABLE IF NOT EXISTS prod_assuntos_pontuacao (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id_assunto_ixc  INTEGER UNIQUE NOT NULL,
            descricao       TEXT,
            tipo            TEXT,
            pontuacao       INTEGER DEFAULT 0,
            ativo           INTEGER DEFAULT 1,
            atualizado_em   TEXT DEFAULT (datetime('now','-3 hours'))
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_pont_assunto ON prod_assuntos_pontuacao(id_assunto_ixc)")

    # 2. Popular com dados da planilha
    print("2. Inserindo pontuações...")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inserted = updated = 0
    for (id_ixc, desc, tipo, pts, ativo) in ASSUNTOS_PONTUACAO:
        existe = db.execute(
            "SELECT id FROM prod_assuntos_pontuacao WHERE id_assunto_ixc=?", (id_ixc,)
        ).fetchone()
        if existe:
            db.execute("""
                UPDATE prod_assuntos_pontuacao
                SET descricao=?, tipo=?, pontuacao=?, ativo=?, atualizado_em=?
                WHERE id_assunto_ixc=?
            """, (desc, tipo, pts, ativo, now, id_ixc))
            updated += 1
        else:
            db.execute("""
                INSERT INTO prod_assuntos_pontuacao
                    (id_assunto_ixc, descricao, tipo, pontuacao, ativo, atualizado_em)
                VALUES (?,?,?,?,?,?)
            """, (id_ixc, desc, tipo, pts, ativo, now))
            inserted += 1

    # 3. Atualizar configs de meta
    print("3. Atualizando metas no sais_config...")
    metas = [
        ('meta_dia_pontos',  '80',   'Meta de pontos por dia para a equipe'),
        ('meta_mes_pontos',  '1780', 'Meta de pontos por mês para a equipe'),
        ('meta_tec_dia',     '80',   'Meta de pontos por dia por técnico (padrão)'),
        ('meta_tec_mes',     '1780', 'Meta de pontos por mês por técnico (padrão)'),
    ]
    for (chave, valor, desc) in metas:
        db.execute(
            "INSERT OR REPLACE INTO sais_config (chave, valor, descricao) VALUES (?,?,?)",
            (chave, valor, desc)
        )

    # 4. Adicionar coluna pontos_meta nas metas de técnico (se não existir)
    try:
        db.execute("ALTER TABLE prod_metas ADD COLUMN tipo_meta TEXT DEFAULT 'os'")
        print("4. Coluna tipo_meta adicionada em prod_metas")
    except:
        print("4. Coluna tipo_meta já existe")

    # 5. Inserir metas de pontos para cada técnico
    print("5. Inserindo metas de pontos por técnico...")
    tecnicos = db.execute("SELECT id FROM prod_tecnicos WHERE ativo=1").fetchall()
    metas_inseridas = 0
    for t in tecnicos:
        existe_dia = db.execute("""
            SELECT id FROM prod_metas WHERE tecnico_id=? AND tipo='pontos_dia' AND vigente=1
        """, (t["id"],)).fetchone()
        if not existe_dia:
            db.execute("""
                INSERT INTO prod_metas (tecnico_id, tipo, valor, periodo, vigente)
                VALUES (?, 'pontos_dia', 80, 'diario', 1)
            """, (t["id"],))
            metas_inseridas += 1

        existe_mes = db.execute("""
            SELECT id FROM prod_metas WHERE tecnico_id=? AND tipo='pontos_mes' AND vigente=1
        """, (t["id"],)).fetchone()
        if not existe_mes:
            db.execute("""
                INSERT INTO prod_metas (tecnico_id, tipo, valor, periodo, vigente)
                VALUES (?, 'pontos_mes', 1780, 'mensal', 1)
            """, (t["id"],))
            metas_inseridas += 1

    db.commit()

    # 6. Verificação
    total = db.execute("SELECT COUNT(*) as t FROM prod_assuntos_pontuacao").fetchone()["t"]
    ativos = db.execute("SELECT COUNT(*) as t FROM prod_assuntos_pontuacao WHERE ativo=1").fetchone()["t"]
    p20 = db.execute("SELECT COUNT(*) as t FROM prod_assuntos_pontuacao WHERE pontuacao=20").fetchone()["t"]
    p10 = db.execute("SELECT COUNT(*) as t FROM prod_assuntos_pontuacao WHERE pontuacao=10").fetchone()["t"]

    # Teste rápido: calcular pontos das OS do dia atual
    teste = db.execute("""
        SELECT
            COUNT(*) as total_os,
            SUM(COALESCE(p.pontuacao, 0)) as total_pontos,
            COUNT(CASE WHEN p.id IS NULL THEN 1 END) as sem_pontuacao
        FROM prod_os_cache o
        LEFT JOIN prod_assuntos_pontuacao p ON p.id_assunto_ixc = o.ixc_assunto_id
        WHERE o.status = 'finalizada'
          AND DATE(o.data_fechamento, '+3 hours') = DATE('now', '-3 hours')
    """).fetchone()

    db.close()

    print("\n" + "="*50)
    print("✅ SETUP CONCLUÍDO")
    print("="*50)
    print(f"Assuntos: {total} total | {ativos} ativos")
    print(f"Pontuações: {p20} com 20pts | {p10} com 10pts")
    print(f"Inseridos: {inserted} | Atualizados: {updated}")
    print(f"Metas técnicos: {metas_inseridas} inseridas")
    print(f"\nTESTE (OS finalizadas hoje):")
    print(f"  OS: {teste['total_os']} | Pontos: {teste['total_pontos']} | Sem pontuação: {teste['sem_pontuacao']}")

if __name__ == "__main__":
    main()
