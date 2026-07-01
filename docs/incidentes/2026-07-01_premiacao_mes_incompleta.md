# Incidente: Premiação do mês exibindo dados incompletos

**Data:** 01/07/2026

## Sintoma
Tela de Premiação (`/produtividade/premiacao`) mostrava apenas 2 técnicos
com poucos pontos ao filtrar o mês de junho inteiro, quando deveria trazer
todos os técnicos ativos com o volume real de OS do período.

## Causa raiz
1. O crontab do usuário `root` perdeu as linhas de `cron_sync_ixc` e
   `cron_destaques` por volta de 27/05/2026 (provável sobrescrita em um
   deploy anterior). O sync automático parou de rodar por ~5 semanas.
2. `prod_os_cache` ficou sem dados para os dias 02 a 27/06 (o sync `--full`
   só cobre uma janela de -3/+2 dias, não repõe histórico).
3. O serviço `hubprod_cliquedf` estava rodando um processo em memória
   desde 31/03/2026 (nunca reiniciado), então mesmo com o código já
   atualizado no disco (rota `/ws` incluída em commits posteriores),
   o processo ativo não tinha essa rota carregada — WebSocket retornava
   404 mesmo localmente.
4. Nginx do domínio `operacional.iatechhub.com.br`
   (`/etc/nginx/sites-available/operacional`) não tinha os headers de
   upgrade de conexão configurados para `/ws`.

## Correções aplicadas
- Crontab do HubProdutividade reinstalado (delta 15min, full diário 08h
  UTC, crítico 5min, destaques 22h UTC).
- Linha corrompida do HubCobrança (`cron_auditoria_retiradas`) separada
  em duas entradas válidas (estava concatenada com `cron_auditoria_retiradas`
  de novo na mesma linha, quebrando o parser do cron).
- Backfill retroativo de 01 a 30/06 via `backfill_junho.py`
  (novo script, ver `app/bootstrap/backfill_manual.py` — cópia adicionada
  ao repo).
- Bloco `location /ws` adicionado ao Nginx (`proxy_http_version 1.1`,
  `proxy_set_header Upgrade $http_upgrade`, `Connection "upgrade"`).
- `systemctl restart hubprod_cliquedf` para recarregar o código atual
  em memória.

## Débito técnico identificado (não corrigido ainda)
`sais_os_pontuacao.tecnico_id` usa `ixc_funcionario_id`, enquanto
`sais_destaques.tecnico_id` usa o `id` interno de `prod_tecnicos`.
Não quebra a tela hoje (o front casa por nome), mas é uma inconsistência
de schema que deveria ser padronizada.

## Prevenção
Considerar um monitoramento (`cron_saude` ou item no `watchdog_global.py`)
que alerte se `sais_sync.log` ou `sais_destaques.log` não tiverem uma
entrada nova nas últimas N horas — isso teria pego o problema em maio,
em vez de só ser notado em julho.
