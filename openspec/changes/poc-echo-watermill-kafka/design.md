## Context

Projeto novo (POC) em Go validando o Watermill como abstração de Pub/Sub sobre Kafka. O objetivo principal é demonstrar que o padrão de handlers e middlewares já utilizado com SQS/SNS pode ser reaproveitado trocando apenas o adapter de transporte. Não há código legado — todas as decisões partem do zero.

Stack escolhida: Go + Echo (HTTP) + Watermill + watermill-kafka/v3 (usa franz-go internamente) + Kafka em modo KRaft.

## Goals / Non-Goals

**Goals:**
- Validar Watermill como abstração portável de Pub/Sub (SQS/SNS → Kafka sem mudar handlers)
- Demonstrar dois padrões de consumo: sequencial (via Router) e batch (goroutine externa)
- Implementar Retry + DLQ automático no consumer sequencial via middleware Watermill
- Implementar Retry + DLQ manual no consumer batch com semântica correta de ACK
- Infraestrutura local reproduzível via Docker Compose (KRaft, sem Zookeeper)
- Graceful shutdown sem perda de mensagens em processamento

**Non-Goals:**
- Alta disponibilidade ou configuração multi-broker para produção
- Autenticação/autorização nos endpoints HTTP
- Persistência de estado além do próprio Kafka
- Observabilidade avançada (métricas Prometheus, tracing)
- Testes automatizados (unitários ou de integração)

## Decisions

### D1: watermill-kafka/v3 em vez de franz-go direto

**Decisão**: Usar `watermill-kafka/v3` como adapter Kafka, que usa franz-go internamente.

**Rationale**: O objetivo da POC é validar a abstração do Watermill — não construir um adapter customizado. O watermill-kafka/v3 expõe as opções franz-go via `AdditionalFranzOptions` quando necessário, dando controle sem perder a integração oficial.

**Alternativa descartada**: Implementar `watermill.Publisher`/`watermill.Subscriber` manualmente com franz-go — mais código, menos foco no objetivo da POC.

---

### D2: Consumer batch fora do Watermill Router

**Decisão**: O consumer batch usa `subscriber.Subscribe()` diretamente, em uma goroutine própria, fora do Router do Watermill.

**Rationale**: O Router do Watermill processa uma mensagem por vez por handler. Para implementar batch com semântica correta (ACK somente após processamento), é necessário controlar o loop de consumo externamente. Dentro do Router, o ACK seria forçado por mensagem, perdendo a atomicidade do lote.

**Alternativa descartada**: Handler no Router que acumula internamente e retorna `nil` imediatamente (ACK prematuro) — perde garantias de entrega se o processo morrer durante o processamento do lote.

---

### D3: Estratégia "drain what's available" para o batch

**Decisão**: O batch não espera acumular exatamente N mensagens. Bloqueia aguardando a primeira mensagem (ou timeout de 5s), depois drena o canal sem bloquear até `maxBatch` (10) ou até o canal estar vazio.

**Rationale**: Evita latência artificial em cenários de baixo volume. Um sistema que recebe 3 mensagens não deve esperar 7 a mais para processar.

**Implementação**: Dois `select` encadeados — o primeiro bloqueante (aguarda 1 msg ou timeout), o segundo não-bloqueante em loop (drena o canal).

---

### D4: DLQ manual no consumer batch

**Decisão**: Quando o processamento do lote falha, o batch tenta processar cada mensagem individualmente com retry manual (3x, backoff exponencial). Se o retry esgotar, publica na `jobs_dlq` via `publisher.Publish()` e faz ACK da mensagem original.

**Rationale**: No Kafka, não existe "devolver" uma mensagem — o offset avança sempre. A DLQ é uma publicação separada. O ACK após publicar na DLQ é obrigatório para avançar o offset do consumer group.

**Diferença do consumer sequencial**: O sequencial delega o retry e DLQ ao middleware do Watermill Router (automático). O batch não passa pelo Router, portanto implementa a mesma semântica manualmente.

---

### D5: Kafka KRaft (sem Zookeeper)

**Decisão**: Usar `confluentinc/cp-kafka` em modo KRaft com `KAFKA_PROCESS_ROLES: broker,controller`.

**Rationale**: Elimina a necessidade de um container Zookeeper separado. Mais simples para ambiente de desenvolvimento local. Requer `CLUSTER_ID` em base64 (22 chars) e formatação de storage na inicialização via `kafka-storage format`.

---

### D6: Publisher Watermill injetado no contexto Echo

**Decisão**: O `watermill.Publisher` é criado uma vez no `main.go` e injetado nos handlers Echo via middleware de contexto.

**Rationale**: Desacopla a camada HTTP da mensageria. Os handlers HTTP não precisam saber qual broker está por baixo — apenas chamam `publisher.Publish(topic, msg)`.

---

### D7: Tópico único "jobs" com dois consumer groups

**Decisão**: Ambos os consumers (sequencial e batch) consomem do mesmo tópico `jobs`, cada um com seu consumer group distinto (`poc-sequential` e `poc-batch`).

**Rationale**: Realista para demonstrar que múltiplos consumers independentes podem processar o mesmo stream de mensagens. Facilita observação no Redpanda Console (dois offsets independentes avançando).

## Risks / Trade-offs

| Risco | Mitigação |
|---|---|
| Mensagem perdida se processo morrer entre o processamento do lote e o ACK | Aceitável para POC; em produção usar transações Kafka ou `enable.auto.commit=false` com commit manual após ACK |
| Kafka KRaft requer formatação de storage no primeiro start | Usar `command` no Docker Compose para executar `kafka-storage format` antes de subir o broker |
| watermill-kafka/v3 API pode diferir da v2 (sem sarama) | Ler documentação atualizada; configurações de sarama não existem mais na v3 |
| Dois consumer groups consumindo o mesmo tópico dobram o throughput de processamento na POC | Intencional para demonstração; em produção cada grupo teria sua própria responsabilidade |
| Batch handler fora do Router não usa middlewares do Watermill | DLQ e retry implementados manualmente no batch handler com a mesma semântica |
