## ADDED Requirements

### Requirement: Consumir mensagens em lote com estratégia drain-what's-available
O sistema SHALL implementar um consumer batch em goroutine externa (fora do Watermill Router) que consome o tópico `jobs` (consumer group `poc-batch`) usando a estratégia: bloqueia aguardando pelo menos 1 mensagem (ou timeout de 5s), depois drena o canal sem bloquear até o máximo de 10 mensagens ou até o canal estar vazio.

#### Scenario: Canal com menos mensagens que o máximo
- **WHEN** existem 3 mensagens disponíveis no tópico e o máximo é 10
- **THEN** sistema forma um lote de 3 mensagens e processa imediatamente, sem aguardar mais

#### Scenario: Canal com mais mensagens que o máximo
- **WHEN** existem 15 mensagens disponíveis no tópico e o máximo é 10
- **THEN** sistema forma um lote de 10 mensagens, processa, e na próxima iteração processa as 5 restantes

#### Scenario: Canal vazio após timeout
- **WHEN** nenhuma mensagem chega no tópico durante 5 segundos
- **THEN** sistema retorna lote vazio e reinicia o loop de espera sem processar

---

### Requirement: Tratamento de erro no lote com fallback individual
O sistema SHALL, quando o processamento de um lote falhar, tentar reprocessar cada mensagem do lote individualmente com retry manual (até 3 tentativas com backoff).

#### Scenario: Processamento do lote falha, individual bem-sucedido
- **WHEN** `processBatch()` retorna erro para um lote de N mensagens
- **THEN** sistema itera sobre cada mensagem do lote, chama `processIndividual()`, e faz ACK nas que tiveram sucesso

#### Scenario: Processamento individual falha após retries
- **WHEN** `processIndividual()` falha nas 3 tentativas para uma mensagem específica
- **THEN** sistema publica a mensagem no tópico `jobs_dlq` via `publisher.Publish()`, loga o evento de DLQ e faz ACK da mensagem original

---

### Requirement: ACK correto no consumer batch
O sistema SHALL garantir que toda mensagem consumida receba ACK (positivo), seja após processamento bem-sucedido ou após publicação na DLQ, para avançar o offset do consumer group `poc-batch`.

#### Scenario: Mensagem processada com sucesso
- **WHEN** mensagem é processada (individualmente ou em lote) com sucesso
- **THEN** sistema chama `msg.Ack()` para avançar o offset

#### Scenario: Mensagem publicada na DLQ
- **WHEN** mensagem é publicada na `jobs_dlq` após retries esgotados
- **THEN** sistema chama `msg.Ack()` após o `publisher.Publish()` para avançar o offset (não existe "devolver" mensagem no Kafka)
