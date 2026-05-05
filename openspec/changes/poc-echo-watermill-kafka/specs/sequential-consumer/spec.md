## ADDED Requirements

### Requirement: Processar mensagens sequencialmente via Watermill Router
O sistema SHALL registrar um handler no Watermill Router que consome o tópico `jobs` (consumer group `poc-sequential`) e processa uma mensagem por vez, logando sucesso ao completar.

#### Scenario: Processamento bem-sucedido
- **WHEN** uma mensagem chega no tópico `jobs`
- **THEN** o handler processa a mensagem, loga `{"level":"info","msg":"mensagem processada","id":"..."}` e retorna `nil`

#### Scenario: Processamento falha
- **WHEN** o handler retorna erro ao processar uma mensagem
- **THEN** o middleware de Retry tenta reprocessar até 3 vezes com intervalo inicial de 100ms

---

### Requirement: Retry automático com backoff
O sistema SHALL configurar o middleware `watermill/middleware.Retry` no Router com `MaxRetries: 3` e `InitialInterval: 100ms`.

#### Scenario: Retry esgotado
- **WHEN** o handler falha nas 3 tentativas de retry
- **THEN** o middleware PoisonQueue intercepta o erro e encaminha a mensagem para o tópico `jobs_dlq`

---

### Requirement: DLQ automático via PoisonQueue middleware
O sistema SHALL configurar o middleware `watermill/middleware.PoisonQueue` no Router para publicar mensagens que esgotaram o retry no tópico `jobs_dlq`.

#### Scenario: Mensagem enviada para DLQ
- **WHEN** uma mensagem esgota todas as tentativas de retry
- **THEN** sistema publica a mensagem em `jobs_dlq`, loga `{"level":"warn","msg":"mensagem enviada para DLQ","id":"...","topic":"jobs_dlq"}` e faz ACK da mensagem original

#### Scenario: Ordem dos middlewares
- **WHEN** o Router é configurado
- **THEN** a ordem DEVE ser: `Retry.Middleware` primeiro, `PoisonQueue` segundo — garantindo que o Retry envolve o PoisonQueue na cadeia
