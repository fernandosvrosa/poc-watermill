# POC: Go + Echo + Watermill + Kafka

Prova de conceito demonstrando o uso do [Watermill](https://watermill.io/) como abstração portável de Pub/Sub, com Echo para a camada HTTP e Kafka como broker de mensagens.

Este projeto implementa dois padrões de consumo:
- **Consumer sequencial**: processa uma mensagem por vez, com retry automático (3x) e Dead Letter Queue (DLQ)
- **Consumer em batch**: acumula mensagens e processa em lotes, com retry individual e DLQ manual

---

## Pré-requisitos

- **Docker** e **Docker Compose** instalados
  - Docker Desktop (Windows/macOS) ou Docker + Docker Compose (Linux)
  - Testado com Docker 20.10+ e Docker Compose 1.29+
- **Go 1.25+** (opcional, apenas se executar localmente sem Docker)
- **curl** ou similar para testar os endpoints (já disponível em macOS/Linux)

---

## Como Subir a Infraestrutura

### Passo 1: Clonar ou preparar o diretório

Assegure-se de estar no diretório raiz do projeto:
```bash
cd /caminho/para/poc-watermill/.worktrees/poc-echo-watermill-kafka
```

### Passo 2: Subir os containers

```bash
docker compose up --build
```

Isso irá:
1. Construir a imagem Docker da aplicação Go
2. Iniciar o Kafka em KRaft (sem Zookeeper)
3. Iniciar o Redpanda Console para visualizar tópicos e mensagens
4. Iniciar a aplicação Echo

### Passo 3: Aguardar a inicialização

O Kafka leva alguns segundos para ficar pronto. Você verá logs como:
```
poc-app    | 2026-05-04T23:15:30.000Z info servidor iniciado port=8090 topic=jobs
```

---

## Como Verificar a Infraestrutura

### Health Check da Aplicação

```bash
curl -X GET http://localhost:8090/health
```

Esperado:
```json
{"status":"ok"}
```

### Acessar o Redpanda Console

Abra no navegador: **http://localhost:8080**

Aqui você pode:
- Ver os tópicos criados (`jobs`, `jobs_dlq`)
- Inspecionar mensagens em tempo real
- Monitorar os consumer groups (`poc-sequential`, `poc-batch`)

### Verificar Logs da Aplicação

```bash
docker logs -f poc-app
```

---

## Exemplos de Uso

### Exemplo 1: Publicar uma mensagem simples (sucesso)

```bash
curl -X POST http://localhost:8090/publish/single \
  -H "Content-Type: application/json" \
  -d '{
    "id": "msg-001",
    "payload": "Processamento simples"
  }'
```

Resposta esperada:
```json
{"ok":true,"id":"msg-001"}
```

**O que acontece**:
- A mensagem é publicada no tópico `jobs`
- O consumer sequencial (`poc-sequential`) a consome imediatamente
- Como o payload não contém `"fail"`, é processada com sucesso
- O consumer batch (`poc-batch`) também a consome (offsets independentes)

---

### Exemplo 2: Publicar múltiplas mensagens em batch

```bash
curl -X POST http://localhost:8090/publish/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"id": "batch-001", "payload": "Primeira mensagem do lote"},
    {"id": "batch-002", "payload": "Segunda mensagem do lote"},
    {"id": "batch-003", "payload": "Terceira mensagem do lote"}
  ]'
```

Resposta esperada (HTTP 207 Multi-Status):
```json
[
  {"id":"batch-001","ok":true},
  {"id":"batch-002","ok":true},
  {"id":"batch-003","ok":true}
]
```

**O que acontece**:
- As três mensagens são publicadas no tópico `jobs`
- O consumer sequencial as processa uma por uma
- O consumer batch as acumula e processa em um lote (máx 10 mensagens ou após 5 segundos)

---

### Exemplo 3: Testar retry e DLQ no consumer sequencial

Este exemplo publica uma mensagem com `"fail"` no payload, acionando retry automático:

```bash
curl -X POST http://localhost:8090/publish/single \
  -H "Content-Type: application/json" \
  -d '{
    "id": "fail-seq-001",
    "payload": "Essa mensagem falha e será retentada 3 vezes"
  }'
```

Resposta esperada:
```json
{"ok":true,"id":"fail-seq-001"}
```

**O que acontece nos logs** (acompanhe com `docker logs -f poc-app`):
1. Consumer sequencial tenta processar → falha (payload contém `"fail"`)
2. Retry 1: tenta novamente após ~100ms → falha
3. Retry 2: tenta novamente após ~200ms → falha
4. Retry 3: tenta novamente após ~300ms → falha
5. Após 3 tentativas, o middleware `PoisonQueue` envia a mensagem para `jobs_dlq`

**Verificar no Redpanda Console**:
1. Acesse http://localhost:8080
2. Navegue para o tópico `jobs_dlq`
3. Você verá a mensagem de falha armazenada para análise posterior

---

### Exemplo 4: Testar retry e DLQ no consumer batch

Este exemplo publica uma mensagem com `"fail-batch"` no payload:

```bash
curl -X POST http://localhost:8090/publish/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"id": "batch-fail-001", "payload": "Sucesso normal"},
    {"id": "batch-fail-002", "payload": "fail-batch aqui - vai para DLQ"},
    {"id": "batch-fail-003", "payload": "Sucesso normal"}
  ]'
```

Resposta esperada:
```json
[
  {"id":"batch-fail-001","ok":true},
  {"id":"batch-fail-002","ok":true},
  {"id":"batch-fail-003","ok":true}
]
```

**O que acontece nos logs**:
1. Consumer batch acumula as 3 mensagens
2. Tenta processar o lote inteiro → falha (uma mensagem contém `"fail-batch"`)
3. Processa cada mensagem **individualmente**:
   - `batch-fail-001`: sucesso na tentativa 1
   - `batch-fail-002`: falha 3 vezes, enviada para `jobs_dlq`
   - `batch-fail-003`: sucesso na tentativa 1
4. Mensagens com sucesso são ACKed, mensagens em DLQ também

**Verificar no Redpanda Console**:
1. Acesse http://localhost:8080
2. Navegue para o tópico `jobs_dlq`
3. Você verá a mensagem `batch-fail-002` armazenada

---

### Exemplo 5: Teste integrado com múltiplas falhas

```bash
# Publica um lote com falhas e sucessos
curl -X POST http://localhost:8090/publish/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"id": "test-1", "payload": "OK"},
    {"id": "test-2", "payload": "fail"},
    {"id": "test-3", "payload": "fail-batch"},
    {"id": "test-4", "payload": "OK"}
  ]'
```

**Resultado esperado**:
- `test-1` e `test-4`: processadas com sucesso por ambos os consumers
- `test-2`: retentada 3x no consumer sequencial → DLQ
- `test-3`: retentada 3x no consumer batch → DLQ

Ambas as DLQs estarão visíveis no Redpanda Console.

---

## Arquitetura e Decisões de Design

### 1. KRaft vs Zookeeper

**Decisão**: Usar **KRaft** (kraft mode) sem Zookeeper.

**Motivo**:
- KRaft é o modo padrão moderno do Kafka (v3.0+)
- Simplifica a infraestrutura local (um container em vez de dois)
- Mais rápido para inicializar e testar
- Adequado para ambientes de desenvolvimento

**Configuração** (docker-compose.yml):
```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

---

### 2. Watermill-Kafka/v3 vs Client Kafka Direto

**Decisão**: Usar **watermill-kafka/v3** (adapter Watermill para Kafka).

**Detalhe de implementação**: o `watermill-kafka/v3` usa **IBM/sarama** como client Kafka internamente (não franz-go). A configuração de baixo nível é feita via `kafka.DefaultSaramaSubscriberConfig()` e `OverwriteSaramaConfig`.

**Motivo**:
- O objetivo da POC é validar a abstração Watermill, não construir um adapter customizado
- Watermill abstrai Publisher e Subscriber, permitindo trocar o broker sem mudar os handlers
- Implementar o client Kafka (sarama ou franz-go) diretamente não teria essa portabilidade
- Os middlewares (Retry, PoisonQueue) operam na camada Watermill

**Implicação**:
- Handlers (`SequentialHandler`, `ProcessBatch`) são agnósticos ao broker
- Para usar SQS/SNS, basta trocar o adapter (veja seção "Trocar o Broker de Mensagens")

---

### 3. Consumer Batch Fora do Watermill Router

**Decisão**: Implementar consumer batch como uma goroutine separada, **fora do Watermill Router**.

**Motivo**:
- O Router processa uma mensagem por vez (semântica padrão)
- Para garantir que as mensagens sejam ACKed **após** processar o lote inteiro (não após tentar), precisamos controlar o loop manualmente
- Se usássemos o Router, não teríamos controle fino sobre quando fazer ACK após sucesso/falha do lote
- Separar permite que ambos os consumers (sequencial e batch) operem independentemente com seus próprios offsets

**Implementação** (cmd/server/main.go):
```go
wg.Add(1)
go func() {
    defer wg.Done()
    if err := router.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("router encerrado: %v", err)
    }
}()

wg.Add(1)
go func() {
    defer wg.Done()
    if err := batchConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Printf("batch consumer encerrado: %v", err)
    }
}()
```

---

### 4. Estratégia Drain-What's-Available para Lotes

**Decisão**: O consumer batch **não aguarda acumular exatamente N mensagens**; processa o que está disponível agora (até máx N).

**Motivo**:
- Evita latência artificial em baixo volume (não fica aguardando encher o lote)
- Aproveita mensagens que já chegaram, mas não bloqueia
- Usa timeout (`5s` por padrão) para não ficar infinitamente esperando
- Melhor experiência em testes e baixa carga

**Implementação** (internal/batch/accumulator.go):
```go
// NextBatch retorna:
// - Até maxSize mensagens se houver
// - Menos mensagens se timeout for atingido
// - Nenhuma se contexto for cancelado
```

---

### 5. DLQ Manual vs Automático

**Decisão**: 
- Consumer **sequencial**: DLQ automático via middleware `PoisonQueue`
- Consumer **batch**: DLQ manual (implementado em `ProcessIndividual`)

**Motivo**:
- Sequencial usa o Router, que tem acesso aos middlewares
- Batch está fora do Router, então implementa a mesma semântica manualmente
- Ambos garantem que mensagens com falha vão para `jobs_dlq` após 3 tentativas

**Implementação automática** (internal/messaging/router.go):
```go
poisonMiddleware, err := middleware.PoisonQueue(publisher, dlqTopic)
router.AddMiddleware(
    middleware.Retry{MaxRetries: 3, ...}.Middleware,
    poisonMiddleware,
)
```

**Implementação manual** (internal/handler/batch.go):
```go
for attempt := 1; attempt <= maxAttempts; attempt++ {
    if err := processMessage(msg); err == nil {
        return nil
    }
}
// Após 3 tentativas, publica no DLQ:
if err := publisher.Publish(dlqTopic, msg); err != nil { ... }
```

---

### 6. Ordem dos Middlewares: Retry → PoisonQueue

**Decisão**: O middleware Retry envolve o PoisonQueue.

**Motivo**:
- Retry tenta o handler 3 vezes (internamente)
- Se todas falharem, o Retry propaga o erro para cima
- PoisonQueue intercepta esse erro e envia para DLQ
- Ordem: Mensagem → Retry (3x) → Falha → PoisonQueue → DLQ

**Implementação** (internal/messaging/router.go):
```go
router.AddMiddleware(
    middleware.Retry{...}.Middleware,       // Tenta 3x
    poisonMiddleware,                        // Envia ao DLQ se falhar
)
```

---

### 7. Tópico Único `jobs` com Dois Consumer Groups

**Decisão**: Ambos os consumers consomem do **mesmo tópico** `jobs`, com **dois consumer groups distintos**.

**Motivo**:
- Cada consumer group mantém seus próprios offsets
- `poc-sequential`: processa mensagens uma por uma
- `poc-batch`: processa em lotes
- Ambos recebem todas as mensagens, independentemente
- Simula o caso de uso real onde múltiplas aplicações consomem do mesmo evento

**Offsets independentes**:
```
Tópico: jobs
├── Consumer group "poc-sequential" → offset 42
└── Consumer group "poc-batch" → offset 39 (processa mais devagar em batches)
```

---

## Trocar o Broker de Mensagens (Portabilidade Watermill)

A principal vantagem do Watermill é que **os handlers e lógica de business não mudam** quando você trocar o broker. Segue como:

### De Kafka para AWS SQS/SNS

**Passo 1**: Instale o adapter Watermill para AWS
```bash
go get github.com/ThreeDotsLabs/watermill-aws/v2
```

**Passo 2**: Troque os construtores de Publisher/Subscriber
```go
// Antes (Kafka):
publisher, err := messaging.NewPublisher(cfg)
subscriber, err := messaging.NewSubscriber(cfg)

// Depois (SQS):
import "github.com/ThreeDotsLabs/watermill-aws/v2/pkg/sqs"

publisherConfig := sqs.PublisherConfig{
    AWSConfig: awsConfig,
    QueueName: "jobs-queue",
}
publisher, err := sqs.NewPublisher(publisherConfig, logger)

subscriberConfig := sqs.SubscriberConfig{
    AWSConfig:     awsConfig,
    QueueName:     "jobs-queue",
    ConsumerGroup: "poc-sequential",
}
subscriber, err := sqs.NewSubscriber(subscriberConfig, logger)
```

**Passo 3**: O resto do código **continua igual**
```go
// Handlers - sem mudanças
handler.SequentialHandler(msg)

// Router - sem mudanças
router, err := messaging.NewRouter(publisher, subscriber, topic, dlqTopic, handler.SequentialHandler)

// Middlewares (Retry, PoisonQueue) - sem mudanças
router.AddMiddleware(
    middleware.Retry{...}.Middleware,
    poisonMiddleware,
)
```

A abstração do Watermill garante que:
- `message.Publisher.Publish()` funciona com qualquer broker
- `message.Subscriber.Subscribe()` funciona com qualquer broker
- Os middlewares funcionam independentemente do broker
- Os handlers só veem `*message.Message`, não sabem qual broker é usado

### Adapters Suportados

Watermill tem adapters para:
- Kafka (watermill-kafka)
- RabbitMQ (watermill-amqp)
- AWS SQS/SNS (watermill-aws)
- Google Pub/Sub (watermill-googlecloud)
- Azure Service Bus (watermill-azureservicebus)
- NATS (watermill-nats)
- Redis Streams (watermill-redisstream)
- SQL (watermill-sql)

---

## Estrutura do Projeto

```
.
├── cmd/
│   └── server/
│       └── main.go                  # Entrada da aplicação
├── internal/
│   ├── batch/
│   │   └── accumulator.go           # Acumula mensagens para batch
│   ├── config/
│   │   └── config.go                # Lê variáveis de ambiente
│   ├── handler/
│   │   ├── batch.go                 # Lógica de processamento batch
│   │   ├── http.go                  # Handlers HTTP (publish, health)
│   │   └── sequential.go            # Lógica de processamento sequencial
│   └── messaging/
│       ├── batch_consumer.go        # Consumer batch (fora do Router)
│       ├── publisher.go             # Cria Publisher Watermill
│       ├── router.go                # Configura Router com middlewares
│       └── subscriber.go            # Cria Subscribers Watermill
├── docker-compose.yml               # Infraestrutura (Kafka, Console, App)
├── Dockerfile                       # Build da aplicação Go
├── .env                             # Variáveis de ambiente
├── go.mod                           # Dependências Go
├── go.sum                           # Checksums de dependências
└── README.md                        # Este arquivo
```

---

## Variáveis de Ambiente

Configure no `.env` ou passe diretamente ao `docker compose up`:

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `KAFKA_BROKERS` | `kafka:9092` | Lista de brokers Kafka (separados por vírgula) |
| `KAFKA_TOPIC` | `jobs` | Tópico principal de mensagens |
| `KAFKA_DLQ_TOPIC` | `jobs_dlq` | Tópico de Dead Letter Queue |
| `KAFKA_CONSUMER_GROUP_SEQUENTIAL` | `poc-sequential` | Consumer group sequencial |
| `KAFKA_CONSUMER_GROUP_BATCH` | `poc-batch` | Consumer group batch |
| `KAFKA_BATCH_SIZE` | `10` | Máximo de mensagens por lote |
| `KAFKA_BATCH_TIMEOUT` | `5s` | Timeout para esperar mensagens (formato Go duration) |
| `APP_PORT` | `8090` | Porta HTTP da aplicação |

**Exemplo**: Alterar tamanho do lote
```bash
KAFKA_BATCH_SIZE=20 docker compose up
```

---

## Endpoints HTTP

### POST /publish/single

Publica uma mensagem simples.

**Request**:
```json
{
  "id": "msg-001",
  "payload": "Conteúdo da mensagem"
}
```

**Response** (200 OK):
```json
{
  "ok": true,
  "id": "msg-001"
}
```

**Response** (400 Bad Request):
```json
{
  "error": "campos 'id' e 'payload' são obrigatórios"
}
```

---

### POST /publish/batch

Publica múltiplas mensagens.

**Request**:
```json
[
  {"id": "msg-001", "payload": "Primeira"},
  {"id": "msg-002", "payload": "Segunda"}
]
```

**Response** (207 Multi-Status):
```json
[
  {"id": "msg-001", "ok": true},
  {"id": "msg-002", "ok": true}
]
```

Cada item pode ter `"ok": true` ou `"error": "descrição"`.

---

### GET /health

Health check simples.

**Response** (200 OK):
```json
{
  "status": "ok"
}
```

---

## Monitoramento com Redpanda Console

Após subir a infraestrutura, acesse **http://localhost:8080** para:

### Ver Tópicos
1. Clique em **Brokers** → **Topics**
2. Você verá `jobs` e `jobs_dlq`

### Inspecionar Mensagens
1. Clique em um tópico (ex: `jobs`)
2. Veja as mensagens em tempo real
3. Clique em uma mensagem para ver o payload completo

### Monitorar Consumer Groups
1. Clique em **Consumer Groups**
2. Veja `poc-sequential` e `poc-batch`
3. Acompanhe o lag (diferença entre última mensagem e último offset consumido)

### Acompanhar DLQ
1. Clique no tópico `jobs_dlq`
2. Veja todas as mensagens que falharam 3 vezes
3. Útil para troubleshooting e análise de erros

---

## Troubleshooting

### Aplicação não consegue conectar ao Kafka

**Erro**: `failed to dial all brokers`

**Solução**:
1. Verifique se o Kafka está rodando: `docker ps | grep poc-kafka`
2. Verifique os logs do Kafka: `docker logs poc-kafka | tail -20`
3. Aguarde a inicialização (pode levar 30-60 segundos)

---

### Mensagens não aparecem no Redpanda Console

**Causa**: Lag de atualização (o console não atualiza em tempo real)

**Solução**:
1. Atualize a página (F5)
2. Ou use `curl` para confirmar que as mensagens foram publicadas

---

### Consumer batch não processa em tempo real

**Comportamento esperado**: Consumer batch aguarda até 5 segundos ou 10 mensagens (configurável) antes de processar.

**Se quiser processar mais rápido**:
```bash
KAFKA_BATCH_TIMEOUT=1s docker compose up
```

---

### Ver logs de toda a stack

```bash
docker compose logs -f
```

Ou apenas de um serviço:
```bash
docker compose logs -f app        # Application
docker compose logs -f kafka      # Kafka
docker compose logs -f console    # Redpanda Console
```

---

## Parar a Infraestrutura

```bash
docker compose down
```

Isso para e remove os containers. Para manter o estado:
```bash
docker compose stop
docker compose start  # mais tarde
```

Para remover tudo incluindo volumes (limpa as mensagens):
```bash
docker compose down -v
```

---

## Próximos Passos

### Para Desenvolvimento
1. Modifique os handlers em `internal/handler/`
2. Teste localmente ou via Docker
3. Use `docker compose logs -f` para acompanhar

### Para Produção
1. Escolha um broker de mensagens (SQS, Kafka, etc.)
2. Mude o adapter Watermill no `internal/messaging/publisher.go` e `subscriber.go`
3. Ajuste as variáveis de ambiente para o ambiente de produção
4. Configure monitoramento (logs, métricas, alertas)
5. Implemente circuit breakers e fallbacks para DLQ

### Para Escalar
1. Aumente `KAFKA_BATCH_SIZE` para lotes maiores
2. Aumente `KAFKA_BATCH_TIMEOUT` para latência menor (trade-off com throughput)
3. Implemente workers adicionais para paralelismo
4. Use Kafka partitions para distribuição entre múltiplos workers

---

## Referências

- [Watermill Documentation](https://watermill.io/)
- [Watermill Kafka Adapter](https://github.com/ThreeDotsLabs/watermill-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Redpanda Console](https://docs.redpanda.com/docs/manage/console/)
- [Echo Web Framework](https://echo.labstack.com/)

---

**Última atualização**: 2026-05-04
