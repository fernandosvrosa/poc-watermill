## 1. Infraestrutura Docker Compose

- [x] 1.1 Criar `docker-compose.yml` com Kafka em modo KRaft (`confluentinc/cp-kafka`), sem Zookeeper, com `CLUSTER_ID` fixo e formatação de storage no `command`
- [x] 1.2 Adicionar portas: `9092` (rede interna Docker), `9094` (acesso do host para testes curl)
- [x] 1.3 Adicionar serviço `redpandadata/console` apontando para `kafka:9092`, exposto na porta `8080` do host
- [x] 1.4 Adicionar serviço `app` (Go) com `depends_on` e health check no Kafka
- [x] 1.5 Criar arquivo `.env` com variáveis padrão (`KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC`, `KAFKA_CONSUMER_GROUP_SEQUENTIAL`, `KAFKA_CONSUMER_GROUP_BATCH`, `KAFKA_BATCH_SIZE`, `KAFKA_BATCH_TIMEOUT`, `APP_PORT`)

## 2. Projeto Go — Base

- [x] 2.1 Inicializar módulo Go (`go mod init`) e adicionar dependências: `echo/v4`, `watermill`, `watermill-kafka/v3`
- [x] 2.2 Criar `internal/config/config.go` que lê todas as variáveis de ambiente com valores padrão via `os.Getenv`
- [x] 2.3 Criar `Dockerfile` multi-stage para build da aplicação Go

## 3. Mensageria — Publisher e Subscriber

- [x] 3.1 Criar `internal/messaging/publisher.go` com função que inicializa `kafka.NewPublisher` (watermill-kafka/v3) usando config
- [x] 3.2 Criar `internal/messaging/subscriber.go` com função que inicializa `kafka.NewSubscriber` para o consumer sequencial (consumer group `poc-sequential`)
- [x] 3.3 Criar segunda instância de subscriber para o consumer batch (consumer group `poc-batch`)

## 4. Consumer Sequencial — Watermill Router

- [x] 4.1 Criar `internal/handler/sequential.go` com função handler que processa mensagem e loga sucesso; retorna erro simulado quando `payload` contém `"fail"` (para testar DLQ)
- [x] 4.2 Criar `internal/messaging/router.go` que configura o Router com middlewares na ordem correta: `Retry{MaxRetries:3, InitialInterval:100ms}.Middleware` → `PoisonQueue(publisher, "jobs_dlq")`
- [x] 4.3 Registrar o handler sequencial no Router com `router.AddHandler("sequential", "jobs", subscriber, "", nil, sequentialHandler)`
- [x] 4.4 Adicionar log estruturado quando mensagem vai para DLQ (interceptar via `middleware.PoisonQueueWithFilter` ou log no handler)

## 5. Consumer Batch — Goroutine Externa

- [x] 5.1 Criar `internal/batch/accumulator.go` com função `NextBatch(ctx, msgChan, maxSize, timeout)` que implementa a estratégia drain-what's-available: `select` bloqueante para primeira msg, loop `select/default` não-bloqueante para drenar até `maxSize`
- [x] 5.2 Criar `internal/handler/batch.go` com `ProcessBatch(msgs)` que simula processamento do lote; retorna erro quando qualquer msg do lote contém `"fail-batch"`
- [x] 5.3 Implementar `ProcessIndividual(msg)` com retry manual (3x, backoff 100ms*tentativa); se esgotar, publicar em `jobs_dlq` e logar evento de DLQ
- [x] 5.4 Criar `internal/messaging/batch_consumer.go` com goroutine principal que: subscreve em `jobs`, chama `NextBatch()` em loop, tenta `ProcessBatch()`, faz fallback para `ProcessIndividual()` em caso de erro, e encerra limpo ao cancelar contexto
- [x] 5.5 Garantir `msg.Ack()` em todos os caminhos de saída (sucesso, DLQ)

## 6. Camada HTTP — Echo

- [x] 6.1 Criar `internal/handler/http.go` com middleware Echo que injeta o `watermill.Publisher` no contexto
- [x] 6.2 Implementar handler `PublishSingle(c echo.Context)`: deserializa body, chama `publisher.Publish("jobs", msg)`, retorna 200 ou 400
- [x] 6.3 Implementar handler `PublishBatch(c echo.Context)`: itera sobre array de mensagens, tenta publicar cada uma, acumula resultados, retorna 207 Multi-Status com `[{"id":"...","ok":true/false,"error":"..."}]`
- [x] 6.4 Adicionar endpoint `GET /health` que retorna 200 (usado pelo Docker Compose health check e Kubernetes)

## 7. Main — Orquestração e Graceful Shutdown

- [x] 7.1 Criar `main.go` que inicializa config, publisher, subscribers, router, batch consumer e servidor Echo
- [x] 7.2 Implementar captura de `SIGTERM`/`SIGINT` via `signal.NotifyContext`
- [x] 7.3 Implementar graceful shutdown na ordem: `echo.Shutdown(ctx)` → `cancel()` → `wg.Wait()` → `publisher.Close()` → `subscriber.Close()`
- [x] 7.4 Usar `sync.WaitGroup` para aguardar `router.Run()` e `batchConsumer.Run()` encerrarem antes de fechar conexões

## 8. Documentação — README e Decisões de Arquitetura

- [x] 8.1 Criar `README.md` com seção de pré-requisitos (Docker, Go) e comandos para subir o ambiente (`docker compose up`)
- [x] 8.2 Documentar todos os endpoints com exemplos `curl` para fluxo de sucesso (`/publish/single`, `/publish/batch`)
- [x] 8.3 Documentar comandos `curl` para acionar o fluxo de falha e validar DLQ (enviar payload com `"fail"` para o sequential e `"fail-batch"` para o batch)
- [x] 8.4 Documentar as decisões de arquitetura tomadas durante a exploração: KRaft vs Zookeeper, watermill-kafka/v3 vs franz-go direto, batch fora do Router, estratégia drain-what's-available, DLQ manual vs automático, ordem dos middlewares Retry+PoisonQueue, tópico único com dois consumer groups
- [x] 8.5 Adicionar seção explicando como trocar o adapter Kafka por SQS/SNS (demonstrando o valor da abstração Watermill)
