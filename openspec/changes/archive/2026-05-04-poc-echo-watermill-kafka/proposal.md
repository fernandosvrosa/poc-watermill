## Why

Validar o Watermill como camada de abstração de Pub/Sub sobre Kafka, reutilizando padrões já estabelecidos com SQS/SNS. A POC deve demonstrar que a troca de broker (SQS → Kafka) exige mudança apenas no adapter, sem alterar a lógica de negócio dos handlers.

## What Changes

- Novo projeto Go com infraestrutura Docker Compose (Kafka KRaft + Redpanda Console)
- Dois endpoints HTTP via Echo: `POST /publish/single` e `POST /publish/batch` (207 Multi-Status)
- Publisher Watermill injetado no contexto Echo para desacoplamento entre camada HTTP e mensageria
- Consumer sequencial via Watermill Router com middleware Retry (3x) + PoisonQueue automático → `jobs_dlq`
- Consumer batch via goroutine externa com estratégia "drain what's available" (máx 10 msgs, sem esperar acumular)
- DLQ manual no consumer batch: retry individual por mensagem, fallback para `jobs_dlq` após esgotar tentativas
- Graceful shutdown coordenado: Echo → cancelamento de contexto → WaitGroup → close de publisher/subscriber
- Configuração via variáveis de ambiente; `.env` no Docker Compose

## Capabilities

### New Capabilities

- `http-publisher`: Endpoints Echo que publicam mensagens no Kafka via Watermill Publisher injetado no contexto
- `sequential-consumer`: Handler Watermill Router com Retry + PoisonQueue para processamento um-a-um com DLQ automático
- `batch-consumer`: Goroutine de consumo em lote com drain-what's-available, retry individual e DLQ manual
- `kafka-infra`: Docker Compose com Kafka KRaft (sem Zookeeper) e Redpanda Console
- `graceful-shutdown`: Encerramento coordenado de Echo e Watermill Router sem perda de mensagens em processamento

### Modified Capabilities

## Impact

- **Dependências Go**: `github.com/labstack/echo/v4`, `github.com/ThreeDotsLabs/watermill`, `github.com/ThreeDotsLabs/watermill-kafka/v3`
- **Infraestrutura**: Docker Compose com `confluentinc/cp-kafka` (KRaft mode) e `redpandadata/console`
- **Tópicos Kafka**: `jobs` (consumido por dois consumer groups) e `jobs_dlq`
- **Sem impacto em sistemas existentes**: projeto novo, isolado
