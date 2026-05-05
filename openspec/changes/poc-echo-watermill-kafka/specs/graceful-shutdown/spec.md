## ADDED Requirements

### Requirement: Graceful shutdown coordenado em ordem segura
O sistema SHALL implementar shutdown coordenado ao receber `SIGTERM` ou `SIGINT`, respeitando a ordem: (1) parar Echo de receber novas requisições, (2) propagar cancelamento de contexto para Watermill Router e batch goroutine, (3) aguardar goroutines terminarem via `sync.WaitGroup`, (4) fechar publisher e subscriber Kafka.

#### Scenario: SIGTERM recebido durante processamento
- **WHEN** o processo recebe `SIGTERM` enquanto uma mensagem está sendo processada
- **THEN** o sistema aguarda o processamento em andamento terminar antes de fechar conexões Kafka, sem perda de mensagem

#### Scenario: Sequência de shutdown
- **WHEN** shutdown é iniciado
- **THEN** a sequência SHALL ser: `echo.Shutdown(ctx)` → `cancel()` → `wg.Wait()` → `publisher.Close()` → `subscriber.Close()`

---

### Requirement: Watermill Router encerrado via cancelamento de contexto
O sistema SHALL passar um `context.Context` cancelável para `router.Run(ctx)`, permitindo que o Router drene mensagens em processamento e encerre ao detectar o cancelamento.

#### Scenario: Router encerra após cancelamento
- **WHEN** o contexto é cancelado durante o shutdown
- **THEN** o Watermill Router para de consumir novas mensagens, termina os handlers em andamento e retorna do `router.Run()`

---

### Requirement: Batch goroutine encerrada via cancelamento de contexto
O sistema SHALL monitorar o cancelamento do contexto no loop principal da goroutine de batch, encerrando o consumo limpo ao detectar o cancelamento.

#### Scenario: Goroutine de batch encerra no shutdown
- **WHEN** o contexto é cancelado
- **THEN** a goroutine de batch termina o processamento do lote corrente (se houver), faz ACK das mensagens processadas e encerra o loop, sinalizando via `WaitGroup`
