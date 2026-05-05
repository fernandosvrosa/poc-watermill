## ADDED Requirements

### Requirement: Publicar mensagem individual via HTTP
O sistema SHALL expor o endpoint `POST /publish/single` que recebe um objeto JSON com `id` e `payload` e publica uma mensagem no tópico Kafka `jobs` via Watermill Publisher.

#### Scenario: Publicação bem-sucedida
- **WHEN** cliente envia `POST /publish/single` com body `{"id":"abc","payload":"..."}`
- **THEN** sistema publica a mensagem no tópico `jobs` e retorna HTTP 200 com `{"ok":true,"id":"abc"}`

#### Scenario: Body inválido
- **WHEN** cliente envia `POST /publish/single` com body malformado ou sem campo obrigatório
- **THEN** sistema retorna HTTP 400 sem publicar mensagem

---

### Requirement: Publicar lote de mensagens via HTTP com resultado por item
O sistema SHALL expor o endpoint `POST /publish/batch` que recebe um array JSON de mensagens e tenta publicar cada uma individualmente no tópico `jobs`, retornando HTTP 207 Multi-Status com o resultado de cada item.

#### Scenario: Todas as mensagens publicadas com sucesso
- **WHEN** cliente envia `POST /publish/batch` com array de N mensagens válidas
- **THEN** sistema retorna HTTP 207 com array de N resultados, todos com `"ok":true`

#### Scenario: Publicação parcialmente falha
- **WHEN** cliente envia `POST /publish/batch` e algumas mensagens falham ao publicar
- **THEN** sistema retorna HTTP 207 com resultado individual por mensagem, indicando `"ok":false` e `"error"` nas que falharam

#### Scenario: Array vazio
- **WHEN** cliente envia `POST /publish/batch` com array vazio `[]`
- **THEN** sistema retorna HTTP 207 com array vazio de resultados

---

### Requirement: Publisher Watermill injetado no contexto Echo
O sistema SHALL injetar o `watermill.Publisher` no contexto dos handlers Echo via middleware, desacoplando a camada HTTP da implementação de mensageria.

#### Scenario: Handler acessa publisher via contexto
- **WHEN** handler HTTP é chamado pelo Echo
- **THEN** handler recupera o publisher do contexto e chama `publisher.Publish(topic, msg)` sem importar o tipo concreto do broker
