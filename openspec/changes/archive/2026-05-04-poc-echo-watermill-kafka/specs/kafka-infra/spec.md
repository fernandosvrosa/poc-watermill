## ADDED Requirements

### Requirement: Kafka em modo KRaft sem Zookeeper
O sistema SHALL configurar o broker Kafka usando `confluentinc/cp-kafka` em modo KRaft com `KAFKA_PROCESS_ROLES: broker,controller`, eliminando a necessidade de container Zookeeper.

#### Scenario: Kafka inicializa com KRaft
- **WHEN** `docker compose up` é executado
- **THEN** o container Kafka inicializa sem Zookeeper, formata o storage com o `CLUSTER_ID` configurado e fica disponível na porta `9092` (interna) e `9094` (host)

#### Scenario: Kafka acessível pela aplicação Go
- **WHEN** a aplicação Go tenta conectar em `kafka:9092`
- **THEN** a conexão é estabelecida com sucesso dentro da rede Docker

---

### Requirement: Painel de visualização Redpanda Console
O sistema SHALL subir o `redpandadata/console` no Docker Compose apontando para o broker Kafka, acessível no host na porta `8080`.

#### Scenario: Console exibe tópicos e consumer groups
- **WHEN** usuário acessa `http://localhost:8080` no browser
- **THEN** console exibe os tópicos `jobs` e `jobs_dlq` com seus offsets, e os consumer groups `poc-sequential` e `poc-batch` com seus respectivos lags

---

### Requirement: Configuração via variáveis de ambiente
O sistema SHALL ler todas as configurações de conexão e comportamento via variáveis de ambiente, com valores padrão sensatos para ambiente local.

#### Scenario: Variáveis obrigatórias presentes
- **WHEN** as variáveis `KAFKA_BROKERS`, `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC` estão definidas
- **THEN** a aplicação usa esses valores para configurar publisher, subscriber e router

#### Scenario: Variáveis com valores padrão
- **WHEN** variáveis opcionais como `KAFKA_BATCH_SIZE` e `KAFKA_BATCH_TIMEOUT` não estão definidas
- **THEN** a aplicação usa os padrões `10` e `5s` respectivamente

---

### Requirement: Aplicação Go no Docker Compose
O sistema SHALL incluir a aplicação Go como serviço no Docker Compose, com `depends_on` no Kafka e health check para garantir que o broker está pronto antes de conectar.

#### Scenario: Aplicação aguarda Kafka estar pronto
- **WHEN** `docker compose up` é executado
- **THEN** a aplicação Go só inicia após o health check do Kafka passar, evitando erros de conexão na inicialização
