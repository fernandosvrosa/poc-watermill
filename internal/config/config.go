package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contém todas as configurações da aplicação lidas de variáveis de ambiente.
type Config struct {
	// KafkaBrokers lista de brokers Kafka (KAFKA_BROKERS)
	KafkaBrokers []string
	// KafkaTopic tópico principal de mensagens (KAFKA_TOPIC)
	KafkaTopic string
	// KafkaDLQTopic tópico de dead-letter queue (KAFKA_DLQ_TOPIC)
	KafkaDLQTopic string
	// KafkaConsumerGroupSeq grupo de consumidores sequencial (KAFKA_CONSUMER_GROUP_SEQUENTIAL)
	KafkaConsumerGroupSeq string
	// KafkaConsumerGroupBatch grupo de consumidores em batch (KAFKA_CONSUMER_GROUP_BATCH)
	KafkaConsumerGroupBatch string
	// KafkaBatchSize tamanho do lote para processamento em batch (KAFKA_BATCH_SIZE)
	KafkaBatchSize int
	// KafkaBatchTimeout tempo máximo de espera para completar um lote (KAFKA_BATCH_TIMEOUT)
	KafkaBatchTimeout time.Duration
	// AppPort porta HTTP do servidor (APP_PORT)
	AppPort string
}

// getEnv retorna o valor da variável de ambiente ou o valor padrão caso não esteja definida.
func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// Load lê as variáveis de ambiente e retorna uma Config populada com os valores
// ou os defaults quando as variáveis não estão definidas.
func Load() Config {
	// Lê os brokers separados por vírgula
	brokersRaw := getEnv("KAFKA_BROKERS", "localhost:9094")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	// Lê o tamanho do batch como inteiro
	batchSize := 10
	if raw := os.Getenv("KAFKA_BATCH_SIZE"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}

	// Lê o timeout do batch como duration
	batchTimeout := 5 * time.Second
	if raw := os.Getenv("KAFKA_BATCH_TIMEOUT"); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			batchTimeout = parsed
		}
	}

	return Config{
		KafkaBrokers:            brokers,
		KafkaTopic:              getEnv("KAFKA_TOPIC", "jobs"),
		KafkaDLQTopic:           getEnv("KAFKA_DLQ_TOPIC", "jobs_dlq"),
		KafkaConsumerGroupSeq:   getEnv("KAFKA_CONSUMER_GROUP_SEQUENTIAL", "poc-sequential"),
		KafkaConsumerGroupBatch: getEnv("KAFKA_CONSUMER_GROUP_BATCH", "poc-batch"),
		KafkaBatchSize:          batchSize,
		KafkaBatchTimeout:       batchTimeout,
		AppPort:                 getEnv("APP_PORT", "8090"),
	}
}
