package messaging

import (
	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"poc-watermill/internal/config"
)

// newSaramaConfig cria um sarama.Config com offset inicial configurado para OffsetNewest.
func newSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	return cfg
}

// NewSubscriberSequential cria e retorna um subscriber Kafka para processamento sequencial.
// Utiliza o consumer group configurado em KafkaConsumerGroupSeq.
func NewSubscriberSequential(cfg config.Config) (message.Subscriber, error) {
	logger := watermill.NewStdLogger(false, false)

	subscriberCfg := kafka.SubscriberConfig{
		Brokers:               cfg.KafkaBrokers,
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         cfg.KafkaConsumerGroupSeq,
		OverwriteSaramaConfig: newSaramaConfig(),
	}

	return kafka.NewSubscriber(subscriberCfg, logger)
}

// NewSubscriberBatch cria e retorna um subscriber Kafka para processamento em batch.
// Utiliza o consumer group configurado em KafkaConsumerGroupBatch.
func NewSubscriberBatch(cfg config.Config) (message.Subscriber, error) {
	logger := watermill.NewStdLogger(false, false)

	subscriberCfg := kafka.SubscriberConfig{
		Brokers:               cfg.KafkaBrokers,
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         cfg.KafkaConsumerGroupBatch,
		OverwriteSaramaConfig: newSaramaConfig(),
	}

	return kafka.NewSubscriber(subscriberCfg, logger)
}
