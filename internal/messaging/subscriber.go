package messaging

import (
	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"poc-watermill/internal/config"
)

// OffsetNewest evita reprocessar mensagens históricas ao reiniciar em ambiente de dev/POC.
func newSaramaConfig() *sarama.Config {
	cfg := kafka.DefaultSaramaSubscriberConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	return cfg
}

func newSubscriber(cfg config.Config, group string) (message.Subscriber, error) {
	logger := watermill.NewStdLogger(false, false)
	subscriberCfg := kafka.SubscriberConfig{
		Brokers:               cfg.KafkaBrokers,
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         group,
		OverwriteSaramaConfig: newSaramaConfig(),
	}
	return kafka.NewSubscriber(subscriberCfg, logger)
}

func NewSubscriberSequential(cfg config.Config) (message.Subscriber, error) {
	return newSubscriber(cfg, cfg.KafkaConsumerGroupSeq)
}

func NewSubscriberBatch(cfg config.Config) (message.Subscriber, error) {
	return newSubscriber(cfg, cfg.KafkaConsumerGroupBatch)
}
