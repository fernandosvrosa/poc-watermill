package messaging

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"poc-watermill/internal/config"
)

func NewPublisher(cfg config.Config) (message.Publisher, error) {
	logger := watermill.NewStdLogger(false, false)

	publisherCfg := kafka.PublisherConfig{
		Brokers:   cfg.KafkaBrokers,
		Marshaler: kafka.DefaultMarshaler{},
	}

	return kafka.NewPublisher(publisherCfg, logger)
}
