package messaging

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// NewRouter configura o Watermill Router com Retry + PoisonQueue.
// Retry vem antes do PoisonQueue para que as tentativas ocorram antes do descarte para DLQ.
func NewRouter(publisher message.Publisher, subscriber message.Subscriber, topic, dlqTopic string, handlerFunc message.HandlerFunc) (*message.Router, error) {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	poisonMiddleware, err := middleware.PoisonQueue(publisher, dlqTopic)
	if err != nil {
		return nil, err
	}

	router.AddMiddleware(
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: 100 * time.Millisecond,
		}.Middleware,
		poisonMiddleware,
	)

	// publisher nil: handler não produz mensagens de saída
	router.AddHandler("sequential-handler", topic, subscriber, "", nil, handlerFunc)

	return router, nil
}
