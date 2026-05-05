package messaging

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// NewRouter cria e configura um Router Watermill com middlewares de Retry e PoisonQueue (DLQ).
// Os handlers devem ser registrados externamente via RegisterHandlers.
func NewRouter(publisher message.Publisher, subscriber message.Subscriber, handlerFunc message.HandlerFunc) (*message.Router, error) {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	// PoisonQueue envia para DLQ as mensagens que esgotaram as tentativas de retry
	poisonMiddleware, err := middleware.PoisonQueue(publisher, "jobs_dlq")
	if err != nil {
		return nil, err
	}

	// Retry deve vir antes do PoisonQueue para que as tentativas ocorram antes do descarte
	router.AddMiddleware(
		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: time.Millisecond * 100,
		}.Middleware,
		poisonMiddleware,
	)

	// Registra o handler sequencial no tópico "jobs" sem tópico de saída
	router.AddHandler(
		"sequential-handler",
		"jobs",      // tópico de entrada
		subscriber,
		"",          // sem tópico de saída
		nil,         // sem publisher de saída
		handlerFunc,
	)

	return router, nil
}
