package handler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Critério compartilhado entre ProcessBatch e ProcessIndividual.
func processMessage(msg *message.Message) error {
	if strings.Contains(string(msg.Payload), "fail-batch") {
		return fmt.Errorf("falha simulada para mensagem id=%s", msg.UUID)
	}
	return nil
}

func ProcessBatch(msgs []*message.Message) error {
	for _, msg := range msgs {
		if err := processMessage(msg); err != nil {
			return err
		}
	}
	slog.Info("lote processado", "size", len(msgs))
	return nil
}

// ProcessIndividual retorna nil em todos os caminhos: sucesso, ou falha enviada ao DLQ.
func ProcessIndividual(ctx context.Context, msg *message.Message, publisher message.Publisher, dlqTopic string) error {
	id := msg.Metadata.Get("id")
	if id == "" {
		id = msg.UUID
	}

	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := processMessage(msg); err == nil {
			slog.Info("mensagem processada individualmente", "id", id, "tentativa", attempt)
			return nil
		}

		slog.Warn("tentativa falhou", "id", id, "tentativa", attempt)
	}

	if err := publisher.Publish(dlqTopic, msg); err != nil {
		slog.Error("falha ao publicar no DLQ", "id", id, "erro", err)
		return err
	}

	slog.Warn("mensagem enviada para DLQ", "id", id)
	return nil
}
