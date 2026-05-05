package handler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// processaMensagem verifica se o payload contém "fail-batch" e retorna erro.
// Critério compartilhado entre ProcessBatch e ProcessIndividual.
func processaMensagem(msg *message.Message) error {
	if strings.Contains(string(msg.Payload), "fail-batch") {
		return fmt.Errorf("falha simulada para mensagem id=%s", msg.UUID)
	}
	return nil
}

// ProcessBatch processa um lote de mensagens.
// Retorna erro se qualquer mensagem do lote contém "fail-batch" no payload.
func ProcessBatch(msgs []*message.Message) error {
	for _, msg := range msgs {
		if err := processaMensagem(msg); err != nil {
			return err
		}
	}
	slog.Info("lote processado", "size", len(msgs))
	return nil
}

// ProcessIndividual processa uma mensagem individualmente com até 3 tentativas.
// Se esgotar os retries, publica no DLQ e retorna nil (mensagem foi para DLQ).
func ProcessIndividual(ctx context.Context, msg *message.Message, publisher message.Publisher, dlqTopic string) error {
	id := msg.Metadata.Get("id")
	if id == "" {
		id = msg.UUID
	}

	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Aplica backoff entre tentativas (exceto na primeira)
		if attempt > 1 {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}

		err := processaMensagem(msg)
		if err == nil {
			slog.Info("mensagem processada individualmente", "id", id, "tentativa", attempt)
			return nil
		}

		slog.Warn("tentativa falhou", "id", id, "tentativa", attempt, "erro", err)
	}

	// Esgotou retries — envia para DLQ
	if err := publisher.Publish(dlqTopic, msg); err != nil {
		slog.Error("falha ao publicar no DLQ", "id", id, "erro", err)
		return err
	}

	slog.Warn("mensagem enviada para DLQ", "id", id)
	return nil // não propaga erro — a mensagem foi para DLQ
}
