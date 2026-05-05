package handler

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

func SequentialHandler(msg *message.Message) ([]*message.Message, error) {
	id := msg.Metadata.Get("id")
	if id == "" {
		id = msg.UUID
	}

	if strings.Contains(string(msg.Payload), "fail") {
		slog.Warn("handler falhou, será reprocessado", "id", id)
		return nil, fmt.Errorf("falha simulada para mensagem id=%s", id)
	}

	slog.Info("mensagem processada", "id", id)
	return nil, nil
}
