package batch

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// NextBatch coleta mensagens do canal usando a estratégia "drain what's available":
// aguarda pelo menos 1 mensagem (ou timeout), e drena o restante sem bloquear até maxSize.
func NextBatch(ctx context.Context, msgChan <-chan *message.Message, maxSize int, timeout time.Duration) []*message.Message {
	var batch []*message.Message

	// Aguarda primeira mensagem, timeout ou cancelamento de contexto
	select {
	case msg, ok := <-msgChan:
		if !ok {
			return nil
		}
		batch = append(batch, msg)
	case <-time.After(timeout):
		return batch // retorna vazio após timeout
	case <-ctx.Done():
		return batch
	}

	// Drena o restante do canal sem bloquear até atingir maxSize
	for len(batch) < maxSize {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				return batch
			}
			batch = append(batch, msg)
		default:
			return batch // canal vazio agora, retorna o que foi coletado
		}
	}

	return batch
}
