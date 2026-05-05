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

	// NewTimer + Stop evita o vazamento de goroutines que time.After causa em loops.
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg, ok := <-msgChan:
		if !ok {
			return nil
		}
		batch = append(batch, msg)
	case <-timer.C:
		return batch
	case <-ctx.Done():
		return batch
	}

	for len(batch) < maxSize {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				return batch
			}
			batch = append(batch, msg)
		default:
			return batch
		}
	}

	return batch
}
