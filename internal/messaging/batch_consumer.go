package messaging

import (
	"context"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill/message"
	"poc-watermill/internal/batch"
	"poc-watermill/internal/config"
	"poc-watermill/internal/handler"
)

// BatchConsumer consome mensagens do Kafka em lotes usando a estratégia drain-what's-available,
// com retry manual e Dead Letter Queue (DLQ) para mensagens que esgotam as tentativas.
type BatchConsumer struct {
	subscriber message.Subscriber
	publisher  message.Publisher
	cfg        config.Config
}

// NewBatchConsumer cria um novo BatchConsumer com o subscriber, publisher e configuração fornecidos.
func NewBatchConsumer(sub message.Subscriber, pub message.Publisher, cfg config.Config) *BatchConsumer {
	return &BatchConsumer{
		subscriber: sub,
		publisher:  pub,
		cfg:        cfg,
	}
}

// Run inicia o loop de consumo em batch. Encerra quando o contexto for cancelado.
func (bc *BatchConsumer) Run(ctx context.Context) error {
	msgChan, err := bc.subscriber.Subscribe(ctx, bc.cfg.KafkaTopic)
	if err != nil {
		return err
	}

	slog.Info("batch consumer iniciado", "topic", bc.cfg.KafkaTopic, "batchSize", bc.cfg.KafkaBatchSize)

	for {
		msgs := batch.NextBatch(ctx, msgChan, bc.cfg.KafkaBatchSize, bc.cfg.KafkaBatchTimeout)

		if len(msgs) == 0 {
			// Contexto cancelado: encerra limpo
			if ctx.Err() != nil {
				slog.Info("batch consumer encerrado por cancelamento de contexto")
				return nil
			}
			// Timeout sem mensagens: continua aguardando
			continue
		}

		if err := handler.ProcessBatch(msgs); err != nil {
			// Lote falhou: processa cada mensagem individualmente com retry e DLQ
			slog.Warn("lote falhou, processando individualmente", "size", len(msgs), "erro", err)

			for _, msg := range msgs {
				// ProcessIndividual retorna nil em todos os caminhos (sucesso ou DLQ)
				_ = handler.ProcessIndividual(ctx, msg, bc.publisher, bc.cfg.KafkaDLQTopic)
				// ACK obrigatório após processamento individual (sucesso ou DLQ)
				msg.Ack()
			}
		} else {
			// Lote processado com sucesso: ACK em todas as mensagens
			for _, msg := range msgs {
				msg.Ack()
			}
		}
	}
}
