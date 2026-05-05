package handler

import (
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// SequentialHandler processa mensagens do tópico "jobs" de forma sequencial.
// Retorna erro quando o payload contém "fail", permitindo testar o fluxo DLQ.
func SequentialHandler(msg *message.Message) ([]*message.Message, error) {
	// Lê o id do metadata da mensagem
	id := msg.Metadata.Get("id")
	if id == "" {
		id = msg.UUID
	}

	payload := string(msg.Payload)

	// Simula falha quando o payload contém "fail"
	if strings.Contains(payload, "fail") {
		fmt.Printf(`{"level":"warn","msg":"handler falhou, será reprocessado","id":"%s"}`+"\n", id)
		return nil, fmt.Errorf("falha simulada para mensagem id=%s", id)
	}

	fmt.Printf(`{"level":"info","msg":"mensagem processada","id":"%s"}`+"\n", id)
	return nil, nil
}
