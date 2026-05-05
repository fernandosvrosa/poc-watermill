package handler

import (
	"fmt"
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
)

// publisherKey é a chave usada para armazenar o publisher no contexto Echo
const publisherKey = "publisher"

// PublisherMiddleware injeta o publisher Watermill no contexto Echo
func PublisherMiddleware(pub message.Publisher) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(publisherKey, pub)
			return next(c)
		}
	}
}

// publishSingleRequest representa o corpo da requisição para publicação individual
type publishSingleRequest struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

// publishSingleResponse representa a resposta de publicação individual
type publishSingleResponse struct {
	OK bool   `json:"ok"`
	ID string `json:"id"`
}

// PublishSingle publica uma mensagem individual no Kafka
// POST /publish/single
// Body: {"id":"abc","payload":"..."}
// Response 200: {"ok":true,"id":"abc"}
// Response 400: body inválido ou campos faltando
func PublishSingle(c echo.Context) error {
	var req publishSingleRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "body inválido"})
	}

	if req.ID == "" || req.Payload == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "campos 'id' e 'payload' são obrigatórios"})
	}

	pub := c.Get(publisherKey).(message.Publisher)

	msg := message.NewMessage(watermill.NewUUID(), []byte(req.Payload))
	msg.Metadata.Set("id", req.ID)

	if err := pub.Publish("jobs", msg); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("falha ao publicar mensagem: %s", err.Error())})
	}

	return c.JSON(http.StatusOK, publishSingleResponse{
		OK: true,
		ID: req.ID,
	})
}

// publishBatchItem representa um item no corpo da requisição de publicação em lote
type publishBatchItem struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

// publishBatchResult representa o resultado de publicação de um item do lote
type publishBatchResult struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// PublishBatch publica múltiplas mensagens no Kafka e retorna resultado por item
// POST /publish/batch
// Body: [{"id":"1","payload":"..."},{"id":"2","payload":"..."}]
// Response 207: [{"id":"1","ok":true},{"id":"2","ok":false,"error":"..."}]
func PublishBatch(c echo.Context) error {
	var items []publishBatchItem
	if err := c.Bind(&items); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "body inválido"})
	}

	pub := c.Get(publisherKey).(message.Publisher)

	results := make([]publishBatchResult, 0, len(items))

	for _, item := range items {
		result := publishBatchResult{ID: item.ID}

		if item.ID == "" || item.Payload == "" {
			result.OK = false
			result.Error = "campos 'id' e 'payload' são obrigatórios"
			results = append(results, result)
			continue
		}

		msg := message.NewMessage(watermill.NewUUID(), []byte(item.Payload))
		msg.Metadata.Set("id", item.ID)

		if err := pub.Publish("jobs", msg); err != nil {
			result.OK = false
			result.Error = fmt.Sprintf("falha ao publicar mensagem: %s", err.Error())
		} else {
			result.OK = true
		}

		results = append(results, result)
	}

	return c.JSON(http.StatusMultiStatus, results)
}

// HealthCheck verifica a saúde da aplicação
// GET /health
// Response 200: {"status":"ok"}
func HealthCheck(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}
