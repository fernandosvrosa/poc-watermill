package handler

import (
	"fmt"
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
)

const publisherKey = "publisher"

func PublisherMiddleware(pub message.Publisher) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set(publisherKey, pub)
			return next(c)
		}
	}
}

func getPublisher(c echo.Context) (message.Publisher, error) {
	pub, ok := c.Get(publisherKey).(message.Publisher)
	if !ok || pub == nil {
		return nil, fmt.Errorf("publisher não disponível no contexto")
	}
	return pub, nil
}

type publishSingleRequest struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

func PublishSingle(c echo.Context) error {
	var req publishSingleRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "body inválido"})
	}
	if req.ID == "" || req.Payload == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "campos 'id' e 'payload' são obrigatórios"})
	}

	pub, err := getPublisher(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte(req.Payload))
	msg.Metadata.Set("id", req.ID)

	if err := pub.Publish("jobs", msg); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"ok": true, "id": req.ID})
}

type publishBatchItem struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
}

type publishBatchResult struct {
	ID    string `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func PublishBatch(c echo.Context) error {
	var items []publishBatchItem
	if err := c.Bind(&items); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "body inválido"})
	}

	pub, err := getPublisher(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	results := make([]publishBatchResult, 0, len(items))
	for _, item := range items {
		result := publishBatchResult{ID: item.ID}
		if item.ID == "" || item.Payload == "" {
			result.Error = "campos 'id' e 'payload' são obrigatórios"
			results = append(results, result)
			continue
		}

		msg := message.NewMessage(watermill.NewUUID(), []byte(item.Payload))
		msg.Metadata.Set("id", item.ID)

		if err := pub.Publish("jobs", msg); err != nil {
			result.Error = err.Error()
		} else {
			result.OK = true
		}
		results = append(results, result)
	}

	return c.JSON(http.StatusMultiStatus, results)
}

func HealthCheck(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}
