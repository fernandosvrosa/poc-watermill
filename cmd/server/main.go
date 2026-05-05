package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	"poc-watermill/internal/config"
	"poc-watermill/internal/handler"
	"poc-watermill/internal/messaging"
)

func main() {
	cfg := config.Load()

	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		log.Fatalf("erro ao criar publisher: %v", err)
	}

	subscriberSeq, err := messaging.NewSubscriberSequential(cfg)
	if err != nil {
		log.Fatalf("erro ao criar subscriber sequencial: %v", err)
	}

	subscriberBatch, err := messaging.NewSubscriberBatch(cfg)
	if err != nil {
		log.Fatalf("erro ao criar subscriber batch: %v", err)
	}

	router, err := messaging.NewRouter(publisher, subscriberSeq, cfg.KafkaTopic, cfg.KafkaDLQTopic, handler.SequentialHandler)
	if err != nil {
		log.Fatalf("erro ao criar router: %v", err)
	}

	batchConsumer := messaging.NewBatchConsumer(subscriberBatch, publisher, cfg)

	e := echo.New()
	e.HideBanner = true
	e.Use(handler.PublisherMiddleware(publisher))
	e.POST("/publish/single", handler.PublishSingle)
	e.POST("/publish/batch", handler.PublishBatch)
	e.GET("/health", handler.HealthCheck)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := router.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("router encerrado: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := batchConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("batch consumer encerrado: %v", err)
		}
	}()

	go func() {
		if err := e.Start(":" + cfg.AppPort); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("echo encerrado: %v", err)
		}
	}()

	slog.Info("servidor iniciado", "port", cfg.AppPort, "topic", cfg.KafkaTopic)

	<-ctx.Done()
	slog.Info("sinal recebido, iniciando shutdown...")

	// Echo primeiro: para de aceitar novas requisições antes de drenar os consumers.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Printf("erro no shutdown do echo: %v", err)
	}

	wg.Wait()

	if err := publisher.Close(); err != nil {
		log.Printf("erro ao fechar publisher: %v", err)
	}
	if err := subscriberSeq.Close(); err != nil {
		log.Printf("erro ao fechar subscriber sequencial: %v", err)
	}
	if err := subscriberBatch.Close(); err != nil {
		log.Printf("erro ao fechar subscriber batch: %v", err)
	}

	slog.Info("shutdown concluído")
}
