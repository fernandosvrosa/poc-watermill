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
	// Carrega configurações a partir de variáveis de ambiente
	cfg := config.Load()

	// Inicializa publisher Kafka
	publisher, err := messaging.NewPublisher(cfg)
	if err != nil {
		log.Fatalf("erro ao criar publisher: %v", err)
	}

	// Inicializa subscriber sequencial
	subscriberSeq, err := messaging.NewSubscriberSequential(cfg)
	if err != nil {
		log.Fatalf("erro ao criar subscriber sequencial: %v", err)
	}

	// Inicializa subscriber para processamento em batch
	subscriberBatch, err := messaging.NewSubscriberBatch(cfg)
	if err != nil {
		log.Fatalf("erro ao criar subscriber batch: %v", err)
	}

	// Cria o router Watermill com handler sequencial e DLQ
	router, err := messaging.NewRouter(publisher, subscriberSeq, cfg.KafkaTopic, cfg.KafkaDLQTopic, handler.SequentialHandler)
	if err != nil {
		log.Fatalf("erro ao criar router: %v", err)
	}

	// Cria o batch consumer
	batchConsumer := messaging.NewBatchConsumer(subscriberBatch, publisher, cfg)

	// Configura o servidor Echo com middleware de publisher
	e := echo.New()
	e.HideBanner = true
	e.Use(handler.PublisherMiddleware(publisher))

	// Registra as rotas HTTP
	e.POST("/publish/single", handler.PublishSingle)
	e.POST("/publish/batch", handler.PublishBatch)
	e.GET("/health", handler.HealthCheck)

	// Captura sinais de encerramento (SIGINT / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	// Goroutine do router sequencial Watermill
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := router.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("router encerrado: %v", err)
		}
	}()

	// Goroutine do batch consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := batchConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("batch consumer encerrado: %v", err)
		}
	}()

	// Goroutine do servidor HTTP Echo
	go func() {
		if err := e.Start(":" + cfg.AppPort); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("echo encerrado: %v", err)
		}
	}()

	slog.Info("servidor iniciado", "port", cfg.AppPort, "topic", cfg.KafkaTopic)

	// Aguarda sinal de encerramento
	<-ctx.Done()
	slog.Info("sinal recebido, iniciando shutdown...")

	// 1. Para o Echo graciosamente
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Printf("erro no shutdown do echo: %v", err)
	}

	// 2. Aguarda goroutines do router e batch consumer encerrarem
	wg.Wait()

	// 3. Fecha conexões Kafka
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
