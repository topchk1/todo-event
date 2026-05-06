package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"todoe/internal/messaging"
)

type lokiPush struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

func pushToLoki(lokiURL, eventType string, payload json.RawMessage) error {
	line, _ := json.Marshal(map[string]any{
		"event_type": eventType,
		"payload":    payload,
	})
	body := lokiPush{Streams: []lokiStream{{
		Stream: map[string]string{"service": "audit", "event_type": eventType},
		Values: [][2]string{{fmt.Sprintf("%d", time.Now().UnixNano()), string(line)}},
	}}}
	data, _ := json.Marshal(body)
	resp, err := http.Post(lokiURL+"/loki/api/v1/push", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("loki push: status %d", resp.StatusCode)
	}
	return nil
}

func main() {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}
	lokiURL := os.Getenv("LOKI_URL")
	if lokiURL == "" {
		lokiURL = "http://localhost:3100"
	}

	conn, ch, err := messaging.Connect(amqpURL)
	if err != nil {
		log.Fatal("rabbit:", err)
	}
	defer conn.Close()

	if err := messaging.Subscribe(ch, messaging.TaskExchange, messaging.QueueAuditTaskEvents, func(msg messaging.Message) {
		slog.Info("audit: received event", "type", msg.Type)
		if err := pushToLoki(lokiURL, msg.Type, msg.Payload); err != nil {
			slog.Error("audit: loki push", "err", err)
		}
	}); err != nil {
		log.Fatal("rabbit subscribe task.events:", err)
	}

	if err := messaging.Subscribe(ch, messaging.CaptchaExchange, messaging.QueueAuditCaptchaEvents, func(msg messaging.Message) {
		slog.Info("audit: received event", "type", msg.Type)
		if err := pushToLoki(lokiURL, msg.Type, msg.Payload); err != nil {
			slog.Error("audit: loki push", "err", err)
		}
	}); err != nil {
		log.Fatal("rabbit subscribe captcha.events:", err)
	}

	slog.Info("audit service listening", "exchanges", []string{messaging.TaskExchange, messaging.CaptchaExchange})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("audit service stopping")
}
