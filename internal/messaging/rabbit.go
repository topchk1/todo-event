package messaging

import (
	"context"
	"encoding/json"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"todoe/internal/event"
)

func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

func DeclareExchange(ch *amqp.Channel, name string) error {
	return ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil)
}

const (
	QueueAuditTaskEvents         = "audit.task.events"
	QueueAuditCaptchaEvents      = "audit.captcha.events"
	QueueWelcomeUserEvents       = "welcome.user.events"
	QueueCreditUserEvents        = "credit.user.events"
	QueueAuthenUserEvents        = "authen.user.events"
	QueueOnboardingCreditResults = "onboarding.credit.results"
)

type Binding struct {
	Exchange string
	Queue    string
}

func DeclareTopology(ch *amqp.Channel, bindings []Binding) error {
	for _, b := range bindings {
		if err := DeclareExchange(ch, b.Exchange); err != nil {
			return err
		}
		if _, err := ch.QueueDeclare(b.Queue, true, false, false, false, nil); err != nil {
			return err
		}
		if err := ch.QueueBind(b.Queue, "", b.Exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}

type Publisher struct {
	ch       *amqp.Channel
	exchange string
}

func NewPublisher(ch *amqp.Channel, exchange string) *Publisher {
	return &Publisher{ch: ch, exchange: exchange}
}

func (p *Publisher) Publish(ctx context.Context, e event.Event) {
	payload, _ := json.Marshal(e.Payload)
	data, _ := json.Marshal(Message{Type: e.Type, Payload: payload})
	err := p.ch.PublishWithContext(ctx, p.exchange, "", false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         data,
	})
	if err != nil {
		slog.Error("rabbit: publish error", "exchange", p.exchange, "err", err)
	}
}

var _ event.Publisher = (*Publisher)(nil)

func Subscribe(ch *amqp.Channel, exchange, queue string, handler func(Message)) error {
	if err := DeclareExchange(ch, exchange); err != nil {
		return err
	}
	q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return err
	}
	if err := ch.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		return err
	}
	deliveries, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for d := range deliveries {
			var msg Message
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				slog.Error("rabbit: unmarshal", "queue", queue, "err", err)
				d.Nack(false, false)
				continue
			}
			handler(msg)
			d.Ack(false)
		}
	}()
	return nil
}
