package messaging

import "encoding/json"

const (
	TaskExchange         = "task.events"
	UserExchange         = "user.events"
	CaptchaExchange      = "captcha.events"
	CreditResultExchange = "credit.results"
)

type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}
