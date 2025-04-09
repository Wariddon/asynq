package payload

type LineNotificationPayload struct {
	CorrelationID string `json:"correlation_id"`
	Content       string `json:"content"`
	Priority      string    `json:"priority"`
	RetryLimit    int    `json:"retry_limit"`
}
