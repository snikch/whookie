package whookie

import "encoding/json"

type Event struct {
	Id        string          `json:"id"`
	Message   string          `json:"message"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}
