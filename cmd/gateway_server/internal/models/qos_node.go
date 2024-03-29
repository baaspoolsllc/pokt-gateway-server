package models

import "time"

type PublicQosNode struct {
	ServiceUrl      string    `json:"service_url"`
	Chain           string    `json:"chain"`
	SessionHeight   uint      `json:"session_height"`
	AppPublicKey    string    `json:"app_public_key"`
	TimeoutUntil    time.Time `json:"timeout_until"`
	TimeoutReason   string    `json:"timeout_reason"`
	LastKnownErr    string    `json:"last_known_err"`
	IsHeathy        bool      `json:"is_heathy"`
	IsSynced        bool      `json:"is_synced"`
	LastKnownHeight uint64    `json:"last_known_height"`
	P90Latency      float64   `json:"p90_latency"`
}
