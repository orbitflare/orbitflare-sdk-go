package orbitflare

import "github.com/orbitflare/orbitflare-sdk-go/internal/events"

type (
	ConnectionEvent   = events.Event
	ConnectionEvents  = events.Events
	Connecting        = events.Connecting
	Connected         = events.Connected
	Disconnected      = events.Disconnected
	ReplayClamped     = events.ReplayClamped
	ReplayUnavailable = events.ReplayUnavailable
	Lagged            = events.Lagged
)
