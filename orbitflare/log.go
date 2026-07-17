package orbitflare

import (
	"log/slog"
	"os"
	"strings"
)

func NoopLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func ResolveLogger(explicit *slog.Logger) *slog.Logger {
	if explicit != nil {
		return explicit
	}
	level, enabled := envLevel()
	if !enabled {
		return NoopLogger()
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func envLevel() (slog.Level, bool) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("ORBITFLARE_LOG"))) {
	case "trace", "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error":
		return slog.LevelError, true
	default:
		return slog.LevelInfo, false
	}
}
