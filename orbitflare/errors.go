package orbitflare

import (
	"errors"
	"fmt"
	"time"
)

var ErrTimeout = errors.New("request timed out")

type TransportError struct {
	Err      error
	Endpoint string
}

func (e *TransportError) Error() string {

	msg := SanitizeURL(e.Err.Error())
	if e.Endpoint != "" {
		return fmt.Sprintf("[%s] %s", e.Endpoint, msg)
	}
	return msg
}

func (e *TransportError) Unwrap() error { return e.Err }

func NewTransportError(err error) *TransportError { return &TransportError{Err: err} }

func TransportErrorf(format string, args ...any) *TransportError {
	return &TransportError{Err: fmt.Errorf(format, args...)}
}

type RPCError struct {
	Code     int64
	Message  string
	Endpoint string
}

func (e *RPCError) Error() string {
	if e.Endpoint != "" {
		return fmt.Sprintf("rpc error %d: [%s] %s", e.Code, e.Endpoint, e.Message)
	}
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

type GRPCError struct {
	Code     int32
	Message  string
	Endpoint string
}

func (e *GRPCError) Error() string {
	if e.Endpoint != "" {
		return fmt.Sprintf("grpc error (%d): [%s] %s", e.Code, e.Endpoint, e.Message)
	}
	return fmt.Sprintf("grpc error (%d): %s", e.Code, e.Message)
}

type RateLimitedError struct {
	RetryAfter *time.Duration
}

func (e *RateLimitedError) Error() string {
	if e.RetryAfter != nil {
		return fmt.Sprintf("rate limited (retry after %ds)", int64(e.RetryAfter.Seconds()))
	}
	return "rate limited"
}

type AuthError struct{ Message string }

func (e *AuthError) Error() string { return "auth: " + e.Message }

type StreamError struct{ Message string }

func (e *StreamError) Error() string { return "stream closed: " + e.Message }

type ConfigError struct{ Message string }

func (e *ConfigError) Error() string { return e.Message }

type SerializationError struct{ Message string }

func (e *SerializationError) Error() string { return e.Message }

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTimeout) {
		return true
	}
	var te *TransportError
	if errors.As(err, &te) {
		return true
	}
	var rl *RateLimitedError
	if errors.As(err, &rl) {
		return true
	}
	var se *StreamError
	if errors.As(err, &se) {
		return true
	}
	var re *RPCError
	if errors.As(err, &re) {
		switch re.Code {
		case -32005, -32007, -32014, -32015, -32016:
			return true
		default:
			return false
		}
	}
	return false
}

func RetryAfter(err error) *time.Duration {
	var rl *RateLimitedError
	if errors.As(err, &rl) {
		return rl.RetryAfter
	}
	return nil
}

func WithEndpoint(err error, endpoint string) error {
	ep := SanitizeURL(endpoint)
	switch e := err.(type) {
	case *TransportError:
		return &TransportError{Err: e.Err, Endpoint: ep}
	case *RPCError:
		return &RPCError{Code: e.Code, Message: e.Message, Endpoint: ep}
	case *GRPCError:
		return &GRPCError{Code: e.Code, Message: e.Message, Endpoint: ep}
	default:
		return err
	}
}
