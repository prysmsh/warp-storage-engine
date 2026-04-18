package middleware

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// ANSI color codes
const (
	green   = "\033[97;42m"
	white   = "\033[90;47m"
	yellow  = "\033[90;43m"
	red     = "\033[97;41m"
	blue    = "\033[97;44m"
	magenta = "\033[97;45m"
	cyan    = "\033[97;46m"
	reset   = "\033[0m"
)

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.size += n
	return n, err
}

// Flush implements http.Flusher for streaming responses.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// RequestLoggerConfig configures the request logger.
type RequestLoggerConfig struct {
	// SkipPaths are URL paths to skip logging (e.g. /health, /metrics).
	SkipPaths []string
	// UseColor enables ANSI color output. Auto-detected if unset.
	UseColor bool
}

// DefaultLoggerConfig returns a config that skips health/ready/metrics.
func DefaultLoggerConfig() RequestLoggerConfig {
	return RequestLoggerConfig{
		SkipPaths: []string{"/health", "/ready", "/metrics"},
		UseColor:  isTerminal(),
	}
}

// RequestLogger returns Gin-style HTTP request logging middleware.
func RequestLogger(cfg RequestLoggerConfig) func(http.Handler) http.Handler {
	skipSet := make(map[string]bool, len(cfg.SkipPaths))
	for _, p := range cfg.SkipPaths {
		skipSet[p] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipSet[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(sw, r)

			latency := time.Since(start)
			clientIP := extractClientIP(r)
			method := r.Method
			path := r.URL.Path
			if r.URL.RawQuery != "" {
				path = path + "?" + r.URL.RawQuery
			}
			status := sw.status

			if cfg.UseColor {
				fmt.Fprintf(os.Stderr, "[FSE] %s |%s %3d %s| %13v | %15s |%s %-7s %s %q\n",
					start.Format("2006/01/02 - 15:04:05"),
					statusColor(status), status, reset,
					latency,
					clientIP,
					methodColor(method), method, reset,
					path,
				)
			} else {
				fmt.Fprintf(os.Stderr, "[FSE] %s | %3d | %13v | %15s | %-7s %q\n",
					start.Format("2006/01/02 - 15:04:05"),
					status,
					latency,
					clientIP,
					method,
					path,
				)
			}
		})
	}
}

func statusColor(code int) string {
	switch {
	case code >= 200 && code < 300:
		return green
	case code >= 300 && code < 400:
		return white
	case code >= 400 && code < 500:
		return yellow
	default:
		return red
	}
}

func methodColor(method string) string {
	switch method {
	case http.MethodGet:
		return blue
	case http.MethodPost:
		return cyan
	case http.MethodPut:
		return yellow
	case http.MethodDelete:
		return red
	case http.MethodPatch:
		return green
	case http.MethodHead:
		return magenta
	case http.MethodOptions:
		return white
	default:
		return reset
	}
}

func extractClientIP(r *http.Request) string {
	// Check X-Forwarded-For first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	// Then X-Real-Ip
	if xri := r.Header.Get("X-Real-Ip"); xri != "" {
		return xri
	}
	// Fall back to RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func isTerminal() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
