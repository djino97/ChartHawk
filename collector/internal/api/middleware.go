package api

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"charthawk/collector/internal/metrics"
)

// Auth требует заголовок Authorization: Bearer <ADMIN_API_KEY> на всех
// эндпоинтах подписок (2.7, "Аутентификация и защита").
func Auth(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "

			header := r.Header.Get("Authorization")
			if !strings.HasPrefix(header, prefix) {
				writeError(w, http.StatusUnauthorized, "unauthorized", "missing or malformed Authorization header")
				return
			}

			token := strings.TrimPrefix(header, prefix)
			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				writeError(w, http.StatusUnauthorized, "unauthorized", "invalid API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit ограничивает частоту PUT-запросов: не более 10 в минуту
// (2.7, "Rate limit"). При превышении отвечает 429 с Retry-After.
func RateLimit(limiter *rate.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				next.ServeHTTP(w, r)
				return
			}
			if !limiter.Allow() {
				w.Header().Set("Retry-After", "6")
				writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "too many PUT requests, retry later")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// NewPutRateLimiter создаёт лимитер на 10 PUT-запросов в минуту с таким же
// начальным запасом (burst), чтобы не резать первую волну запросов.
func NewPutRateLimiter() *rate.Limiter {
	return rate.NewLimiter(rate.Every(time.Minute/10), 10)
}

// Metrics записывает api_requests_total и api_request_duration_seconds
// (см. таблицу "Дополнительные метрики", 2.7).
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		metrics.APIRequestsTotal.WithLabelValues(r.Method, strconv.Itoa(rec.status)).Inc()
		metrics.APIRequestDuration.WithLabelValues(r.Method).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
