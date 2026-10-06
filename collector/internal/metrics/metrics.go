// Package metrics содержит Prometheus-метрики Collector-а (см. таблицы
// "Дополнительные метрики" и "Мониторинг" в аналитике).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	APIRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "api_requests_total",
		Help: "Количество HTTP-запросов к API подписок по методу и коду ответа.",
	}, []string{"method", "code"})

	APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "api_request_duration_seconds",
		Help:    "Время обработки HTTP-запроса к API подписок.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	SubscriptionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "subscriptions_active",
		Help: "Текущее число подписанных инструментов.",
	})

	SubscriptionsChangesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "subscriptions_changes_total",
		Help: "Количество успешных применений PUT /api/v1/subscriptions.",
	})

	WSConnectionsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ws_connections_active",
		Help: "Активные stream-соединения с T-Invest API.",
	})

	WSReconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ws_reconnects_total",
		Help: "Количество переподключений стрима рыночных данных.",
	})

	KafkaMessagesProducedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_produced_total",
		Help: "Количество сообщений, отправленных в Kafka, по топику.",
	}, []string{"topic"})

	KafkaProduceErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_produce_errors_total",
		Help: "Количество ошибок отправки в Kafka, по топику.",
	}, []string{"topic"})
)
