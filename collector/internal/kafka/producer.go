// Package kafka содержит продюсер Kafka для топика market.candles (2.6).
package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"charthawk/collector/internal/metrics"
)

// Producer публикует нормализованные свечи в Kafka.
type Producer struct {
	writer *kafkago.Writer
	topic  string
}

// NewProducer создаёт продюсер с партиционированием по FIGI (см. 2.6).
func NewProducer(brokers []string, topic string, batchSize int, batchTimeout time.Duration) *Producer {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{}, // партиционирование по ключу (FIGI)
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Compression:  kafkago.Snappy,
		RequiredAcks: kafkago.RequireOne,
	}
	return &Producer{writer: writer, topic: topic}
}

// Produce публикует одно сообщение с ключом key (FIGI) и телом value (JSON).
func (p *Producer) Produce(ctx context.Context, key, value []byte) error {
	err := p.writer.WriteMessages(ctx, kafkago.Message{Key: key, Value: value})
	if err != nil {
		metrics.KafkaProduceErrorsTotal.WithLabelValues(p.topic).Inc()
		return err
	}
	metrics.KafkaMessagesProducedTotal.WithLabelValues(p.topic).Inc()
	return nil
}

// Close закрывает продюсер, дожидаясь отправки буферизованных сообщений.
func (p *Producer) Close() error {
	return p.writer.Close()
}
