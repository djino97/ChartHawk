package tinkoff

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tinkoff/invest-api-go-sdk/investgo"
	pb "github.com/tinkoff/invest-api-go-sdk/proto"
)

// Resolver кэширует справочник акций T-Invest API (тикер <-> FIGI),
// используемый REST API подписок для резолвинга тикеров (2.7, шаг
// "Резолвинг тикеров").
type Resolver struct {
	instruments *investgo.InstrumentsServiceClient
	logger      *slog.Logger

	mu       sync.RWMutex
	byTicker map[string]string
	byFigi   map[string]string
}

// NewResolver создаёт резолвер поверх InstrumentsServiceClient.
func NewResolver(instruments *investgo.InstrumentsServiceClient, logger *slog.Logger) *Resolver {
	return &Resolver{
		instruments: instruments,
		logger:      logger,
		byTicker:    make(map[string]string),
		byFigi:      make(map[string]string),
	}
}

// Refresh перезагружает справочник акций целиком.
func (r *Resolver) Refresh() error {
	resp, err := r.instruments.Shares(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		return fmt.Errorf("load shares reference: %w", err)
	}

	byTicker := make(map[string]string)
	byFigi := make(map[string]string)
	for _, s := range resp.GetInstruments() {
		byTicker[s.GetTicker()] = s.GetFigi()
		byFigi[s.GetFigi()] = s.GetTicker()
	}

	r.mu.Lock()
	r.byTicker = byTicker
	r.byFigi = byFigi
	r.mu.Unlock()

	r.logger.Info("instrument reference refreshed", "count", len(byTicker))
	return nil
}

// StartAutoRefresh периодически обновляет справочник в фоне.
func (r *Resolver) StartAutoRefresh(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.Refresh(); err != nil {
					r.logger.Error("failed to refresh instrument reference", "error", err)
				}
			}
		}
	}()
}

// Resolve реализует subscription.TickerResolver: тикер -> FIGI.
func (r *Resolver) Resolve(ticker string) (figi string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	figi, ok = r.byTicker[ticker]
	return figi, ok
}

// TickerByFigi возвращает тикер по FIGI (используется при публикации свечей
// в Kafka). Если инструмент неизвестен, возвращает пустую строку.
func (r *Resolver) TickerByFigi(figi string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byFigi[figi]
}
