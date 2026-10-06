// Package tinkoff — клиент T-Invest API: подключение к стриму рыночных
// данных, подписка/отписка на свечи, переподключение с backoff+jitter
// (2.5) и публикация полученных свечей в Kafka (2.6).
package tinkoff

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/tinkoff/invest-api-go-sdk/investgo"
	pb "github.com/tinkoff/invest-api-go-sdk/proto"

	"charthawk/collector/internal/config"
	"charthawk/collector/internal/kafka"
	"charthawk/collector/internal/metrics"
	"charthawk/collector/internal/subscription"
)

// streamHandle — состояние одного логического stream-соединения
// (соответствует stream_id в REST API подписок, 2.7).
type streamHandle struct {
	id int

	mu        sync.Mutex
	mds       *investgo.MarketDataStream
	candleCh  <-chan *pb.Candle
	listening bool
	// subs — активные подписки этого стрима: таймфрейм -> тикер -> инструмент.
	// Используется для ресабскрайба после реконнекта.
	subs map[string]map[string]subscription.Instrument
}

// Client — обёртка над investgo.Client с управлением множеством
// stream-соединений рыночных данных.
type Client struct {
	logger       *slog.Logger
	cfg          config.TinkoffConfig
	sdk          *investgo.Client
	mdStreamCli  *investgo.MarketDataStreamClient
	resolver     *Resolver
	producer     *kafka.Producer
	candlesTopic string

	// baseCtx — контекст времени жизни сервиса, используется для фоновых
	// stream-горутин. Он не совпадает с ctx, приходящим в Subscribe/
	// Unsubscribe из HTTP-запроса: тот живёт только на время запроса и не
	// годится для долгоживущего чтения стрима.
	baseCtx context.Context

	mu      sync.Mutex
	streams map[int]*streamHandle
}

// Connect устанавливает соединение с T-Invest API по адресу, заданному в
// общих настройках сервиса (cfg.EndPoint), загружает справочник
// инструментов и возвращает готовый к работе клиент.
func Connect(ctx context.Context, cfg config.TinkoffConfig, producer *kafka.Producer, candlesTopic string, logger *slog.Logger) (*Client, error) {
	sdkCfg := investgo.Config{
		EndPoint:   cfg.EndPoint,
		Token:      cfg.Token,
		AppName:    cfg.AppName,
		AccountId:  cfg.AccountID,
		MaxRetries: cfg.MaxRetries,
	}

	sdkClient, err := investgo.NewClient(ctx, sdkCfg, newSDKLogger(logger))
	if err != nil {
		return nil, fmt.Errorf("connect to T-Invest API at %s: %w", cfg.EndPoint, err)
	}

	resolver := NewResolver(sdkClient.NewInstrumentsServiceClient(), logger)
	if err := resolver.Refresh(); err != nil {
		return nil, err
	}
	resolver.StartAutoRefresh(ctx, time.Hour)

	c := &Client{
		logger:       logger,
		cfg:          cfg,
		sdk:          sdkClient,
		mdStreamCli:  sdkClient.NewMarketDataStreamClient(),
		resolver:     resolver,
		producer:     producer,
		candlesTopic: candlesTopic,
		baseCtx:      ctx,
		streams:      make(map[int]*streamHandle),
	}
	return c, nil
}

// Resolver возвращает резолвер тикеров (реализует subscription.TickerResolver).
func (c *Client) Resolver() *Resolver { return c.resolver }

// Subscribe реализует subscription.StreamSubscriber: подписывает набор
// инструментов на свечи заданного таймфрейма в рамках stream-соединения
// streamID, создавая соединение при первом обращении.
func (c *Client) Subscribe(_ context.Context, streamID int, timeframe string, instruments []subscription.Instrument) error {
	if len(instruments) == 0 {
		return nil
	}
	interval, err := timeframeToInterval(timeframe)
	if err != nil {
		return err
	}

	h, err := c.getOrCreateHandle(streamID)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	candleCh, err := h.mds.SubscribeCandle(figisOf(instruments), interval, false)
	if err != nil {
		return fmt.Errorf("subscribe candle: %w", err)
	}
	h.candleCh = candleCh

	if h.subs == nil {
		h.subs = make(map[string]map[string]subscription.Instrument)
	}
	tfSubs, ok := h.subs[timeframe]
	if !ok {
		tfSubs = make(map[string]subscription.Instrument)
		h.subs[timeframe] = tfSubs
	}
	for _, ins := range instruments {
		tfSubs[ins.Ticker] = ins
	}

	if !h.listening {
		h.listening = true
		go c.runStream(c.baseCtx, h)
	}

	return nil
}

// Unsubscribe реализует subscription.StreamSubscriber.
func (c *Client) Unsubscribe(_ context.Context, streamID int, timeframe string, instruments []subscription.Instrument) error {
	if len(instruments) == 0 {
		return nil
	}
	interval, err := timeframeToInterval(timeframe)
	if err != nil {
		return err
	}

	c.mu.Lock()
	h, ok := c.streams[streamID]
	c.mu.Unlock()
	if !ok {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.mds.UnSubscribeCandle(figisOf(instruments), interval, false); err != nil {
		return fmt.Errorf("unsubscribe candle: %w", err)
	}

	if tfSubs, ok := h.subs[timeframe]; ok {
		for _, ins := range instruments {
			delete(tfSubs, ins.Ticker)
		}
		if len(tfSubs) == 0 {
			delete(h.subs, timeframe)
		}
	}

	return nil
}

func (c *Client) getOrCreateHandle(streamID int) (*streamHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if h, ok := c.streams[streamID]; ok {
		return h, nil
	}

	mds, err := c.mdStreamCli.MarketDataStream()
	if err != nil {
		return nil, fmt.Errorf("open market data stream %d: %w", streamID, err)
	}

	h := &streamHandle{
		id:   streamID,
		mds:  mds,
		subs: make(map[string]map[string]subscription.Instrument),
	}
	c.streams[streamID] = h
	metrics.WSConnectionsActive.Inc()
	return h, nil
}

// runStream слушает стрим свечей и публикует их в Kafka. При обрыве
// соединения выполняет переподключение с экспоненциальным backoff и
// jitter (2.5) и восстанавливает все активные подписки стрима.
func (c *Client) runStream(ctx context.Context, h *streamHandle) {
	for {
		h.mu.Lock()
		mds := h.mds
		candleCh := h.candleCh
		h.mu.Unlock()

		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			for candle := range candleCh {
				c.publishCandle(ctx, candle)
			}
		}()

		err := mds.Listen()
		<-consumerDone

		select {
		case <-ctx.Done():
			return
		default:
		}

		if err == nil {
			// Стрим остановлен штатно (Stop()/отмена контекста SDK).
			return
		}

		h.mu.Lock()
		hasSubs := len(h.subs) > 0
		h.mu.Unlock()
		if !hasSubs {
			c.logger.Info("market data stream has no active subscriptions, stopping", "stream_id", h.id)
			return
		}

		metrics.WSReconnectsTotal.Inc()
		c.logger.Warn("market data stream disconnected, reconnecting", "stream_id", h.id, "error", err)

		newMds, newCh, ok := c.reconnect(ctx, h)
		if !ok {
			return
		}

		h.mu.Lock()
		h.mds = newMds
		h.candleCh = newCh
		h.mu.Unlock()

		c.logger.Info("market data stream reconnected", "stream_id", h.id)
	}
}

// reconnect переустанавливает stream-соединение и восстанавливает все
// подписки, использующие экспоненциальный backoff с jitter, пока не
// получится либо не отменится контекст.
func (c *Client) reconnect(ctx context.Context, h *streamHandle) (*investgo.MarketDataStream, <-chan *pb.Candle, bool) {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = c.cfg.Reconnect.InitialInterval
	b.MaxInterval = c.cfg.Reconnect.MaxInterval
	b.Multiplier = c.cfg.Reconnect.Multiplier
	b.RandomizationFactor = c.cfg.Reconnect.RandomizationFactor
	b.MaxElapsedTime = 0 // ретраить бесконечно, пока не отменится ctx

	for {
		wait := b.NextBackOff()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, false
		case <-timer.C:
		}

		newMds, err := c.mdStreamCli.MarketDataStream()
		if err != nil {
			c.logger.Warn("reconnect: failed to open stream", "stream_id", h.id, "error", err)
			continue
		}

		h.mu.Lock()
		var newCh <-chan *pb.Candle
		var subErr error
		for timeframe, tickers := range h.subs {
			interval, _ := timeframeToInterval(timeframe)
			figis := make([]string, 0, len(tickers))
			for _, ins := range tickers {
				figis = append(figis, ins.Figi)
			}
			ch, err := newMds.SubscribeCandle(figis, interval, false)
			if err != nil {
				subErr = fmt.Errorf("resubscribe %s: %w", timeframe, err)
				break
			}
			newCh = ch
		}
		h.mu.Unlock()

		if subErr != nil {
			c.logger.Warn("reconnect: failed to resubscribe", "stream_id", h.id, "error", subErr)
			newMds.Stop()
			continue
		}

		return newMds, newCh, true
	}
}

func (c *Client) publishCandle(ctx context.Context, candle *pb.Candle) {
	ticker := c.resolver.TickerByFigi(candle.GetFigi())
	payload := newCandlePayload(candle, ticker)

	value, err := json.Marshal(payload)
	if err != nil {
		c.logger.Error("failed to marshal candle payload", "figi", candle.GetFigi(), "error", err)
		return
	}

	if err := c.producer.Produce(ctx, []byte(candle.GetFigi()), value); err != nil {
		c.logger.Error("failed to publish candle to kafka", "figi", candle.GetFigi(), "error", err)
	}
}

// Close останавливает все стримы и закрывает соединение с T-Invest API.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, h := range c.streams {
		h.mu.Lock()
		h.mds.Stop()
		h.mu.Unlock()
	}
}
