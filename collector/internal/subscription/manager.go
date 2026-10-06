package subscription

import (
	"context"
	"fmt"
	"sort"

	"charthawk/collector/internal/metrics"
)

// TickerResolver резолвит тикер в FIGI через справочник инструментов
// T-Invest API.
type TickerResolver interface {
	Resolve(ticker string) (figi string, ok bool)
}

// StreamSubscriber абстрагирует операции подписки/отписки на конкретном
// stream-соединении T-Invest API, чтобы Manager не зависел от деталей SDK.
type StreamSubscriber interface {
	Subscribe(ctx context.Context, streamID int, timeframe string, instruments []Instrument) error
	Unsubscribe(ctx context.Context, streamID int, timeframe string, instruments []Instrument) error
}

// RequestGroup — одна группа таймфрейм → тикеры из тела PUT-запроса.
type RequestGroup struct {
	Timeframe   string
	Instruments []string
}

// ApplyResult — результат применения PUT-запроса, используется для
// построения HTTP-ответа.
type ApplyResult struct {
	Groups           []StreamGroup
	TotalInstruments int
	ActiveStreams    int
	Added            []string
	Removed          []string
	Unchanged        []string
}

// Manager — единая точка применения изменений подписок: валидация,
// резолвинг тикеров, проверка лимитов, диффинг и атомарное применение.
type Manager struct {
	state        *State
	resolver     TickerResolver
	subscriber   StreamSubscriber
	maxPerStream int
	maxStreams   int
	timeframes   map[string]struct{}
}

// NewManager создаёт менеджер подписок.
func NewManager(state *State, resolver TickerResolver, subscriber StreamSubscriber, maxPerStream, maxStreams int, allowedTimeframes []string) *Manager {
	tf := make(map[string]struct{}, len(allowedTimeframes))
	for _, t := range allowedTimeframes {
		tf[t] = struct{}{}
	}
	return &Manager{
		state:        state,
		resolver:     resolver,
		subscriber:   subscriber,
		maxPerStream: maxPerStream,
		maxStreams:   maxStreams,
		timeframes:   tf,
	}
}

// State возвращает текущее хранилище состояния (для GET-эндпоинта).
func (m *Manager) State() *State { return m.state }

// Apply валидирует, резолвит и атомарно применяет новый список подписок.
// Логика соответствует 2.7 аналитики:
//  1. Валидация запроса.
//  2. Резолвинг тикеров в FIGI.
//  3. Проверка лимитов (N > maxPerStream * maxStreams → ErrKindLimitExceeded).
//  4. Diff со старым состоянием (added/removed/unchanged).
//  5. Применение: сначала новые подписки, потом отписки.
//  6. При ошибке на шаге 5 — rollback к предыдущему состоянию.
func (m *Manager) Apply(ctx context.Context, req []RequestGroup) (*ApplyResult, error) {
	if err := m.validate(req); err != nil {
		return nil, err
	}

	resolved, err := m.resolveAll(req)
	if err != nil {
		return nil, err
	}

	total := 0
	for _, ins := range resolved {
		total += len(ins)
	}
	if limit := m.maxPerStream * m.maxStreams; total > limit {
		return nil, newLimitExceededErr(total, limit)
	}

	newGroups := packIntoStreams(resolved, m.maxPerStream)

	oldTickers := m.state.TickerSet()
	newTickers := make(map[string]Instrument)
	for _, g := range newGroups {
		for _, ins := range g.Instruments {
			newTickers[ins.Ticker] = ins
		}
	}

	added, removed, unchanged := diff(oldTickers, newTickers)

	if err := m.applyToStream(ctx, newGroups, added, removed); err != nil {
		return nil, newApplyErr(err)
	}

	m.state.Replace(newGroups)

	instrCount, streamCount := m.state.Count()
	metrics.SubscriptionsActive.Set(float64(instrCount))
	metrics.SubscriptionsChangesTotal.Inc()
	return &ApplyResult{
		Groups:           newGroups,
		TotalInstruments: instrCount,
		ActiveStreams:    streamCount,
		Added:            added,
		Removed:          removed,
		Unchanged:        unchanged,
	}, nil
}

func (m *Manager) validate(req []RequestGroup) error {
	if len(req) == 0 {
		return newValidationErr("subscriptions list must not be empty")
	}

	seenTicker := make(map[string]struct{})
	for _, g := range req {
		if len(m.timeframes) > 0 {
			if _, ok := m.timeframes[g.Timeframe]; !ok {
				return newValidationErr("unknown timeframe %q", g.Timeframe)
			}
		}
		if len(g.Instruments) == 0 {
			return newValidationErr("timeframe %q has an empty instrument list", g.Timeframe)
		}
		for _, ticker := range g.Instruments {
			key := g.Timeframe + ":" + ticker
			if _, ok := seenTicker[key]; ok {
				return newValidationErr("duplicate ticker %q for timeframe %q", ticker, g.Timeframe)
			}
			seenTicker[key] = struct{}{}
		}
	}
	return nil
}

// resolveAll резолвит все тикеры в FIGI. Возвращает первую же ошибку
// неизвестного тикера — состояние при этом не меняется.
func (m *Manager) resolveAll(req []RequestGroup) (map[string][]Instrument, error) {
	out := make(map[string][]Instrument, len(req))
	for _, g := range req {
		instruments := make([]Instrument, 0, len(g.Instruments))
		for _, ticker := range g.Instruments {
			figi, ok := m.resolver.Resolve(ticker)
			if !ok {
				return nil, newUnknownTickerErr(ticker)
			}
			instruments = append(instruments, Instrument{Ticker: ticker, Figi: figi})
		}
		out[g.Timeframe] = append(out[g.Timeframe], instruments...)
	}
	return out, nil
}

// packIntoStreams раскладывает инструменты по stream-соединениям,
// сохраняя каждую таймфрейм-группу целиком на одном стриме и заполняя
// стримы последовательно до maxPerStream инструментов.
func packIntoStreams(resolved map[string][]Instrument, maxPerStream int) []StreamGroup {
	timeframes := make([]string, 0, len(resolved))
	for tf := range resolved {
		timeframes = append(timeframes, tf)
	}
	sort.Strings(timeframes)

	var groups []StreamGroup
	streamID := 1
	usedInStream := 0

	for _, tf := range timeframes {
		instruments := resolved[tf]
		if usedInStream > 0 && usedInStream+len(instruments) > maxPerStream {
			streamID++
			usedInStream = 0
		}
		groups = append(groups, StreamGroup{StreamID: streamID, Timeframe: tf, Instruments: instruments})
		usedInStream += len(instruments)
	}

	return groups
}

func diff(old, new map[string]Instrument) (added, removed, unchanged []string) {
	for ticker := range new {
		if _, ok := old[ticker]; ok {
			unchanged = append(unchanged, ticker)
		} else {
			added = append(added, ticker)
		}
	}
	for ticker := range old {
		if _, ok := new[ticker]; !ok {
			removed = append(removed, ticker)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(unchanged)
	return added, removed, unchanged
}

// applyToStream подписывает новые инструменты и отписывает удалённые.
// Сначала все Subscribe, затем все Unsubscribe — чтобы не терять данные
// между шагами. При ошибке выполняется best-effort компенсация уже
// применённых операций подписки.
func (m *Manager) applyToStream(ctx context.Context, newGroups []StreamGroup, added, removed []string) error {
	addedSet := make(map[string]struct{}, len(added))
	for _, t := range added {
		addedSet[t] = struct{}{}
	}

	type appliedSub struct {
		streamID    int
		timeframe   string
		instruments []Instrument
	}
	var applied []appliedSub

	for _, group := range newGroups {
		var toSubscribe []Instrument
		for _, ins := range group.Instruments {
			if _, ok := addedSet[ins.Ticker]; ok {
				toSubscribe = append(toSubscribe, ins)
			}
		}
		if len(toSubscribe) == 0 {
			continue
		}
		if err := m.subscriber.Subscribe(ctx, group.StreamID, group.Timeframe, toSubscribe); err != nil {
			// Rollback: отписываем всё, что успели подписать на этом шаге.
			for _, a := range applied {
				_ = m.subscriber.Unsubscribe(ctx, a.streamID, a.timeframe, a.instruments)
			}
			return fmt.Errorf("subscribe %s: %w", group.Timeframe, err)
		}
		applied = append(applied, appliedSub{streamID: group.StreamID, timeframe: group.Timeframe, instruments: toSubscribe})
	}

	if len(removed) == 0 {
		return nil
	}

	removedSet := make(map[string]struct{}, len(removed))
	for _, t := range removed {
		removedSet[t] = struct{}{}
	}

	oldGroups := m.state.Snapshot()
	for _, g := range oldGroups {
		var toUnsubscribe []Instrument
		for _, ins := range g.Instruments {
			if _, ok := removedSet[ins.Ticker]; ok {
				toUnsubscribe = append(toUnsubscribe, ins)
			}
		}
		if len(toUnsubscribe) == 0 {
			continue
		}
		if err := m.subscriber.Unsubscribe(ctx, g.StreamID, g.Timeframe, toUnsubscribe); err != nil {
			// Новые подписки уже применены и рабочие — отписку от старых
			// инструментов сервис безопасно повторит на следующем PUT.
			return fmt.Errorf("unsubscribe %s: %w", g.Timeframe, err)
		}
	}

	return nil
}
