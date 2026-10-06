// Package subscription управляет текущим состоянием подписок Collector-а
// на свечи T-Invest API и применяет изменения, полученные через REST API
// (см. 2.7 аналитики).
package subscription

import "sync"

// Instrument — тикер с его FIGI-идентификатором.
type Instrument struct {
	Ticker string `json:"ticker"`
	Figi   string `json:"figi"`
}

// StreamGroup — набор инструментов одного таймфрейма, закреплённый за
// конкретным stream-соединением.
type StreamGroup struct {
	StreamID    int          `json:"stream_id"`
	Timeframe   string       `json:"timeframe"`
	Instruments []Instrument `json:"instruments"`
}

// State хранит текущее состояние подписок.
//
//   - GET читает под RLock.
//   - PUT изменяет под Lock.
//   - Сами операции Subscribe/Unsubscribe к T-Invest API выполняются вне
//     блокировки (в Manager.Apply), чтобы не тормозить GET.
type State struct {
	mu     sync.RWMutex
	groups []StreamGroup
}

// NewState создаёт пустое состояние.
func NewState() *State {
	return &State{}
}

// Snapshot возвращает копию текущих групп подписок (под RLock).
func (s *State) Snapshot() []StreamGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]StreamGroup, len(s.groups))
	for i, g := range s.groups {
		instruments := make([]Instrument, len(g.Instruments))
		copy(instruments, g.Instruments)
		out[i] = StreamGroup{StreamID: g.StreamID, Timeframe: g.Timeframe, Instruments: instruments}
	}
	return out
}

// Replace атомарно заменяет состояние целиком (под Lock).
func (s *State) Replace(groups []StreamGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups = groups
}

// TickerSet возвращает множество тикеров, находящихся сейчас в подписке.
func (s *State) TickerSet() map[string]Instrument {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]Instrument)
	for _, g := range s.groups {
		for _, ins := range g.Instruments {
			out[ins.Ticker] = ins
		}
	}
	return out
}

// Count возвращает суммарное число подписанных инструментов и число
// активных stream-соединений.
func (s *State) Count() (instruments int, streams int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[int]struct{})
	for _, g := range s.groups {
		instruments += len(g.Instruments)
		seen[g.StreamID] = struct{}{}
	}
	return instruments, len(seen)
}
