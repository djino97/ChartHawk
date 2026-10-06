package subscription

import (
	"context"
	"errors"
	"sort"
	"testing"
)

type fakeResolver struct {
	byTicker map[string]string
}

func (f *fakeResolver) Resolve(ticker string) (string, bool) {
	figi, ok := f.byTicker[ticker]
	return figi, ok
}

type call struct {
	op          string
	streamID    int
	timeframe   string
	instruments []Instrument
}

type fakeSubscriber struct {
	calls     []call
	failOn    string // ticker that causes Subscribe/Unsubscribe to fail
	unsubErrs int
}

func (f *fakeSubscriber) Subscribe(_ context.Context, streamID int, timeframe string, instruments []Instrument) error {
	for _, ins := range instruments {
		if ins.Ticker == f.failOn {
			return errors.New("boom")
		}
	}
	f.calls = append(f.calls, call{op: "subscribe", streamID: streamID, timeframe: timeframe, instruments: instruments})
	return nil
}

func (f *fakeSubscriber) Unsubscribe(_ context.Context, streamID int, timeframe string, instruments []Instrument) error {
	f.calls = append(f.calls, call{op: "unsubscribe", streamID: streamID, timeframe: timeframe, instruments: instruments})
	return nil
}

func newTestManager(resolver map[string]string, sub *fakeSubscriber, maxPerStream, maxStreams int) *Manager {
	state := NewState()
	return NewManager(state, &fakeResolver{byTicker: resolver}, sub, maxPerStream, maxStreams, []string{"1min", "5min"})
}

func TestApply_EmptyRequest(t *testing.T) {
	m := newTestManager(nil, &fakeSubscriber{}, 300, 1)
	_, err := m.Apply(context.Background(), nil)
	assertValidationErr(t, err)
}

func TestApply_UnknownTimeframe(t *testing.T) {
	m := newTestManager(map[string]string{"SBER": "FIGI1"}, &fakeSubscriber{}, 300, 1)
	_, err := m.Apply(context.Background(), []RequestGroup{{Timeframe: "1h", Instruments: []string{"SBER"}}})
	assertValidationErr(t, err)
}

func TestApply_DuplicateTicker(t *testing.T) {
	m := newTestManager(map[string]string{"SBER": "FIGI1"}, &fakeSubscriber{}, 300, 1)
	_, err := m.Apply(context.Background(), []RequestGroup{{Timeframe: "1min", Instruments: []string{"SBER", "SBER"}}})
	assertValidationErr(t, err)
}

func TestApply_UnknownTicker(t *testing.T) {
	m := newTestManager(map[string]string{"SBER": "FIGI1"}, &fakeSubscriber{}, 300, 1)
	_, err := m.Apply(context.Background(), []RequestGroup{{Timeframe: "1min", Instruments: []string{"XXXX"}}})

	var subErr *Error
	if !errors.As(err, &subErr) || subErr.Kind != ErrKindUnknownTicker {
		t.Fatalf("expected ErrKindUnknownTicker, got %v", err)
	}
	if subErr.Ticker != "XXXX" {
		t.Fatalf("expected ticker XXXX in error, got %q", subErr.Ticker)
	}
}

func TestApply_LimitExceeded(t *testing.T) {
	resolver := map[string]string{"A": "FA", "B": "FB", "C": "FC"}
	m := newTestManager(resolver, &fakeSubscriber{}, 2, 1)
	_, err := m.Apply(context.Background(), []RequestGroup{{Timeframe: "1min", Instruments: []string{"A", "B", "C"}}})

	var subErr *Error
	if !errors.As(err, &subErr) || subErr.Kind != ErrKindLimitExceeded {
		t.Fatalf("expected ErrKindLimitExceeded, got %v", err)
	}
}

func TestApply_Success_DiffAndState(t *testing.T) {
	resolver := map[string]string{
		"SBER": "BBG004730N88", "GAZP": "BBG00475KKY8", "LKOH": "BBG004731032",
		"YDEX": "TCS00A107T19", "VKCO": "RU000A106T36", "OZON": "BBG00Y91R9T3",
	}
	sub := &fakeSubscriber{}
	m := newTestManager(resolver, sub, 300, 1)

	// Первичное применение.
	res, err := m.Apply(context.Background(), []RequestGroup{
		{Timeframe: "1min", Instruments: []string{"SBER", "GAZP", "LKOH"}},
		{Timeframe: "5min", Instruments: []string{"YDEX", "VKCO"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.TotalInstruments != 5 || res.ActiveStreams != 1 {
		t.Fatalf("unexpected counts: total=%d streams=%d", res.TotalInstruments, res.ActiveStreams)
	}
	sort.Strings(res.Added)
	assertStringSlice(t, res.Added, []string{"GAZP", "LKOH", "SBER", "VKCO", "YDEX"})
	assertStringSlice(t, res.Removed, nil)

	// Второе применение: GAZP уходит, OZON добавляется, остальные без изменений.
	res2, err := m.Apply(context.Background(), []RequestGroup{
		{Timeframe: "1min", Instruments: []string{"SBER", "LKOH"}},
		{Timeframe: "5min", Instruments: []string{"YDEX", "VKCO", "OZON"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStringSlice(t, res2.Added, []string{"OZON"})
	assertStringSlice(t, res2.Removed, []string{"GAZP"})
	assertStringSlice(t, res2.Unchanged, []string{"LKOH", "SBER", "VKCO", "YDEX"})

	instrCount, _ := m.State().Count()
	if instrCount != 5 {
		t.Fatalf("expected 5 instruments in state, got %d", instrCount)
	}
}

func TestApply_RollbackOnSubscribeFailure(t *testing.T) {
	resolver := map[string]string{"SBER": "F1", "GAZP": "F2", "BAD": "F3"}
	sub := &fakeSubscriber{failOn: "BAD"}
	m := newTestManager(resolver, sub, 300, 1)

	// Первичное успешное состояние.
	if _, err := m.Apply(context.Background(), []RequestGroup{
		{Timeframe: "1min", Instruments: []string{"SBER"}},
	}); err != nil {
		t.Fatalf("unexpected error priming state: %v", err)
	}

	// Второе применение должно провалиться на подписке BAD и не изменить состояние.
	_, err := m.Apply(context.Background(), []RequestGroup{
		{Timeframe: "1min", Instruments: []string{"SBER", "GAZP"}},
		{Timeframe: "5min", Instruments: []string{"BAD"}},
	})

	var subErr *Error
	if !errors.As(err, &subErr) || subErr.Kind != ErrKindApply {
		t.Fatalf("expected ErrKindApply, got %v", err)
	}

	tickers := m.State().TickerSet()
	if _, ok := tickers["GAZP"]; ok {
		t.Fatalf("state must not change on apply failure, but GAZP was added")
	}
	if len(tickers) != 1 {
		t.Fatalf("expected state to remain at 1 instrument, got %d", len(tickers))
	}
}

func assertValidationErr(t *testing.T, err error) {
	t.Helper()
	var subErr *Error
	if !errors.As(err, &subErr) || subErr.Kind != ErrKindValidation {
		t.Fatalf("expected ErrKindValidation, got %v", err)
	}
}

func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("mismatch at %d: got %v want %v", i, got, want)
		}
	}
}
