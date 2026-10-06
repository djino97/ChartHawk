package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"

	"charthawk/collector/internal/subscription"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeResolver struct{ byTicker map[string]string }

func (f *fakeResolver) Resolve(ticker string) (string, bool) {
	figi, ok := f.byTicker[ticker]
	return figi, ok
}

type fakeSubscriber struct{}

func (f *fakeSubscriber) Subscribe(context.Context, int, string, []subscription.Instrument) error {
	return nil
}
func (f *fakeSubscriber) Unsubscribe(context.Context, int, string, []subscription.Instrument) error {
	return nil
}

func newTestServer(t *testing.T) (http.Handler, *subscription.Manager) {
	t.Helper()
	resolver := &fakeResolver{byTicker: map[string]string{
		"SBER": "BBG004730N88", "GAZP": "BBG00475KKY8",
	}}
	manager := subscription.NewManager(subscription.NewState(), resolver, &fakeSubscriber{}, 300, 1, []string{"1min", "5min"})
	h := NewHandler(manager, discardLogger())

	mux := http.NewServeMux()
	h.Routes(mux)

	var chain http.Handler = mux
	chain = Auth("secret")(chain)

	return chain, manager
}

func doRequest(h http.Handler, method, body, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/v1/subscriptions", bytes.NewBufferString(body))
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandler_Unauthorized(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doRequest(h, http.MethodGet, "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	rec = doRequest(h, http.MethodGet, "", "Bearer wrong-key")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for wrong key, got %d", rec.Code)
	}
}

func TestHandler_GetEmpty(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doRequest(h, http.MethodGet, "", "Bearer secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp getSubscriptionsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.TotalInstruments != 0 {
		t.Fatalf("expected empty subscriptions, got %+v", resp)
	}
}

func TestHandler_PutAndGet(t *testing.T) {
	h, _ := newTestServer(t)

	putBody := `{"subscriptions":[{"timeframe":"1min","instruments":["SBER","GAZP"]}]}`
	rec := doRequest(h, http.MethodPut, putBody, "Bearer secret")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var putResp putSubscriptionsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &putResp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if putResp.Status != "applied" || putResp.TotalInstruments != 2 {
		t.Fatalf("unexpected response: %+v", putResp)
	}

	rec = doRequest(h, http.MethodGet, "", "Bearer secret")
	var getResp getSubscriptionsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &getResp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if getResp.TotalInstruments != 2 || len(getResp.Streams) != 1 {
		t.Fatalf("unexpected state after PUT: %+v", getResp)
	}
}

func TestHandler_PutEmptyBody400(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doRequest(h, http.MethodPut, `{"subscriptions":[]}`, "Bearer secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandler_PutInvalidJSON400(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doRequest(h, http.MethodPut, `not-json`, "Bearer secret")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandler_PutUnknownTicker422(t *testing.T) {
	h, _ := newTestServer(t)
	rec := doRequest(h, http.MethodPut, `{"subscriptions":[{"timeframe":"1min","instruments":["XXXX"]}]}`, "Bearer secret")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_RateLimit429(t *testing.T) {
	h, _ := newTestServer(t)
	// Оборачиваем в лимитер с burst=1, чтобы быстро словить 429 в тесте.
	var chain http.Handler = h
	chain = RateLimit(rate.NewLimiter(0, 1))(chain)

	body := `{"subscriptions":[{"timeframe":"1min","instruments":["SBER"]}]}`
	first := doRequest(chain, http.MethodPut, body, "Bearer secret")
	if first.Code != http.StatusOK {
		t.Fatalf("expected first PUT to succeed, got %d: %s", first.Code, first.Body.String())
	}
	second := doRequest(chain, http.MethodPut, body, "Bearer secret")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", second.Code)
	}
}
