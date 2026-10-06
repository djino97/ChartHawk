package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"charthawk/collector/internal/subscription"
)

// Handler — HTTP-контроллер REST API управления подписками.
type Handler struct {
	manager *subscription.Manager
	logger  *slog.Logger
}

// NewHandler создаёт HTTP-контроллер.
func NewHandler(manager *subscription.Manager, logger *slog.Logger) *Handler {
	return &Handler{manager: manager, logger: logger}
}

// Routes регистрирует эндпоинты на переданном mux.
func (h *Handler) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/subscriptions", h.handleSubscriptions)
}

func (h *Handler) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPut:
		h.handlePut(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "only GET and PUT are supported")
	}
}

func (h *Handler) handleGet(w http.ResponseWriter, _ *http.Request) {
	groups := h.manager.State().Snapshot()
	writeJSON(w, http.StatusOK, toGetResponse(groups))
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putSubscriptionsRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is not valid JSON: "+err.Error())
		return
	}

	if len(req.Subscriptions) == 0 {
		writeError(w, http.StatusBadRequest, "empty_subscriptions", "subscriptions list must not be empty")
		return
	}

	groups := make([]subscription.RequestGroup, len(req.Subscriptions))
	for i, g := range req.Subscriptions {
		groups[i] = subscription.RequestGroup{Timeframe: g.Timeframe, Instruments: g.Instruments}
	}

	result, err := h.manager.Apply(r.Context(), groups)
	if err != nil {
		h.writeApplyError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, toPutResponse(result))
}

func (h *Handler) writeApplyError(w http.ResponseWriter, err error) {
	var subErr *subscription.Error
	if !errors.As(err, &subErr) {
		h.logger.Error("unexpected error applying subscriptions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to apply subscriptions")
		return
	}

	switch subErr.Kind {
	case subscription.ErrKindValidation:
		writeError(w, http.StatusBadRequest, "invalid_request", subErr.Message)
	case subscription.ErrKindUnknownTicker:
		resp := errorResponse{
			Error:   "unknown_ticker",
			Message: subErr.Message,
			Details: map[string]string{"ticker": subErr.Ticker},
		}
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	case subscription.ErrKindLimitExceeded:
		writeError(w, http.StatusConflict, "subscription_limit_exceeded", subErr.Message)
	case subscription.ErrKindApply:
		h.logger.Error("failed to apply subscriptions to stream", "error", subErr.Err)
		writeError(w, http.StatusInternalServerError, "apply_failed", "failed to apply subscriptions")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", subErr.Message)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}
