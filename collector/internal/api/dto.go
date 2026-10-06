// Package api — HTTP-контроллер REST API управления подписками (2.7).
package api

import "charthawk/collector/internal/subscription"

// --- GET /api/v1/subscriptions ---

type instrumentDTO struct {
	Ticker string `json:"ticker"`
	Figi   string `json:"figi"`
}

type streamDTO struct {
	StreamID    int             `json:"stream_id"`
	Timeframe   string          `json:"timeframe"`
	Instruments []instrumentDTO `json:"instruments"`
}

type getSubscriptionsResponse struct {
	Streams          []streamDTO `json:"streams"`
	TotalInstruments int         `json:"total_instruments"`
	ActiveStreams    int         `json:"active_streams"`
}

func toGetResponse(groups []subscription.StreamGroup) getSubscriptionsResponse {
	streams := make([]streamDTO, 0, len(groups))
	total := 0
	activeStreams := make(map[int]struct{})
	for _, g := range groups {
		instruments := make([]instrumentDTO, 0, len(g.Instruments))
		for _, ins := range g.Instruments {
			instruments = append(instruments, instrumentDTO{Ticker: ins.Ticker, Figi: ins.Figi})
		}
		streams = append(streams, streamDTO{StreamID: g.StreamID, Timeframe: g.Timeframe, Instruments: instruments})
		total += len(g.Instruments)
		activeStreams[g.StreamID] = struct{}{}
	}
	return getSubscriptionsResponse{
		Streams:          streams,
		TotalInstruments: total,
		ActiveStreams:    len(activeStreams),
	}
}

// --- PUT /api/v1/subscriptions ---

type putSubscriptionGroup struct {
	Timeframe   string   `json:"timeframe"`
	Instruments []string `json:"instruments"`
}

type putSubscriptionsRequest struct {
	Subscriptions []putSubscriptionGroup `json:"subscriptions"`
}

type putSubscriptionsResponse struct {
	Status           string   `json:"status"`
	TotalInstruments int      `json:"total_instruments"`
	ActiveStreams    int      `json:"active_streams"`
	Added            []string `json:"added"`
	Removed          []string `json:"removed"`
	Unchanged        []string `json:"unchanged"`
}

func toPutResponse(r *subscription.ApplyResult) putSubscriptionsResponse {
	return putSubscriptionsResponse{
		Status:           "applied",
		TotalInstruments: r.TotalInstruments,
		ActiveStreams:    r.ActiveStreams,
		Added:            emptyIfNil(r.Added),
		Removed:          emptyIfNil(r.Removed),
		Unchanged:        emptyIfNil(r.Unchanged),
	}
}

func emptyIfNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// --- Ошибки ---

type errorResponse struct {
	Error   string            `json:"error"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}
