package tinkoff

import pb "github.com/tinkoff/invest-api-go-sdk/proto"

// candlePayload — нормализованное представление свечи, публикуемое в
// топик market.candles (см. 2.6 и структуру таблицы candles в 6.1).
type candlePayload struct {
	Figi          string  `json:"figi"`
	Ticker        string  `json:"ticker"`
	Interval      string  `json:"interval"`
	Open          float64 `json:"open"`
	High          float64 `json:"high"`
	Low           float64 `json:"low"`
	Close         float64 `json:"close"`
	Volume        int64   `json:"volume"`
	Time          string  `json:"time"`
	InstrumentUID string  `json:"instrument_uid"`
}

func quotationToFloat(q *pb.Quotation) float64 {
	if q == nil {
		return 0
	}
	return float64(q.GetUnits()) + float64(q.GetNano())/1e9
}

func intervalToTimeframe(interval pb.SubscriptionInterval) string {
	switch interval {
	case pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE:
		return "1min"
	case pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_FIVE_MINUTES:
		return "5min"
	default:
		return "unknown"
	}
}

func newCandlePayload(c *pb.Candle, ticker string) candlePayload {
	return candlePayload{
		Figi:          c.GetFigi(),
		Ticker:        ticker,
		Interval:      intervalToTimeframe(c.GetInterval()),
		Open:          quotationToFloat(c.GetOpen()),
		High:          quotationToFloat(c.GetHigh()),
		Low:           quotationToFloat(c.GetLow()),
		Close:         quotationToFloat(c.GetClose()),
		Volume:        c.GetVolume(),
		Time:          c.GetTime().AsTime().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
		InstrumentUID: c.GetInstrumentUid(),
	}
}
