package tinkoff

import (
	"fmt"

	pb "github.com/tinkoff/invest-api-go-sdk/proto"

	"charthawk/collector/internal/subscription"
)

// AllowedTimeframes — список таймфреймов, поддерживаемых текущей версией
// T-Invest API SDK для стрима свечей.
var AllowedTimeframes = []string{"1min", "5min"}

func timeframeToInterval(timeframe string) (pb.SubscriptionInterval, error) {
	switch timeframe {
	case "1min":
		return pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE, nil
	case "5min":
		return pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_FIVE_MINUTES, nil
	default:
		return pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_UNSPECIFIED, fmt.Errorf("unsupported timeframe %q", timeframe)
	}
}

func figisOf(instruments []subscription.Instrument) []string {
	figis := make([]string, len(instruments))
	for i, ins := range instruments {
		figis[i] = ins.Figi
	}
	return figis
}
