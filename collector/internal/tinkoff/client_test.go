package tinkoff

import (
	"testing"

	"charthawk/collector/internal/config"
)

func TestAccountIDForSDK(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		account  string
		want     string
	}{
		{
			name:     "production empty skips sandbox bootstrap",
			endpoint: "invest-public-api.tinkoff.ru:443",
			want:     "charthawk-market-data",
		},
		{
			name:     "production explicit account is kept",
			endpoint: "invest-public-api.tinkoff.ru:443",
			account:  "2000012345",
			want:     "2000012345",
		},
		{
			name:     "sandbox empty lets sdk open an account",
			endpoint: "sandbox-invest-public-api.tinkoff.ru:443",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := accountIDForSDK(config.TinkoffConfig{EndPoint: tt.endpoint, AccountID: tt.account})
			if got != tt.want {
				t.Fatalf("accountIDForSDK() = %q, want %q", got, tt.want)
			}
		})
	}
}
