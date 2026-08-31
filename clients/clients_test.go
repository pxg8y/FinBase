package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFormatCIK(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"320193", "0000320193"},
		{"0000320193", "0000320193"},
		{"0", "0000000000"},
		{"", "0000000000"},
		{"12345678901", "12345678901"},
	}

	for _, tt := range tests {
		result := FormatCIK(tt.input)
		if result != tt.expected {
			t.Errorf("FormatCIK(%q) = %q; expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestBufferPoolReuse(t *testing.T) {
	buf := BufferPool.Get()
	if buf == nil {
		t.Fatal("Expected buffer from pool, got nil")
	}
	BufferPool.Put(buf)
}

func TestClientManagerMockAPIs(t *testing.T) {
	// Mock HTTP Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v3/mapping":
			if r.Header.Get("X-OPENFIGI-KEY") != "test-figi-key" {
				t.Errorf("Expected X-OPENFIGI-KEY header")
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode([]OpenFIGIResult{
				{
					Data: []struct {
						FIGI                string `json:"figi"`
						Name                string `json:"name"`
						Ticker              string `json:"ticker"`
						ExchCode            string `json:"exchCode"`
						CompositeFIGI       string `json:"compositeFIGI"`
						SecurityType        string `json:"securityType"`
						MarketSector        string `json:"marketSector"`
						ShareClassFIGI      string `json:"shareClassFIGI"`
						SecurityType2       string `json:"securityType2"`
						SecurityDescription string `json:"securityDescription"`
					}{
						{
							FIGI:         "BBG000B9XRY4",
							Name:         "APPLE INC",
							Ticker:       "AAPL",
							MarketSector: "Equity",
						},
					},
				},
			})
		case "/api/xbrl/companyfacts/CIK0000320193.json":
			if r.Header.Get("User-Agent") != "TestApp user@test.com" {
				t.Errorf("Expected custom User-Agent header, got %s", r.Header.Get("User-Agent"))
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(SECCompanyFacts{
				CIK:        320193,
				EntityName: "Apple Inc.",
			})
		case "/api/v1/quote":
			if r.Header.Get("X-Finnhub-Token") != "test-finnhub-key" {
				t.Errorf("Expected X-Finnhub-Token header")
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(FinnhubQuote{
				CurrentPrice: 175.50,
				Timestamp:    time.Now().Unix(),
			})
		case "/api/v1/stock/profile2":
			if r.Header.Get("X-Finnhub-Token") != "test-finnhub-key" {
				t.Errorf("Expected X-Finnhub-Token header")
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(FinnhubProfile{
				Name:            "Apple Inc.",
				Ticker:          "AAPL",
				FinnhubIndustry: "Technology",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "test-figi-key")
	cm.httpClient = server.Client()

	// Redirect transport to mock server
	cm.httpClient = server.Client()

	ctx := context.Background()

	// SEC EDGAR test with custom format
	cikFormatted := FormatCIK("320193")
	req, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/xbrl/companyfacts/CIK"+cikFormatted+".json", nil)
	if err != nil {
		t.Fatalf("Failed to build req: %v", err)
	}
	req.Header.Set("User-Agent", cm.secUserAgent)
	resp, err := cm.httpClient.Do(req)
	if err != nil {
		t.Fatalf("SEC mock request failed: %v", err)
	}
	defer resp.Body.Close()

	var secFacts SECCompanyFacts
	if err := json.NewDecoder(resp.Body).Decode(&secFacts); err != nil {
		t.Fatalf("Failed to decode sec facts: %v", err)
	}
	if secFacts.EntityName != "Apple Inc." {
		t.Errorf("Unexpected entity name: %s", secFacts.EntityName)
	}
}
