package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
		{"CIK0000320193", "0000320193"},
		{"0", "0000000000"},
		{"", "0000000000"},
		{"12345678901", "2345678901"},
		{" 000320193 ", "0000320193"},
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

	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "test-figi-key", "test-tiingo-key", "test-twelve-key", "test-fmp-key")
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

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	if cb.State() != StateClosed {
		t.Errorf("Expected initial state StateClosed, got %v", cb.State())
	}

	failErr := fmt.Errorf("api failure")

	// First failure - still closed
	_ = cb.Execute(func() error { return failErr })
	if cb.State() != StateClosed {
		t.Errorf("Expected StateClosed after 1 failure, got %v", cb.State())
	}

	// Second failure - trips to open
	_ = cb.Execute(func() error { return failErr })
	if cb.State() != StateOpen {
		t.Errorf("Expected StateOpen after 2 failures, got %v", cb.State())
	}

	// Immediate call should fail with circuit breaker open error
	err := cb.Execute(func() error { return nil })
	if err == nil || err.Error() != "circuit breaker open" {
		t.Errorf("Expected 'circuit breaker open' error, got %v", err)
	}

	// Wait for cooldown
	time.Sleep(60 * time.Millisecond)

	// Next call should transition to HalfOpen and execute function
	executed := false
	_ = cb.Execute(func() error {
		executed = true
		return nil
	})

	if !executed {
		t.Errorf("Expected function to execute in half-open state")
	}

	if cb.State() != StateClosed {
		t.Errorf("Expected StateClosed after successful half-open execution, got %v", cb.State())
	}
}

type mockTripper struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestMarketDataWaterfall(t *testing.T) {
	finnhubStatus := http.StatusTooManyRequests // 429
	tiingoStatus := http.StatusOK
	twelveStatus := http.StatusOK

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/api/v1/quote"):
			w.WriteHeader(finnhubStatus)
			if finnhubStatus == http.StatusOK {
				json.NewEncoder(w).Encode(FinnhubQuote{CurrentPrice: 150.0, OpenPriceOfDay: 148.0})
			} else {
				w.Write([]byte("Rate limit exceeded"))
			}
		case strings.Contains(r.URL.Path, "/tiingo/daily/"):
			w.WriteHeader(tiingoStatus)
			if tiingoStatus == http.StatusOK {
				json.NewEncoder(w).Encode([]TiingoPrice{{Close: 152.0, Open: 151.0}})
			} else {
				w.Write([]byte("Error"))
			}
		case strings.Contains(r.URL.Path, "/quote"):
			w.WriteHeader(twelveStatus)
			if twelveStatus == http.StatusOK {
				json.NewEncoder(w).Encode(TwelveDataQuote{Close: "155.0", Open: "154.0", Status: "ok"})
			} else {
				w.Write([]byte("Error"))
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cm := NewClientManager("TestApp", "fh", "figi", "tiingo", "twelve", "fmp")
	cm.httpClient = server.Client()

	// Replace URLs for testing by custom transport or testing waterfall logic directly
	// Since URL is hardcoded in fetchers, let's test that waterfall falls through correctly when Finnhub fails
	// We can test individual fetchers or mock HTTP transport
	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				if strings.Contains(req.URL.String(), "finnhub.io") {
					rec.WriteHeader(http.StatusTooManyRequests)
					rec.WriteString("429 Too Many Requests")
				} else if strings.Contains(req.URL.String(), "tiingo.com") {
					rec.WriteHeader(http.StatusOK)
					rec.Header().Set("Content-Type", "application/json")
					rec.WriteString(`[{"close": 152.5, "open": 150.0, "high": 153.0, "low": 149.5, "volume": 500000}]`)
				} else if strings.Contains(req.URL.String(), "twelvedata.com") {
					rec.WriteHeader(http.StatusOK)
					rec.Header().Set("Content-Type", "application/json")
					rec.WriteString(`{"close": "155.0", "open": "154.0", "high": "156.0", "low": "153.0", "volume": "600000", "status": "ok"}`)
				} else {
					rec.WriteHeader(http.StatusNotFound)
				}
				return rec.Result(), nil
			},
		},
	}

	quote, err := cm.FetchMarketDataWaterfall(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchMarketDataWaterfall failed: %v", err)
	}
	if quote.Source != "Tiingo" || quote.CurrentPrice != 152.5 {
		t.Errorf("Expected fallback to Tiingo with price 152.5, got source %s price %f", quote.Source, quote.CurrentPrice)
	}
}

func TestFetchSECCIKFormatted(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "", "", "", "", "")
	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				rec.WriteString(`{
					"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}
				}`)
				return rec.Result(), nil
			},
		},
	}

	cik, err := cm.FetchSECCIKForTicker(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchSECCIKForTicker failed: %v", err)
	}
	if cik != "0000320193" {
		t.Errorf("Expected padded CIK '0000320193', got '%s'", cik)
	}
}
