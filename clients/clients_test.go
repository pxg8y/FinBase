package clients

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPSTrustStore(t *testing.T) {
	// Start an HTTPS mock server with a TLS certificate
	tlsServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer tlsServer.Close()

	// Verify HTTPS client connection using valid TLS certificate pool without InsecureSkipVerify
	client := tlsServer.Client()
	resp, err := client.Get(tlsServer.URL)
	if err != nil {
		t.Fatalf("Failed HTTPS request with valid TLS trust pool: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 OK from HTTPS request, got %d", resp.StatusCode)
	}
}

func TestCredentialRedaction(t *testing.T) {
	apiKey := "super-secret-finnhub-key-12345"
	tiingoKey := "secret-tiingo-token-67890"

	cm := NewClientManager("TestApp", apiKey, "", tiingoKey, "", "")

	// Test direct API key string redaction
	rawErr := fmt.Errorf("failed to fetch from https://finnhub.io/api/v1/quote?symbol=AAPL&token=%s with key %s", tiingoKey, apiKey)
	redactedErr := cm.redact(rawErr)

	if strings.Contains(redactedErr.Error(), apiKey) {
		t.Errorf("Error contains unredacted finnhub API key: %s", redactedErr.Error())
	}
	if strings.Contains(redactedErr.Error(), tiingoKey) {
		t.Errorf("Error contains unredacted tiingo API key: %s", redactedErr.Error())
	}
	if !strings.Contains(redactedErr.Error(), "[REDACTED]") {
		t.Errorf("Expected [REDACTED] in error string, got %s", redactedErr.Error())
	}

	// Test URL pattern redaction for apikey= and token=
	patternErrStr := RedactString("HTTP 401: https://api.twelvedata.com/quote?symbol=AAPL&apikey=secret123&token=abc456")
	if strings.Contains(patternErrStr, "secret123") || strings.Contains(patternErrStr, "abc456") {
		t.Errorf("Failed to redact URL query parameters: %s", patternErrStr)
	}
	if !strings.Contains(patternErrStr, "apikey=[REDACTED]") || !strings.Contains(patternErrStr, "token=[REDACTED]") {
		t.Errorf("Expected parameter redaction in URL string, got %s", patternErrStr)
	}
}

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

func TestFetchFinnhubValuationRatios(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "", "", "", "")

	// Test 1: Successful response (200 OK)
	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				if req.Header.Get("X-Finnhub-Token") != "test-finnhub-key" {
					t.Errorf("Expected X-Finnhub-Token header")
				}
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				rec.WriteString(`{
					"metric": {
						"peNormalizedAnnual": 28.5,
						"pbAnnual": 45.2,
						"psAnnual": 7.8,
						"grossMarginAnnual": 44.1,
						"operatingMarginAnnual": 30.2,
						"netProfitMarginAnnual": 25.3,
						"roeTTM": 160.5,
						"roaTTM": 28.4,
						"totalDebt/totalEquityAnnual": 1.8
					}
				}`)
				return rec.Result(), nil
			},
		},
	}

	ratios, err := cm.FetchFinnhubValuationRatios(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubValuationRatios failed: %v", err)
	}
	if ratios.PERatio != 28.5 || ratios.GrossMargin != 44.1 || ratios.DebtToEquity != 1.8 {
		t.Errorf("Unexpected valuation ratios: %+v", ratios)
	}

	// Test 2: Rate limit response (429)
	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.WriteHeader(http.StatusTooManyRequests)
				rec.WriteString(`{"error": "API limit reached"}`)
				return rec.Result(), nil
			},
		},
	}

	_, err = cm.FetchFinnhubValuationRatios(context.Background(), "AAPL")
	if err == nil || !strings.Contains(err.Error(), "429") {
		t.Errorf("Expected 429 rate limit error, got %v", err)
	}
}

func TestFetchFinnhubDividendsAndSplits(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "", "test-tiingo-key", "", "")

	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				if strings.Contains(req.URL.Path, "/stock/dividend") {
					rec.WriteString(`[
						{"date": "2023-11-10", "payDate": "2023-11-16", "recordDate": "2023-11-13", "amount": 0.24, "currency": "USD"}
					]`)
				} else if strings.Contains(req.URL.Path, "/stock/split") {
					rec.WriteString(`[
						{"date": "2020-08-31", "fromFactor": 1, "toFactor": 4}
					]`)
				} else {
					rec.WriteHeader(http.StatusNotFound)
				}
				return rec.Result(), nil
			},
		},
	}

	divs, err := cm.FetchFinnhubDividends(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubDividends failed: %v", err)
	}
	if len(divs) != 1 || divs[0].Amount != 0.24 {
		t.Errorf("Unexpected dividends: %+v", divs)
	}

	splits, err := cm.FetchFinnhubSplits(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubSplits failed: %v", err)
	}
	if len(splits) != 1 || splits[0].ToFactor != 4 {
		t.Errorf("Unexpected splits: %+v", splits)
	}
}

func TestFetchTiingoHistoricalPrices(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "", "", "test-tiingo-key", "", "")
	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				rec.WriteString(`[
					{"date": "2023-11-10T00:00:00.000Z", "close": 186.4, "high": 186.57, "low": 183.53, "open": 183.97, "volume": 56900000, "adjClose": 186.4}
				]`)
				return rec.Result(), nil
			},
		},
	}

	prices, err := cm.FetchTiingoHistoricalPrices(context.Background(), "AAPL", "2023-11-01", "2023-11-10")
	if err != nil {
		t.Fatalf("FetchTiingoHistoricalPrices failed: %v", err)
	}
	if len(prices) != 1 || prices[0].ClosePrice != 186.4 {
		t.Errorf("Unexpected historical prices: %+v", prices)
	}
}

func TestFetchFinnhubAnalystEarningsAndNews(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "", "", "", "")

	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				if strings.Contains(req.URL.Path, "/stock/recommendation") {
					rec.WriteString(`[{"period": "2023-11-01", "strongBuy": 10, "buy": 15, "hold": 5, "sell": 1, "strongSell": 0}]`)
				} else if strings.Contains(req.URL.Path, "/calendar/earnings") {
					rec.WriteString(`{"earningsCalendar": [{"date": "2023-11-02", "quarter": 4, "year": 2023, "epsEstimate": 1.39, "epsActual": 1.46, "revenueEstimate": 89300000000, "revenueActual": 89500000000}]}`)
				} else if strings.Contains(req.URL.Path, "/company-news") {
					rec.WriteString(`[{"id": 1001, "headline": "Test Headline", "summary": "Test Summary", "source": "Test Source", "url": "https://example.com", "datetime": 1699000000, "sentiment": {"score": 0.8}}]`)
				} else {
					rec.WriteHeader(http.StatusNotFound)
				}
				return rec.Result(), nil
			},
		},
	}

	estimates, err := cm.FetchFinnhubAnalystEstimates(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubAnalystEstimates failed: %v", err)
	}
	if len(estimates) != 1 || estimates[0].StrongBuy != 10 {
		t.Errorf("Unexpected analyst estimates: %+v", estimates)
	}

	earnings, err := cm.FetchFinnhubEarningsCalendar(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubEarningsCalendar failed: %v", err)
	}
	if len(earnings) != 1 || earnings[0].EPSActual != 1.46 {
		t.Errorf("Unexpected earnings calendar: %+v", earnings)
	}

	news, err := cm.FetchFinnhubCompanyNews(context.Background(), "AAPL", "2023-10-01", "2023-11-01")
	if err != nil {
		t.Fatalf("FetchFinnhubCompanyNews failed: %v", err)
	}
	if len(news) != 1 || news[0].Headline != "Test Headline" || news[0].SentimentScore != 0.8 {
		t.Errorf("Unexpected news items: %+v", news)
	}
}

func TestFetchInsiderAndFRED(t *testing.T) {
	cm := NewClientManager("TestApp user@test.com", "test-finnhub-key", "", "", "", "")
	cm.SetFREDAPIKey("test-fred-key")

	cm.httpClient = &http.Client{
		Transport: &mockTripper{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.Header().Set("Content-Type", "application/json")
				if strings.Contains(req.URL.Path, "/stock/insider-transactions") {
					rec.WriteString(`{"data": [{"name": "Cook Timothy D", "share": 3000000, "change": -50000, "filingDate": "2023-10-15", "transactionCode": "S", "transactionPrice": 178.5}]}`)
				} else if strings.Contains(req.URL.Path, "/fred/series/observations") {
					if !strings.Contains(req.URL.RawQuery, "api_key=test-fred-key") {
						t.Errorf("Expected api_key in query params for FRED")
					}
					rec.WriteString(`{"observations": [{"date": "2023-11-01", "value": "4.52"}]}`)
				} else {
					rec.WriteHeader(http.StatusNotFound)
				}
				return rec.Result(), nil
			},
		},
	}

	insiders, err := cm.FetchFinnhubInsiderTransactions(context.Background(), "AAPL")
	if err != nil {
		t.Fatalf("FetchFinnhubInsiderTransactions failed: %v", err)
	}
	if len(insiders) != 1 || insiders[0].Name != "Cook Timothy D" || insiders[0].ChangeShares != -50000 {
		t.Errorf("Unexpected insider transaction: %+v", insiders)
	}

	macros, err := cm.FetchFREDSeries(context.Background(), "DGS10", "10-Year Treasury Constant Maturity Rate")
	if err != nil {
		t.Fatalf("FetchFREDSeries failed: %v", err)
	}
	if len(macros) != 1 || macros[0].SeriesID != "DGS10" || macros[0].Value != 4.52 {
		t.Errorf("Unexpected macro indicator: %+v", macros)
	}
}

func TestFetchSECFactsCompression(t *testing.T) {
	mockFacts := SECCompanyFacts{
		CIK:        320193,
		EntityName: "Apple Inc.",
	}
	mockData, err := json.Marshal(mockFacts)
	if err != nil {
		t.Fatalf("Failed to marshal mock data: %v", err)
	}

	tests := []struct {
		name             string
		encoding         string
		compressFunc     func([]byte) []byte
	}{
		{
			name:     "Uncompressed",
			encoding: "",
			compressFunc: func(b []byte) []byte {
				return b
			},
		},
		{
			name:     "Gzip Compressed",
			encoding: "gzip",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w := gzip.NewWriter(&buf)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
		{
			name:     "Auto Decompressed by Transport (Uncompressed true)",
			encoding: "gzip",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w := gzip.NewWriter(&buf)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
		{
			name:     "Gzip Compressed (Uppercase GZIP)",
			encoding: "GZIP",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w := gzip.NewWriter(&buf)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
		{
			name:     "Gzip Compressed (Whitespace and multiple)",
			encoding: " gzip, deflate ",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w := gzip.NewWriter(&buf)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
		{
			name:     "Deflate Compressed (Zlib)",
			encoding: "deflate",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w := zlib.NewWriter(&buf)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
		{
			name:     "Deflate Compressed (Raw Flate)",
			encoding: "deflate",
			compressFunc: func(b []byte) []byte {
				var buf bytes.Buffer
				w, _ := flate.NewWriter(&buf, flate.DefaultCompression)
				w.Write(b)
				w.Close()
				return buf.Bytes()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept-Encoding") != "gzip, deflate" {
					t.Errorf("Expected Accept-Encoding header 'gzip, deflate', got %s", r.Header.Get("Accept-Encoding"))
				}

				if tt.encoding != "" {
					w.Header().Set("Content-Encoding", tt.encoding)
				}
				w.Header().Set("Content-Type", "application/json")

				w.Write(tt.compressFunc(mockData))
			}))
			defer server.Close()

			cm := NewClientManager("TestApp user@test.com", "", "", "", "", "")
			cm.httpClient = server.Client()

			// Replace URL internally for testing by modifying transport or directly doing the request to server.
			cm.httpClient = &http.Client{
				Transport: &mockTripper{
					roundTripFunc: func(req *http.Request) (*http.Response, error) {
						// Forward the request to our local test server
						req.URL.Scheme = "http"
						req.URL.Host = server.Listener.Addr().String()

						res, err := server.Client().Transport.RoundTrip(req)

						// Simulate Uncompressed=true for that specific test case by manually setting it
						// since we intercept the transport round trip
						if tt.name == "Auto Decompressed by Transport (Uncompressed true)" && res != nil {
							// Wait, we need to actually decompress it here to simulate Transport behavior
							if strings.Contains(res.Header.Get("Content-Encoding"), "gzip") {
								r, _ := gzip.NewReader(res.Body)
								res.Body = r // Use decompressed body
								res.Uncompressed = true
							}
						}

						return res, err
					},
				},
			}

			facts, err := cm.FetchSECFacts(context.Background(), "320193")
			if err != nil {
				t.Fatalf("FetchSECFacts failed: %v", err)
			}

			if facts.EntityName != mockFacts.EntityName {
				t.Errorf("Expected entity name %s, got %s", mockFacts.EntityName, facts.EntityName)
			}
			if facts.CIK != mockFacts.CIK {
				t.Errorf("Expected CIK %d, got %d", mockFacts.CIK, facts.CIK)
			}
		})
	}
}

func TestFetchSECFactsIntegrationMSFT(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cm := NewClientManager("FinBaseTestApp user@test.com", "", "", "", "", "")

	facts, err := cm.FetchSECFacts(context.Background(), "0000789019")
	if err != nil {
		t.Fatalf("Failed to fetch MSFT fundamentals: %v", err)
	}

	if facts == nil {
		t.Fatalf("Expected facts to not be nil")
	}

	if facts.EntityName != "MICROSOFT CORPORATION" {
		t.Errorf("Expected entity name 'MICROSOFT CORPORATION', got %q", facts.EntityName)
	}
	if facts.CIK != 789019 {
		t.Errorf("Expected CIK 789019, got %d", facts.CIK)
	}
}
