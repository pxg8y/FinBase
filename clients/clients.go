package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/ratelimit"
	"golang.org/x/time/rate"
)

// Global sync.Pool for bytes.Buffer reuse during JSON parsing
var BufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker struct {
	mu              sync.Mutex
	state           CircuitState
	failureCount    int
	threshold       int
	cooldown        time.Duration
	lastStateChange time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     StateClosed,
		threshold: threshold,
		cooldown:  cooldown,
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	now := time.Now()
	if cb.state == StateOpen {
		if now.Sub(cb.lastStateChange) >= cb.cooldown {
			cb.state = StateHalfOpen
			cb.lastStateChange = now
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker open")
		}
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failureCount++
		if cb.failureCount >= cb.threshold || cb.state == StateHalfOpen {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		}
		return err
	}

	cb.failureCount = 0
	cb.state = StateClosed
	return nil
}

// ClientManager orchestrates rate-limited calls to financial data APIs.
type ClientManager struct {
	httpClient      *http.Client
	openFIGILimiter ratelimit.Limiter // 25 requests per 6 seconds (~4.16 req/sec)
	secLimiter      ratelimit.Limiter // 10 requests per second maximum (leaky bucket)
	finnhubLimiter  *rate.Limiter     // 60 requests per minute (token bucket: 1 req/sec with burst)

	openFIGICB *CircuitBreaker
	secCB      *CircuitBreaker
	finnhubCB  *CircuitBreaker

	secUserAgent   string
	finnhubAPIKey  string
	openFIGIAPIKey string
}

func NewClientManager(secUserAgent, finnhubAPIKey, openFIGIAPIKey string) *ClientManager {
	if secUserAgent == "" {
		secUserAgent = "FinBaseApp user@example.com"
	}
	return &ClientManager{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		// OpenFIGI: 25 req per 6s => ratelimit.New(25, ratelimit.Per(6*time.Second))
		openFIGILimiter: ratelimit.New(25, ratelimit.Per(6*time.Second)),
		// SEC EDGAR: 10 req per second => ratelimit.New(10, ratelimit.Per(1*time.Second))
		secLimiter: ratelimit.New(10, ratelimit.Per(1*time.Second)),
		// Finnhub: 60 req per minute => rate.Every(1*time.Second) with burst capacity (e.g., 5)
		finnhubLimiter: rate.NewLimiter(rate.Every(time.Second), 5),

		openFIGICB: NewCircuitBreaker(5, 30*time.Second),
		secCB:      NewCircuitBreaker(5, 30*time.Second),
		finnhubCB:  NewCircuitBreaker(5, 30*time.Second),

		secUserAgent:   secUserAgent,
		finnhubAPIKey:  finnhubAPIKey,
		openFIGIAPIKey: openFIGIAPIKey,
	}
}

// Structs for OpenFIGI API
type OpenFIGIMappingRequest struct {
	IDType  string `json:"idType"`
	IDValue string `json:"idValue"`
}

type OpenFIGIResult struct {
	Data []struct {
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
	} `json:"data"`
	Error string `json:"error"`
}

func (cm *ClientManager) FetchOpenFIGI(ctx context.Context, ticker string) (*OpenFIGIResult, error) {
	var result *OpenFIGIResult
	err := cm.openFIGICB.Execute(func() error {
		cm.openFIGILimiter.Take()

		reqBody := []OpenFIGIMappingRequest{
			{
				IDType:  "TICKER",
				IDValue: ticker,
			},
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if err := json.NewEncoder(buf).Encode(reqBody); err != nil {
			return fmt.Errorf("openfigi encode error: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openfigi.com/v3/mapping", buf)
		if err != nil {
			return fmt.Errorf("openfigi create request error: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		if cm.openFIGIAPIKey != "" {
			req.Header.Set("X-OPENFIGI-KEY", cm.openFIGIAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("openfigi http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("openfigi HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		var results []OpenFIGIResult
		buf.Reset()
		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("openfigi read body error: %w", err)
		}

		if err := json.NewDecoder(buf).Decode(&results); err != nil {
			return fmt.Errorf("openfigi decode error: %w", err)
		}

		if len(results) == 0 {
			return fmt.Errorf("openfigi returned empty results for ticker %s", ticker)
		}

		result = &results[0]
		return nil
	})
	return result, err
}

// SEC EDGAR API Structs
type SECCompanyFacts struct {
	CIK        int                    `json:"cik"`
	EntityName string                 `json:"entityName"`
	Facts      map[string]interface{} `json:"facts"`
}

func FormatCIK(cik string) string {
	var digits strings.Builder
	for _, r := range cik {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	cikStr := strings.TrimLeft(digits.String(), "0")
	if cikStr == "" {
		return "0000000000"
	}
	if len(cikStr) >= 10 {
		return cikStr[len(cikStr)-10:]
	}
	return fmt.Sprintf("%010s", cikStr)
}

func (cm *ClientManager) FetchSECFacts(ctx context.Context, cik string) (*SECCompanyFacts, error) {
	var facts *SECCompanyFacts
	err := cm.secCB.Execute(func() error {
		cm.secLimiter.Take()

		formattedCIK := FormatCIK(cik)
		url := fmt.Sprintf("https://data.sec.gov/api/xbrl/companyfacts/CIK%s.json", formattedCIK)

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("sec create request error: %w", err)
		}

		req.Header.Set("User-Agent", cm.secUserAgent)
		req.Header.Set("Accept-Encoding", "gzip, deflate")

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("sec http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("sec HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("sec read body error: %w", err)
		}

		var f SECCompanyFacts
		if err := json.NewDecoder(buf).Decode(&f); err != nil {
			return fmt.Errorf("sec decode error: %w", err)
		}

		facts = &f
		return nil
	})
	return facts, err
}

// Finnhub API Structs
type FinnhubProfile struct {
	Country             string  `json:"country"`
	Currency            string  `json:"currency"`
	Exchange            string  `json:"exchange"`
	Name                string  `json:"name"`
	Ticker              string  `json:"ticker"`
	Ipo                 string  `json:"ipo"`
	MarketCapitalization float64 `json:"marketCapitalization"`
	ShareOutstanding    float64 `json:"shareOutstanding"`
	FinnhubIndustry     string  `json:"finnhubIndustry"`
}

type FinnhubQuote struct {
	CurrentPrice       float64 `json:"c"`
	Change             float64 `json:"d"`
	PercentChange      float64 `json:"dp"`
	HighPriceOfDay     float64 `json:"h"`
	LowPriceOfDay      float64 `json:"l"`
	OpenPriceOfDay     float64 `json:"o"`
	PreviousClosePrice float64 `json:"pc"`
	Timestamp          int64   `json:"t"`
}

func (cm *ClientManager) FetchFinnhubQuote(ctx context.Context, ticker string) (*FinnhubQuote, error) {
	var quote *FinnhubQuote
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/quote?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub quote HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub read body error: %w", err)
		}

		var q FinnhubQuote
		if err := json.NewDecoder(buf).Decode(&q); err != nil {
			return fmt.Errorf("finnhub decode error: %w", err)
		}

		quote = &q
		return nil
	})
	return quote, err
}

func (cm *ClientManager) FetchFinnhubProfile(ctx context.Context, ticker string) (*FinnhubProfile, error) {
	var profile *FinnhubProfile
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/profile2?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub profile create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub profile http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub profile HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub profile read body error: %w", err)
		}

		var p FinnhubProfile
		if err := json.NewDecoder(buf).Decode(&p); err != nil {
			return fmt.Errorf("finnhub profile decode error: %w", err)
		}

		profile = &p
		return nil
	})
	return profile, err
}

// CIK Lookup helper from SEC ticker-to-CIK mapping if needed
func (cm *ClientManager) FetchSECCIKForTicker(ctx context.Context, ticker string) (string, error) {
	var cik string
	err := cm.secCB.Execute(func() error {
		cm.secLimiter.Take()

		url := "https://www.sec.gov/files/company_tickers.json"
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", cm.secUserAgent)

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("SEC tickers HTTP status %d", resp.StatusCode)
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return err
		}

		var tickersMap map[string]struct {
			CIK    int    `json:"cik_str"`
			Ticker string `json:"ticker"`
			Title  string `json:"title"`
		}

		if err := json.NewDecoder(buf).Decode(&tickersMap); err != nil {
			return err
		}

		targetTicker := strings.ToUpper(ticker)
		for _, entry := range tickersMap {
			if strings.ToUpper(entry.Ticker) == targetTicker {
				cik = FormatCIK(strconv.Itoa(entry.CIK))
				return nil
			}
		}

		return fmt.Errorf("CIK not found for ticker %s", ticker)
	})
	return cik, err
}
