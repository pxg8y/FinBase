package clients

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/ratelimit"
	"golang.org/x/time/rate"
)

var paramRedactRegex = regexp.MustCompile(`(?i)(apikey|api_key|token)=([^&\s]+)`)

func RedactString(s string, keys ...string) string {
	if s == "" {
		return ""
	}
	for _, key := range keys {
		if key != "" && len(key) >= 2 {
			s = strings.ReplaceAll(s, key, "[REDACTED]")
		}
	}
	s = paramRedactRegex.ReplaceAllString(s, "$1=[REDACTED]")
	return s
}

func RedactError(err error, keys ...string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s", RedactString(err.Error(), keys...))
}

func (cm *ClientManager) redact(err error) error {
	if err == nil {
		return nil
	}
	return RedactError(err, cm.finnhubAPIKey, cm.openFIGIAPIKey, cm.tiingoAPIKey, cm.twelveDataAPIKey, cm.fmpAPIKey, cm.fredAPIKey)
}

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

func (cb *CircuitBreaker) Status() (string, int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	stateStr := "closed"
	switch cb.state {
	case StateOpen:
		stateStr = "open"
	case StateHalfOpen:
		stateStr = "half-open"
	}
	return stateStr, cb.failureCount
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

// MarketQuote represents standard market quote output across all fetchers.
type MarketQuote struct {
	CurrentPrice     float64
	OpenPrice        float64
	HighPrice        float64
	LowPrice         float64
	PreviousClose    float64
	Volume           int64
	FiftyTwoWeekHigh float64
	FiftyTwoWeekLow  float64
	Source           string
}

// MarketDataFetcher is the common interface implemented by all market data fetchers.
type MarketDataFetcher interface {
	FetchMarketData(ctx context.Context, ticker string) (*MarketQuote, error)
}

// ClientManager orchestrates rate-limited calls to financial data APIs.
type ClientManager struct {
	httpClient        *http.Client
	openFIGILimiter   ratelimit.Limiter // 25 requests per 6 seconds (~4.16 req/sec)
	secLimiter        ratelimit.Limiter // 10 requests per second maximum (leaky bucket)
	finnhubLimiter    *rate.Limiter     // 60 requests per minute (token bucket: 1 req/sec with burst)
	tiingoLimiter     *rate.Limiter     // 500 requests per hour (~1 req/7s, with burst capacity)
	twelveDataLimiter ratelimit.Limiter // 8 requests per minute
	fmpLimiter        *rate.Limiter     // 250 requests per day (~1 req/350s, with burst capacity)
	fredLimiter       *rate.Limiter     // 120 requests per minute (~2 req/sec with burst)

	openFIGICB    *CircuitBreaker
	secCB         *CircuitBreaker
	finnhubCB     *CircuitBreaker
	tiingoCB      *CircuitBreaker
	twelveDataCB  *CircuitBreaker
	fmpCB         *CircuitBreaker
	fredCB        *CircuitBreaker

	keysMu           sync.RWMutex
	secUserAgent     string
	finnhubAPIKey    string
	openFIGIAPIKey   string
	tiingoAPIKey     string
	twelveDataAPIKey string
	fmpAPIKey        string
	fredAPIKey       string
}

func NewClientManager(secUserAgent, finnhubAPIKey, openFIGIAPIKey, tiingoAPIKey, twelveDataAPIKey, fmpAPIKey string) *ClientManager {
	if secUserAgent == "" {
		secUserAgent = "FinBaseApp user@example.com"
	}
	return &ClientManager{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		openFIGILimiter:   ratelimit.New(25, ratelimit.Per(6*time.Second)),
		secLimiter:        ratelimit.New(10, ratelimit.Per(1*time.Second)),
		finnhubLimiter:    rate.NewLimiter(rate.Every(time.Second), 5),
		tiingoLimiter:     rate.NewLimiter(rate.Every(100*time.Millisecond), 5),
		twelveDataLimiter: ratelimit.New(8, ratelimit.Per(60*time.Second)),
		fmpLimiter:        rate.NewLimiter(rate.Every(100*time.Millisecond), 5),
		fredLimiter:       rate.NewLimiter(rate.Every(500*time.Millisecond), 5),

		openFIGICB:   NewCircuitBreaker(5, 30*time.Second),
		secCB:        NewCircuitBreaker(5, 30*time.Second),
		finnhubCB:    NewCircuitBreaker(5, 30*time.Second),
		tiingoCB:     NewCircuitBreaker(5, 30*time.Second),
		twelveDataCB: NewCircuitBreaker(5, 30*time.Second),
		fmpCB:        NewCircuitBreaker(5, 30*time.Second),
		fredCB:       NewCircuitBreaker(5, 30*time.Second),

		secUserAgent:     secUserAgent,
		finnhubAPIKey:    finnhubAPIKey,
		openFIGIAPIKey:   openFIGIAPIKey,
		tiingoAPIKey:     tiingoAPIKey,
		twelveDataAPIKey: twelveDataAPIKey,
		fmpAPIKey:        fmpAPIKey,
	}
}

func (cm *ClientManager) SetSECUserAgent(ua string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.secUserAgent = ua
}

func (cm *ClientManager) SetFinnhubAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.finnhubAPIKey = key
}

func (cm *ClientManager) SetOpenFIGIAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.openFIGIAPIKey = key
}

func (cm *ClientManager) SetTiingoAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.tiingoAPIKey = key
}

func (cm *ClientManager) SetTwelveDataAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.twelveDataAPIKey = key
}

func (cm *ClientManager) SetFMPAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.fmpAPIKey = key
}

func (cm *ClientManager) SetFREDAPIKey(key string) {
	cm.keysMu.Lock()
	defer cm.keysMu.Unlock()
	cm.fredAPIKey = key
}

func (cm *ClientManager) GetSECUserAgent() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.secUserAgent
}

func (cm *ClientManager) GetFinnhubAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.finnhubAPIKey
}

func (cm *ClientManager) GetOpenFIGIAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.openFIGIAPIKey
}

func (cm *ClientManager) GetTiingoAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.tiingoAPIKey
}

func (cm *ClientManager) GetTwelveDataAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.twelveDataAPIKey
}

func (cm *ClientManager) GetFMPAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.fmpAPIKey
}

func (cm *ClientManager) GetFREDAPIKey() string {
	cm.keysMu.RLock()
	defer cm.keysMu.RUnlock()
	return cm.fredAPIKey
}

type APIProviderStatus struct {
	Name          string `json:"name"`
	KeyConfigured bool   `json:"key_configured"`
	CircuitState  string `json:"circuit_state"`
	FailureCount  int    `json:"failure_count"`
}

func (cm *ClientManager) GetProviderStatuses() []APIProviderStatus {
	cm.keysMu.RLock()
	secUA := cm.secUserAgent
	finnhubKey := cm.finnhubAPIKey
	figiKey := cm.openFIGIAPIKey
	tiingoKey := cm.tiingoAPIKey
	twelveKey := cm.twelveDataAPIKey
	fmpKey := cm.fmpAPIKey
	fredKey := cm.fredAPIKey
	cm.keysMu.RUnlock()

	providers := []struct {
		name          string
		keyConfigured bool
		cb            *CircuitBreaker
	}{
		{"SEC EDGAR", secUA != "", cm.secCB},
		{"Finnhub", finnhubKey != "", cm.finnhubCB},
		{"OpenFIGI", figiKey != "", cm.openFIGICB},
		{"Tiingo", tiingoKey != "", cm.tiingoCB},
		{"Twelve Data", twelveKey != "", cm.twelveDataCB},
		{"Financial Modeling Prep (FMP)", fmpKey != "", cm.fmpCB},
		{"FRED Macro", fredKey != "", cm.fredCB},
	}

	statuses := make([]APIProviderStatus, 0, len(providers))
	for _, p := range providers {
		stateStr, failures := p.cb.Status()
		statuses = append(statuses, APIProviderStatus{
			Name:          p.name,
			KeyConfigured: p.keyConfigured,
			CircuitState:  stateStr,
			FailureCount:  failures,
		})
	}
	return statuses
}

func (cm *ClientManager) TestKeyFunctionality(ctx context.Context, serviceName string) (bool, string) {
	switch strings.ToLower(serviceName) {
	case "sec_user_agent", "sec", "sec edgar":
		ua := cm.GetSECUserAgent()
		if ua == "" {
			return false, "User-Agent is not set"
		}
		_, err := cm.FetchSECFacts(ctx, "0000320193")
		if err != nil {
			return false, err.Error()
		}
		return true, "SEC EDGAR API is functional"

	case "finnhub_api_key", "finnhub":
		if cm.GetFinnhubAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchFinnhubQuote(ctx, "AAPL")
		if err != nil {
			return false, err.Error()
		}
		return true, "Finnhub API is functional"

	case "openfigi_api_key", "openfigi":
		if cm.GetOpenFIGIAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchOpenFIGI(ctx, "AAPL")
		if err != nil {
			return false, err.Error()
		}
		return true, "OpenFIGI API is functional"

	case "tiingo_api_key", "tiingo":
		if cm.GetTiingoAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchTiingoMarketData(ctx, "AAPL")
		if err != nil {
			return false, err.Error()
		}
		return true, "Tiingo API is functional"

	case "twelve_data_api_key", "twelvedata", "twelve data":
		if cm.GetTwelveDataAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchTwelveDataMarketData(ctx, "AAPL")
		if err != nil {
			return false, err.Error()
		}
		return true, "Twelve Data API is functional"

	case "fmp_api_key", "fmp", "financial modeling prep":
		if cm.GetFMPAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchFMPIncomeStatement(ctx, "AAPL")
		if err != nil {
			return false, err.Error()
		}
		return true, "FMP API is functional"

	case "fred_api_key", "fred", "fred macro":
		if cm.GetFREDAPIKey() == "" {
			return false, "API key is not set"
		}
		_, err := cm.FetchFREDSeries(ctx, "DGS10", "10-Year Treasury")
		if err != nil {
			return false, err.Error()
		}
		return true, "FRED API is functional"

	default:
		return false, fmt.Sprintf("Unknown service: %s", serviceName)
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
	return result, cm.redact(err)
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
		// We explicitly set this so our default Transport doesn't get confused,
		// but we must robustly handle Uncompressed and various casings of Content-Encoding.
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

		ce := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))

		switch {
		case !resp.Uncompressed && strings.Contains(ce, "gzip"):
			reader, err := gzip.NewReader(resp.Body)
			if err != nil {
				return fmt.Errorf("sec gzip decode error: %w", err)
			}
			defer reader.Close()
			if _, err := buf.ReadFrom(reader); err != nil {
				return fmt.Errorf("sec read body error: %w", err)
			}
		case !resp.Uncompressed && strings.Contains(ce, "deflate"):
			// Read the entire body first to handle zlib vs raw deflate ambiguity
			rawBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("sec read body error: %w", err)
			}

			// Try zlib first
			zlibReader, zlibErr := zlib.NewReader(bytes.NewReader(rawBody))
			if zlibErr == nil {
				if _, err := buf.ReadFrom(zlibReader); err == nil {
					zlibReader.Close()
					break // Success
				}
				zlibReader.Close()
			}

			// Fallback to raw flate
			buf.Reset()
			flateReader := flate.NewReader(bytes.NewReader(rawBody))
			if _, err := buf.ReadFrom(flateReader); err != nil {
				flateReader.Close()
				return fmt.Errorf("sec deflate decode error: %w", err)
			}
			flateReader.Close()
		default:
			if _, err := buf.ReadFrom(resp.Body); err != nil {
				return fmt.Errorf("sec read body error: %w", err)
			}
		}

		var f SECCompanyFacts
		if err := json.NewDecoder(buf).Decode(&f); err != nil {
			return fmt.Errorf("sec decode error: %w", err)
		}

		facts = &f
		return nil
	})
	return facts, cm.redact(err)
}

// Finnhub API Structs
type FinnhubProfile struct {
	Country              string  `json:"country"`
	Currency             string  `json:"currency"`
	Exchange             string  `json:"exchange"`
	Name                 string  `json:"name"`
	Ticker               string  `json:"ticker"`
	Ipo                  string  `json:"ipo"`
	MarketCapitalization float64 `json:"marketCapitalization"`
	ShareOutstanding     float64 `json:"shareOutstanding"`
	FinnhubIndustry      string  `json:"finnhubIndustry"`
	Logo                 string  `json:"logo"`
}

type DividendItem struct {
	ExDate      string  `json:"ex_date"`
	PaymentDate string  `json:"payment_date"`
	RecordDate  string  `json:"record_date"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Frequency   int     `json:"frequency"`
}

type StockSplitItem struct {
	ExecutionDate string  `json:"execution_date"`
	FromFactor    float64 `json:"from_factor"`
	ToFactor      float64 `json:"to_factor"`
}

type HistoricalPriceItem struct {
	Date          string  `json:"date"`
	OpenPrice     float64 `json:"open_price"`
	HighPrice     float64 `json:"high_price"`
	LowPrice      float64 `json:"low_price"`
	ClosePrice    float64 `json:"close_price"`
	AdjClosePrice float64 `json:"adj_close_price"`
	Volume        int64   `json:"volume"`
}

type AnalystEstimateItem struct {
	Period     string `json:"period"`
	StrongBuy  int    `json:"strong_buy"`
	Buy        int    `json:"buy"`
	Hold       int    `json:"hold"`
	Sell       int    `json:"sell"`
	StrongSell int    `json:"strong_sell"`
}

type EarningsCalendarItem struct {
	Date            string  `json:"date"`
	Quarter         int     `json:"quarter"`
	Year            int     `json:"year"`
	EPSEstimate     float64 `json:"eps_estimate"`
	EPSActual       float64 `json:"eps_actual"`
	RevenueEstimate float64 `json:"revenue_estimate"`
	RevenueActual   float64 `json:"revenue_actual"`
}

type CompanyNewsItem struct {
	NewsID         int64     `json:"news_id"`
	Headline       string    `json:"headline"`
	Summary        string    `json:"summary"`
	Source         string    `json:"source"`
	URL            string    `json:"url"`
	PublishedAt    time.Time `json:"published_at"`
	SentimentScore float64   `json:"sentiment_score"`
}

type InsiderTransactionItem struct {
	Name             string  `json:"name"`
	ShareCount       float64 `json:"share_count"`
	ChangeShares     float64 `json:"change_shares"`
	FilingDate       string  `json:"filing_date"`
	TransactionCode  string  `json:"transaction_code"`
	TransactionPrice float64 `json:"transaction_price"`
}

type InstitutionalOwnershipItem struct {
	InvestorName string  `json:"investor_name"`
	SharesHeld   float64 `json:"shares_held"`
	ChangeShares float64 `json:"change_shares"`
	Value        float64 `json:"value"`
	Period       string  `json:"period"`
}

type MacroIndicatorItem struct {
	SeriesID      string  `json:"series_id"`
	IndicatorName string  `json:"indicator_name"`
	Date          string  `json:"date"`
	Value         float64 `json:"value"`
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

type FinnhubValuationRatios struct {
	PERatio         float64
	PBRatio         float64
	PSRatio         float64
	GrossMargin     float64
	OperatingMargin float64
	NetMargin       float64
	ROE             float64
	ROA             float64
	DebtToEquity    float64
}

func (cm *ClientManager) FetchFinnhubValuationRatios(ctx context.Context, ticker string) (*FinnhubValuationRatios, error) {
	var ratios *FinnhubValuationRatios
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/metric?symbol=%s&metric=all", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub metric create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub metric http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub metric HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub metric read body error: %w", err)
		}

		var payload struct {
			Metric map[string]interface{} `json:"metric"`
		}
		if err := json.NewDecoder(buf).Decode(&payload); err != nil {
			return fmt.Errorf("finnhub metric decode error: %w", err)
		}

		getFloat := func(m map[string]interface{}, keys ...string) float64 {
			for _, k := range keys {
				if val, ok := m[k]; ok && val != nil {
					if f, ok := val.(float64); ok {
						return f
					}
				}
			}
			return 0
		}

		m := payload.Metric
		ratios = &FinnhubValuationRatios{
			PERatio:         getFloat(m, "peNormalizedAnnual", "peBasicExclExtraTTM", "peTTM"),
			PBRatio:         getFloat(m, "pbAnnual", "pbQuarterly"),
			PSRatio:         getFloat(m, "psAnnual", "psTTM"),
			GrossMargin:     getFloat(m, "grossMarginAnnual", "grossMarginTTM"),
			OperatingMargin: getFloat(m, "operatingMarginAnnual", "operatingMarginTTM"),
			NetMargin:       getFloat(m, "netProfitMarginAnnual", "netProfitMarginTTM"),
			ROE:             getFloat(m, "roeTTM", "roeRpt"),
			ROA:             getFloat(m, "roaTTM", "roaRpt"),
			DebtToEquity:    getFloat(m, "totalDebt/totalEquityAnnual", "totalDebt/totalEquityQuarterly"),
		}
		return nil
	})
	return ratios, cm.redact(err)
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
	return quote, cm.redact(err)
}

// FetchFinnhubMarketData wraps FetchFinnhubQuote to implement MarketDataFetcher.
func (cm *ClientManager) FetchFinnhubMarketData(ctx context.Context, ticker string) (*MarketQuote, error) {
	q, err := cm.FetchFinnhubQuote(ctx, ticker)
	if err != nil {
		return nil, cm.redact(err)
	}
	if q == nil || q.CurrentPrice == 0 {
		return nil, cm.redact(fmt.Errorf("finnhub returned empty market data for ticker %s", ticker))
	}
	return &MarketQuote{
		CurrentPrice:  q.CurrentPrice,
		OpenPrice:     q.OpenPriceOfDay,
		HighPrice:     q.HighPriceOfDay,
		LowPrice:      q.LowPriceOfDay,
		PreviousClose: q.PreviousClosePrice,
		Source:        "Finnhub",
	}, nil
}

// Tiingo API Structs
type TiingoPrice struct {
	Date     string  `json:"date"`
	Close    float64 `json:"close"`
	High     float64 `json:"high"`
	Low      float64 `json:"low"`
	Open     float64 `json:"open"`
	Volume   int64   `json:"volume"`
	AdjClose float64 `json:"adjClose"`
}

func (cm *ClientManager) FetchFinnhubDividends(ctx context.Context, ticker string) ([]DividendItem, error) {
	var divs []DividendItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/dividend?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub dividend create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub dividend http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub dividend HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub dividend read body error: %w", err)
		}

		var rawList []struct {
			Date       string  `json:"date"`
			PayDate    string  `json:"payDate"`
			RecordDate string  `json:"recordDate"`
			Amount     float64 `json:"amount"`
			Currency   string  `json:"currency"`
			Frequency  int     `json:"freq"`
		}

		if err := json.NewDecoder(buf).Decode(&rawList); err != nil {
			return fmt.Errorf("finnhub dividend decode error: %w", err)
		}

		for _, item := range rawList {
			if item.Amount > 0 {
				curr := item.Currency
				if curr == "" {
					curr = "USD"
				}
				divs = append(divs, DividendItem{
					ExDate:      item.Date,
					PaymentDate: item.PayDate,
					RecordDate:  item.RecordDate,
					Amount:      item.Amount,
					Currency:    curr,
					Frequency:   item.Frequency,
				})
			}
		}
		return nil
	})
	return divs, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubAnalystEstimates(ctx context.Context, ticker string) ([]AnalystEstimateItem, error) {
	var estimates []AnalystEstimateItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/recommendation?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub recommendation create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub recommendation http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub recommendation HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub recommendation read body error: %w", err)
		}

		var rawList []struct {
			Period     string `json:"period"`
			StrongBuy  int    `json:"strongBuy"`
			Buy        int    `json:"buy"`
			Hold       int    `json:"hold"`
			Sell       int    `json:"sell"`
			StrongSell int    `json:"strongSell"`
		}

		if err := json.NewDecoder(buf).Decode(&rawList); err != nil {
			return fmt.Errorf("finnhub recommendation decode error: %w", err)
		}

		for _, item := range rawList {
			estimates = append(estimates, AnalystEstimateItem{
				Period:     item.Period,
				StrongBuy:  item.StrongBuy,
				Buy:        item.Buy,
				Hold:       item.Hold,
				Sell:       item.Sell,
				StrongSell: item.StrongSell,
			})
		}
		return nil
	})
	return estimates, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubEarningsCalendar(ctx context.Context, ticker string) ([]EarningsCalendarItem, error) {
	var events []EarningsCalendarItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/calendar/earnings?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub earnings create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub earnings http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub earnings HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub earnings read body error: %w", err)
		}

		var payload struct {
			EarningsCalendar []struct {
				Date            string  `json:"date"`
				Quarter         int     `json:"quarter"`
				Year            int     `json:"year"`
				EPSEstimate     float64 `json:"epsEstimate"`
				EPSActual       float64 `json:"epsActual"`
				RevenueEstimate float64 `json:"revenueEstimate"`
				RevenueActual   float64 `json:"revenueActual"`
			} `json:"earningsCalendar"`
		}

		if err := json.NewDecoder(buf).Decode(&payload); err != nil {
			return fmt.Errorf("finnhub earnings decode error: %w", err)
		}

		for _, item := range payload.EarningsCalendar {
			events = append(events, EarningsCalendarItem{
				Date:            item.Date,
				Quarter:         item.Quarter,
				Year:            item.Year,
				EPSEstimate:     item.EPSEstimate,
				EPSActual:       item.EPSActual,
				RevenueEstimate: item.RevenueEstimate,
				RevenueActual:   item.RevenueActual,
			})
		}
		return nil
	})
	return events, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubInstitutionalOwnership(ctx context.Context, ticker string) ([]InstitutionalOwnershipItem, error) {
	var holdings []InstitutionalOwnershipItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/ownership?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub ownership create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub ownership http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub ownership HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub ownership read body error: %w", err)
		}

		var payload struct {
			Ownership []struct {
				Name   string  `json:"name"`
				Share  float64 `json:"share"`
				Change float64 `json:"change"`
				Value  float64 `json:"value"`
				Period string  `json:"period"`
			} `json:"ownership"`
		}

		if err := json.NewDecoder(buf).Decode(&payload); err != nil {
			return fmt.Errorf("finnhub ownership decode error: %w", err)
		}

		for _, item := range payload.Ownership {
			holdings = append(holdings, InstitutionalOwnershipItem{
				InvestorName: item.Name,
				SharesHeld:   item.Share,
				ChangeShares: item.Change,
				Value:        item.Value,
				Period:       item.Period,
			})
		}
		return nil
	})
	return holdings, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubInsiderTransactions(ctx context.Context, ticker string) ([]InsiderTransactionItem, error) {
	var txs []InsiderTransactionItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/insider-transactions?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub insider create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub insider http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub insider HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub insider read body error: %w", err)
		}

		var payload struct {
			Data []struct {
				Name            string  `json:"name"`
				Share           float64 `json:"share"`
				Change          float64 `json:"change"`
				FilingDate      string  `json:"filingDate"`
				TransactionCode string  `json:"transactionCode"`
				TransactionPrice float64 `json:"transactionPrice"`
			} `json:"data"`
		}

		if err := json.NewDecoder(buf).Decode(&payload); err != nil {
			return fmt.Errorf("finnhub insider decode error: %w", err)
		}

		for _, item := range payload.Data {
			txs = append(txs, InsiderTransactionItem{
				Name:             item.Name,
				ShareCount:       item.Share,
				ChangeShares:     item.Change,
				FilingDate:       item.FilingDate,
				TransactionCode:  item.TransactionCode,
				TransactionPrice: item.TransactionPrice,
			})
		}
		return nil
	})
	return txs, cm.redact(err)
}

func (cm *ClientManager) FetchFREDSeries(ctx context.Context, seriesID, indicatorName string) ([]MacroIndicatorItem, error) {
	var indicators []MacroIndicatorItem
	err := cm.fredCB.Execute(func() error {
		if err := cm.fredLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("fred rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://api.stlouisfed.org/fred/series/observations?series_id=%s&file_type=json", seriesID)
		if cm.fredAPIKey != "" {
			url = fmt.Sprintf("%s&api_key=%s", url, cm.fredAPIKey)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("fred create request error: %w", err)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("fred http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("fred HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("fred read body error: %w", err)
		}

		var payload struct {
			Observations []struct {
				Date  string `json:"date"`
				Value string `json:"value"`
			} `json:"observations"`
		}

		if err := json.NewDecoder(buf).Decode(&payload); err != nil {
			return fmt.Errorf("fred decode error: %w", err)
		}

		for _, obs := range payload.Observations {
			val := parseStringToFloat(obs.Value)
			if obs.Date != "" && obs.Value != "." {
				indicators = append(indicators, MacroIndicatorItem{
					SeriesID:      seriesID,
					IndicatorName: indicatorName,
					Date:          obs.Date,
					Value:         val,
				})
			}
		}
		return nil
	})
	return indicators, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubCompanyNews(ctx context.Context, ticker, from, to string) ([]CompanyNewsItem, error) {
	var newsList []CompanyNewsItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/company-news?symbol=%s&from=%s&to=%s", ticker, from, to)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub news create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub news http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub news HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub news read body error: %w", err)
		}

		var rawList []struct {
			ID        int64  `json:"id"`
			Headline  string `json:"headline"`
			Summary   string `json:"summary"`
			Source    string `json:"source"`
			URL       string `json:"url"`
			Datetime  int64  `json:"datetime"`
			Sentiment struct {
				Score float64 `json:"score"`
			} `json:"sentiment"`
		}

		if err := json.NewDecoder(buf).Decode(&rawList); err != nil {
			return fmt.Errorf("finnhub news decode error: %w", err)
		}

		for _, item := range rawList {
			pubTime := time.Unix(item.Datetime, 0)
			newsList = append(newsList, CompanyNewsItem{
				NewsID:         item.ID,
				Headline:       item.Headline,
				Summary:        item.Summary,
				Source:         item.Source,
				URL:            item.URL,
				PublishedAt:    pubTime,
				SentimentScore: item.Sentiment.Score,
			})
		}
		return nil
	})
	return newsList, cm.redact(err)
}

func (cm *ClientManager) FetchFinnhubSplits(ctx context.Context, ticker string) ([]StockSplitItem, error) {
	var splits []StockSplitItem
	err := cm.finnhubCB.Execute(func() error {
		if err := cm.finnhubLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("finnhub rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://finnhub.io/api/v1/stock/split?symbol=%s", ticker)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("finnhub split create request error: %w", err)
		}

		if cm.finnhubAPIKey != "" {
			req.Header.Set("X-Finnhub-Token", cm.finnhubAPIKey)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("finnhub split http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("finnhub split HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("finnhub split read body error: %w", err)
		}

		var rawList []struct {
			Date       string  `json:"date"`
			FromFactor float64 `json:"fromFactor"`
			ToFactor   float64 `json:"toFactor"`
		}

		if err := json.NewDecoder(buf).Decode(&rawList); err != nil {
			return fmt.Errorf("finnhub split decode error: %w", err)
		}

		for _, item := range rawList {
			splits = append(splits, StockSplitItem{
				ExecutionDate: item.Date,
				FromFactor:    item.FromFactor,
				ToFactor:      item.ToFactor,
			})
		}
		return nil
	})
	return splits, cm.redact(err)
}

func (cm *ClientManager) FetchTiingoHistoricalPrices(ctx context.Context, ticker string, startDate, endDate string) ([]HistoricalPriceItem, error) {
	var prices []HistoricalPriceItem
	err := cm.tiingoCB.Execute(func() error {
		if err := cm.tiingoLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("tiingo rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices", ticker)
		params := []string{}
		if startDate != "" {
			params = append(params, "startDate="+startDate)
		}
		if endDate != "" {
			params = append(params, "endDate="+endDate)
		}
		if cm.tiingoAPIKey != "" {
			params = append(params, "token="+cm.tiingoAPIKey)
		}
		if len(params) > 0 {
			url = url + "?" + strings.Join(params, "&")
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("tiingo historical create request error: %w", err)
		}

		if cm.tiingoAPIKey != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Token %s", cm.tiingoAPIKey))
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("tiingo historical http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("tiingo historical HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("tiingo historical read body error: %w", err)
		}

		var rawList []struct {
			Date     string  `json:"date"`
			Close    float64 `json:"close"`
			High     float64 `json:"high"`
			Low      float64 `json:"low"`
			Open     float64 `json:"open"`
			Volume   int64   `json:"volume"`
			AdjClose float64 `json:"adjClose"`
		}

		if err := json.NewDecoder(buf).Decode(&rawList); err != nil {
			return fmt.Errorf("tiingo historical decode error: %w", err)
		}

		for _, item := range rawList {
			dateStr := item.Date
			if len(dateStr) >= 10 {
				dateStr = dateStr[:10]
			}
			prices = append(prices, HistoricalPriceItem{
				Date:          dateStr,
				OpenPrice:     item.Open,
				HighPrice:     item.High,
				LowPrice:      item.Low,
				ClosePrice:    item.Close,
				AdjClosePrice: item.AdjClose,
				Volume:        item.Volume,
			})
		}
		return nil
	})
	return prices, cm.redact(err)
}

func (cm *ClientManager) FetchTiingoMarketData(ctx context.Context, ticker string) (*MarketQuote, error) {
	var quote *MarketQuote
	err := cm.tiingoCB.Execute(func() error {
		if err := cm.tiingoLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("tiingo rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://api.tiingo.com/tiingo/daily/%s/prices", ticker)
		if cm.tiingoAPIKey != "" {
			url = fmt.Sprintf("%s?token=%s", url, cm.tiingoAPIKey)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("tiingo create request error: %w", err)
		}

		if cm.tiingoAPIKey != "" {
			req.Header.Set("Authorization", fmt.Sprintf("Token %s", cm.tiingoAPIKey))
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("tiingo http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("tiingo HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("tiingo read body error: %w", err)
		}

		var prices []TiingoPrice
		if err := json.NewDecoder(buf).Decode(&prices); err != nil {
			return fmt.Errorf("tiingo decode error: %w", err)
		}

		if len(prices) == 0 {
			return fmt.Errorf("tiingo returned empty prices for ticker %s", ticker)
		}

		latest := prices[0]
		quote = &MarketQuote{
			CurrentPrice: latest.Close,
			OpenPrice:    latest.Open,
			HighPrice:    latest.High,
			LowPrice:     latest.Low,
			Volume:       latest.Volume,
			Source:       "Tiingo",
		}
		return nil
	})
	return quote, cm.redact(err)
}

// Twelve Data API Structs
type TwelveDataQuote struct {
	Symbol        string `json:"symbol"`
	Name          string `json:"name"`
	Exchange      string `json:"exchange"`
	Open          string `json:"open"`
	High          string `json:"high"`
	Low           string `json:"low"`
	Close         string `json:"close"`
	Volume        string `json:"volume"`
	PreviousClose string `json:"previous_close"`
	FiftyTwoWeek  struct {
		Low  string `json:"low"`
		High string `json:"high"`
	} `json:"fifty_two_week"`
	Status string `json:"status"`
}

func parseStringToFloat(s string) float64 {
	val, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return val
}

func parseStringToInt64(s string) int64 {
	val, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return val
}

func (cm *ClientManager) FetchTwelveDataMarketData(ctx context.Context, ticker string) (*MarketQuote, error) {
	var quote *MarketQuote
	err := cm.twelveDataCB.Execute(func() error {
		cm.twelveDataLimiter.Take()

		url := fmt.Sprintf("https://api.twelvedata.com/quote?symbol=%s", ticker)
		if cm.twelveDataAPIKey != "" {
			url = fmt.Sprintf("%s&apikey=%s", url, cm.twelveDataAPIKey)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("twelve data create request error: %w", err)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("twelve data http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("twelve data HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("twelve data read body error: %w", err)
		}

		var q TwelveDataQuote
		if err := json.NewDecoder(buf).Decode(&q); err != nil {
			return fmt.Errorf("twelve data decode error: %w", err)
		}

		if q.Status == "error" || (q.Close == "" && q.Open == "") {
			return fmt.Errorf("twelve data returned invalid quote for ticker %s", ticker)
		}

		quote = &MarketQuote{
			CurrentPrice:     parseStringToFloat(q.Close),
			OpenPrice:        parseStringToFloat(q.Open),
			HighPrice:        parseStringToFloat(q.High),
			LowPrice:         parseStringToFloat(q.Low),
			PreviousClose:    parseStringToFloat(q.PreviousClose),
			Volume:           parseStringToInt64(q.Volume),
			FiftyTwoWeekHigh: parseStringToFloat(q.FiftyTwoWeek.High),
			FiftyTwoWeekLow:  parseStringToFloat(q.FiftyTwoWeek.Low),
			Source:           "Twelve Data",
		}
		return nil
	})
	return quote, err
}

// FetchMarketDataWaterfall executes the fallback routing logic: Finnhub -> Tiingo -> Twelve Data
func (cm *ClientManager) FetchMarketDataWaterfall(ctx context.Context, ticker string) (*MarketQuote, error) {
	var errs []string

	// 1st Priority: Finnhub
	quote, err := cm.FetchFinnhubMarketData(ctx, ticker)
	if err == nil && quote != nil && quote.CurrentPrice > 0 {
		return quote, nil
	}
	if err != nil {
		errs = append(errs, fmt.Sprintf("Finnhub: %v", err))
	} else {
		errs = append(errs, "Finnhub: zero current price")
	}

	// 2nd Priority: Tiingo API
	quote, err = cm.FetchTiingoMarketData(ctx, ticker)
	if err == nil && quote != nil && quote.CurrentPrice > 0 {
		return quote, nil
	}
	if err != nil {
		errs = append(errs, fmt.Sprintf("Tiingo: %v", err))
	} else {
		errs = append(errs, "Tiingo: zero current price")
	}

	// 3rd Priority: Twelve Data API
	quote, err = cm.FetchTwelveDataMarketData(ctx, ticker)
	if err == nil && quote != nil && quote.CurrentPrice > 0 {
		return quote, nil
	}
	if err != nil {
		errs = append(errs, fmt.Sprintf("Twelve Data: %v", err))
	} else {
		errs = append(errs, "Twelve Data: zero current price")
	}

	return nil, cm.redact(fmt.Errorf("all market data fetchers failed for %s: [%s]", ticker, strings.Join(errs, "; ")))
}

// FMP API Structs
type FMPIncomeStatement struct {
	Date            string  `json:"date"`
	Symbol          string  `json:"symbol"`
	CalendarYear    string  `json:"calendarYear"`
	Period          string  `json:"period"`
	Revenue         float64 `json:"revenue"`
	GrossProfit     float64 `json:"grossProfit"`
	OperatingIncome float64 `json:"operatingIncome"`
	NetIncome       float64 `json:"netIncome"`
	Eps             float64 `json:"eps"`
	EpsDiluted      float64 `json:"epsdiluted"`
}

func (cm *ClientManager) FetchFMPIncomeStatement(ctx context.Context, ticker string) ([]FMPIncomeStatement, error) {
	var stmts []FMPIncomeStatement
	err := cm.fmpCB.Execute(func() error {
		if err := cm.fmpLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("fmp rate limit wait error: %w", err)
		}

		url := fmt.Sprintf("https://financialmodelingprep.com/api/v3/income-statement/%s", ticker)
		if cm.fmpAPIKey != "" {
			url = fmt.Sprintf("%s?apikey=%s", url, cm.fmpAPIKey)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("fmp create request error: %w", err)
		}

		resp, err := cm.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("fmp http error: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("fmp HTTP status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		buf := BufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer BufferPool.Put(buf)

		if _, err := buf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("fmp read body error: %w", err)
		}

		if err := json.NewDecoder(buf).Decode(&stmts); err != nil {
			return fmt.Errorf("fmp decode error: %w", err)
		}

		return nil
	})
	return stmts, cm.redact(err)
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
	return profile, cm.redact(err)
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
	return cik, cm.redact(err)
}
