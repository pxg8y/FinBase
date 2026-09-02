package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// DB wraps the read and write connection pools for SQLite.
type DB struct {
	ReadDB  *sql.DB
	WriteDB *sql.DB
}

// Models
type Watchitem struct {
	ID                int64     `json:"id"`
	Ticker            string    `json:"ticker"`
	Priority          int       `json:"priority"`
	Status            string    `json:"status"`
	LastUpdated       time.Time `json:"last_updated"`
	NextUpdateTime    time.Time `json:"next_update_time"`
	ForceFullRefresh bool      `json:"force_full_refresh"`
}

type JobStatusSummary struct {
	Pending    int `json:"pending"`
	Queued     int `json:"queued"`
	Processing int `json:"processing"`
	Completed  int `json:"completed"`
	Error      int `json:"error"`
	Total      int `json:"total"`
}

type Company struct {
	ID                int64   `json:"id"`
	Ticker            string  `json:"ticker"`
	CIK               string  `json:"cik"`
	ISIN              string  `json:"isin"`
	Name              string  `json:"name"`
	Sector            string  `json:"sector"`
	Exchange          string  `json:"exchange"`
	OutstandingShares float64 `json:"outstanding_shares"`
	LogoURL           string  `json:"logo_url"`
}

type MarketData struct {
	ID                int64     `json:"id"`
	CompanyID         int64     `json:"company_id"`
	Timestamp         time.Time `json:"timestamp"`
	CurrentPrice      float64   `json:"current_price"`
	Volume            int64     `json:"volume"`
	OpenPrice         float64   `json:"open_price"`
	HighPrice         float64   `json:"high_price"`
	LowPrice          float64   `json:"low_price"`
	PreviousClose     float64   `json:"previous_close"`
	MarketCap         float64   `json:"market_cap"`
	FiftyTwoWeekHigh  float64   `json:"fifty_two_week_high"`
	FiftyTwoWeekLow   float64   `json:"fifty_two_week_low"`
}

type Fundamental struct {
	ID         int64   `json:"id"`
	CompanyID  int64   `json:"company_id"`
	Period     string  `json:"period"`
	MetricName string  `json:"metric_name"`
	Value      float64 `json:"value"`
}

type ActionHistory struct {
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	Ticker     string    `json:"ticker"`
	ActionType string    `json:"action_type"`
	Status     string    `json:"status"`
	Message    string    `json:"message"`
}

type ValuationRatio struct {
	ID              int64     `json:"id"`
	CompanyID       int64     `json:"company_id"`
	PERatio         float64   `json:"pe_ratio"`
	PBRatio         float64   `json:"pb_ratio"`
	PSRatio         float64   `json:"ps_ratio"`
	GrossMargin     float64   `json:"gross_margin"`
	OperatingMargin float64   `json:"operating_margin"`
	NetMargin       float64   `json:"net_margin"`
	ROE             float64   `json:"roe"`
	ROA             float64   `json:"roa"`
	DebtToEquity    float64   `json:"debt_to_equity"`
	Timestamp       time.Time `json:"timestamp"`
}

type Dividend struct {
	ID          int64   `json:"id"`
	CompanyID   int64   `json:"company_id"`
	ExDate      string  `json:"ex_date"`
	PaymentDate string  `json:"payment_date"`
	RecordDate  string  `json:"record_date"`
	Amount      float64 `json:"amount"`
	Currency    string  `json:"currency"`
	Frequency   int     `json:"frequency"`
}

type StockSplit struct {
	ID            int64   `json:"id"`
	CompanyID     int64   `json:"company_id"`
	ExecutionDate string  `json:"execution_date"`
	FromFactor    float64 `json:"from_factor"`
	ToFactor      float64 `json:"to_factor"`
}

type HistoricalPrice struct {
	ID            int64   `json:"id"`
	CompanyID     int64   `json:"company_id"`
	Date          string  `json:"date"`
	OpenPrice     float64 `json:"open_price"`
	HighPrice     float64 `json:"high_price"`
	LowPrice      float64 `json:"low_price"`
	ClosePrice    float64 `json:"close_price"`
	AdjClosePrice float64 `json:"adj_close_price"`
	Volume        int64   `json:"volume"`
}

type AnalystEstimate struct {
	ID         int64  `json:"id"`
	CompanyID  int64  `json:"company_id"`
	Period     string `json:"period"`
	StrongBuy  int    `json:"strong_buy"`
	Buy        int    `json:"buy"`
	Hold       int    `json:"hold"`
	Sell       int    `json:"sell"`
	StrongSell int    `json:"strong_sell"`
}

type EarningsCalendar struct {
	ID              int64   `json:"id"`
	CompanyID       int64   `json:"company_id"`
	Date            string  `json:"date"`
	Quarter         int     `json:"quarter"`
	Year            int     `json:"year"`
	EPSEstimate     float64 `json:"eps_estimate"`
	EPSActual       float64 `json:"eps_actual"`
	RevenueEstimate float64 `json:"revenue_estimate"`
	RevenueActual   float64 `json:"revenue_actual"`
}

type CompanyNews struct {
	ID             int64     `json:"id"`
	CompanyID      int64     `json:"company_id"`
	NewsID         int64     `json:"news_id"`
	Headline       string    `json:"headline"`
	Summary        string    `json:"summary"`
	Source         string    `json:"source"`
	URL            string    `json:"url"`
	PublishedAt    time.Time `json:"published_at"`
	SentimentScore float64   `json:"sentiment_score"`
}

type InsiderTransaction struct {
	ID               int64   `json:"id"`
	CompanyID        int64   `json:"company_id"`
	Name             string  `json:"name"`
	ShareCount       float64 `json:"share_count"`
	ChangeShares     float64 `json:"change_shares"`
	FilingDate       string  `json:"filing_date"`
	TransactionCode  string  `json:"transaction_code"`
	TransactionPrice float64 `json:"transaction_price"`
}

type InstitutionalOwnership struct {
	ID           int64   `json:"id"`
	CompanyID    int64   `json:"company_id"`
	InvestorName string  `json:"investor_name"`
	SharesHeld   float64 `json:"shares_held"`
	ChangeShares float64 `json:"change_shares"`
	Value        float64 `json:"value"`
	Period       string  `json:"period"`
}

type MacroIndicator struct {
	ID            int64   `json:"id"`
	SeriesID      string  `json:"series_id"`
	IndicatorName string  `json:"indicator_name"`
	Date          string  `json:"date"`
	Value         float64 `json:"value"`
}

// ConsolidatedCompanyData aggregates data for GET /api/data/company/{ticker}
type ConsolidatedCompanyData struct {
	Company                Company                  `json:"company"`
	Watchlist              *Watchitem               `json:"watchlist,omitempty"`
	MarketData             []MarketData             `json:"market_data"`
	Fundamentals           []Fundamental            `json:"fundamentals"`
	ValuationRatios        []ValuationRatio         `json:"valuation_ratios"`
	Dividends              []Dividend               `json:"dividends"`
	StockSplits            []StockSplit             `json:"stock_splits"`
	Historical             []HistoricalPrice        `json:"historical_prices"`
	AnalystEstimates       []AnalystEstimate        `json:"analyst_estimates"`
	EarningsCalendar       []EarningsCalendar       `json:"earnings_calendar"`
	CompanyNews            []CompanyNews            `json:"company_news"`
	InsiderTransactions    []InsiderTransaction     `json:"insider_transactions"`
	InstitutionalOwnership []InstitutionalOwnership `json:"institutional_ownership"`
	MacroIndicators        []MacroIndicator         `json:"macro_indicators,omitempty"`
	History                []ActionHistory          `json:"history"`
}

// Execer interface to work with sql.Conn, sql.Tx, sql.DB
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// NewDB initializes the SQLite database with separate Read and Write pools.
func NewDB(dbPath string) (*DB, error) {
	if dbPath != ":memory:" && dbPath != "" {
		dir := filepath.Dir(dbPath)
		if dir != "." && dir != "" {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create db directory: %w", err)
			}
		}
	}

	dsn := dbPath
	if dbPath == ":memory:" {
		dsn = "file:memdb1?mode=memory&cache=shared&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	} else {
		dsn = fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", dbPath)
	}

	// Initialize Read Pool: SetMaxOpenConns(100)
	readDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open read db: %w", err)
	}
	readDB.SetMaxOpenConns(100)
	readDB.SetMaxIdleConns(20)

	if _, err := readDB.Exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON;"); err != nil {
		readDB.Close()
		return nil, fmt.Errorf("failed to set pragmas on read pool: %w", err)
	}

	// Initialize Write Pool: SetMaxOpenConns(1)
	writeDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		readDB.Close()
		return nil, fmt.Errorf("failed to open write db: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	if _, err := writeDB.Exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON;"); err != nil {
		readDB.Close()
		writeDB.Close()
		return nil, fmt.Errorf("failed to set pragmas on write pool: %w", err)
	}

	database := &DB{
		ReadDB:  readDB,
		WriteDB: writeDB,
	}

	if err := database.Migrate(); err != nil {
		database.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return database, nil
}

// Close closes both database pools.
func (db *DB) Close() error {
	var err1, err2 error
	if db.ReadDB != nil {
		err1 = db.ReadDB.Close()
	}
	if db.WriteDB != nil {
		err2 = db.WriteDB.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// WithTx executes fn within a write transaction (BEGIN IMMEDIATE) using WriteDB pool.
func (db *DB) WithTx(ctx context.Context, fn func(exec Execer) error) error {
	conn, err := db.WriteDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection from write pool: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("failed to begin immediate transaction: %w", err)
	}

	txSuccess := false
	defer func() {
		if !txSuccess {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	if err := fn(conn); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	txSuccess = true
	return nil
}

// Migrate creates core tables if they don't exist.
func (db *DB) Migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS watchlist (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ticker TEXT UNIQUE NOT NULL,
		priority INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending',
		last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
		force_full_refresh INTEGER DEFAULT 1
	);

	CREATE TABLE IF NOT EXISTS data_update_timestamps (
		company_id INTEGER NOT NULL,
		category TEXT NOT NULL,
		last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (company_id, category),
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS companies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ticker TEXT UNIQUE NOT NULL,
		cik TEXT,
		isin TEXT,
		name TEXT,
		sector TEXT,
		exchange TEXT,
		outstanding_shares REAL DEFAULT 0,
		logo_url TEXT
	);

	CREATE TABLE IF NOT EXISTS market_data (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		current_price REAL NOT NULL,
		volume INTEGER NOT NULL,
		open_price REAL DEFAULT 0,
		high_price REAL DEFAULT 0,
		low_price REAL DEFAULT 0,
		previous_close REAL DEFAULT 0,
		market_cap REAL DEFAULT 0,
		fifty_two_week_high REAL DEFAULT 0,
		fifty_two_week_low REAL DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS fundamentals (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		period TEXT NOT NULL,
		metric_name TEXT NOT NULL,
		value REAL NOT NULL,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS action_history (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		ticker TEXT NOT NULL,
		action_type TEXT NOT NULL,
		status TEXT NOT NULL,
		message TEXT NOT NULL
	);

	CREATE TABLE IF NOT EXISTS valuation_ratios (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		pe_ratio REAL DEFAULT 0,
		pb_ratio REAL DEFAULT 0,
		ps_ratio REAL DEFAULT 0,
		gross_margin REAL DEFAULT 0,
		operating_margin REAL DEFAULT 0,
		net_margin REAL DEFAULT 0,
		roe REAL DEFAULT 0,
		roa REAL DEFAULT 0,
		debt_to_equity REAL DEFAULT 0,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS dividends (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		ex_date TEXT NOT NULL,
		payment_date TEXT,
		record_date TEXT,
		amount REAL NOT NULL,
		currency TEXT DEFAULT 'USD',
		frequency INTEGER DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS stock_splits (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		execution_date TEXT NOT NULL,
		from_factor REAL NOT NULL,
		to_factor REAL NOT NULL,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS historical_prices (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		date TEXT NOT NULL,
		open_price REAL NOT NULL,
		high_price REAL NOT NULL,
		low_price REAL NOT NULL,
		close_price REAL NOT NULL,
		adj_close_price REAL NOT NULL,
		volume INTEGER NOT NULL,
		UNIQUE(company_id, date) ON CONFLICT REPLACE,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS analyst_estimates (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		period TEXT NOT NULL,
		strong_buy INTEGER DEFAULT 0,
		buy INTEGER DEFAULT 0,
		hold INTEGER DEFAULT 0,
		sell INTEGER DEFAULT 0,
		strong_sell INTEGER DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS earnings_calendar (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		date TEXT NOT NULL,
		quarter INTEGER,
		year INTEGER,
		eps_estimate REAL DEFAULT 0,
		eps_actual REAL DEFAULT 0,
		revenue_estimate REAL DEFAULT 0,
		revenue_actual REAL DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS company_news (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		news_id INTEGER,
		headline TEXT NOT NULL,
		summary TEXT,
		source TEXT,
		url TEXT,
		published_at DATETIME NOT NULL,
		sentiment_score REAL DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS insider_transactions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		name TEXT NOT NULL,
		share_count REAL DEFAULT 0,
		change_shares REAL DEFAULT 0,
		filing_date TEXT NOT NULL,
		transaction_code TEXT,
		transaction_price REAL DEFAULT 0,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS institutional_ownership (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		company_id INTEGER NOT NULL,
		investor_name TEXT NOT NULL,
		shares_held REAL DEFAULT 0,
		change_shares REAL DEFAULT 0,
		value REAL DEFAULT 0,
		period TEXT NOT NULL,
		FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS macro_indicators (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		series_id TEXT NOT NULL,
		indicator_name TEXT NOT NULL,
		date TEXT NOT NULL,
		value REAL NOT NULL,
		UNIQUE(series_id, date) ON CONFLICT REPLACE
	);

	CREATE TABLE IF NOT EXISTS api_keys (
		key_name TEXT PRIMARY KEY,
		key_value TEXT NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	return db.WithTx(context.Background(), func(exec Execer) error {
		if _, err := exec.ExecContext(context.Background(), schema); err != nil {
			return err
		}

		// Migrations for existing tables if columns are missing
		watchlistMigrations := []string{
			"ALTER TABLE watchlist ADD COLUMN force_full_refresh INTEGER DEFAULT 1",
		}
		for _, stmt := range watchlistMigrations {
			_, _ = exec.ExecContext(context.Background(), stmt)
		}

		companyMigrations := []string{
			"ALTER TABLE companies ADD COLUMN exchange TEXT",
			"ALTER TABLE companies ADD COLUMN outstanding_shares REAL DEFAULT 0",
			"ALTER TABLE companies ADD COLUMN logo_url TEXT",
		}
		for _, stmt := range companyMigrations {
			_, _ = exec.ExecContext(context.Background(), stmt)
		}

		marketDataMigrations := []string{
			"ALTER TABLE market_data ADD COLUMN open_price REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN high_price REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN low_price REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN previous_close REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN market_cap REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN fifty_two_week_high REAL DEFAULT 0",
			"ALTER TABLE market_data ADD COLUMN fifty_two_week_low REAL DEFAULT 0",
		}
		for _, stmt := range marketDataMigrations {
			_, _ = exec.ExecContext(context.Background(), stmt)
		}

		return nil
	})
}

func (db *DB) InsertInsiderTransactionsBatch(ctx context.Context, companyID int64, txs []InsiderTransaction) error {
	if len(txs) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, t := range txs {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO insider_transactions (company_id, name, share_count, change_shares, filing_date, transaction_code, transaction_price)
				VALUES (?, ?, ?, ?, ?, ?, ?)
			`, companyID, t.Name, t.ShareCount, t.ChangeShares, t.FilingDate, t.TransactionCode, t.TransactionPrice)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertInstitutionalOwnershipBatch(ctx context.Context, companyID int64, holdings []InstitutionalOwnership) error {
	if len(holdings) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, h := range holdings {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO institutional_ownership (company_id, investor_name, shares_held, change_shares, value, period)
				VALUES (?, ?, ?, ?, ?, ?)
			`, companyID, h.InvestorName, h.SharesHeld, h.ChangeShares, h.Value, h.Period)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertMacroIndicatorsBatch(ctx context.Context, indicators []MacroIndicator) error {
	if len(indicators) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, m := range indicators {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO macro_indicators (series_id, indicator_name, date, value)
				VALUES (?, ?, ?, ?)
				ON CONFLICT(series_id, date) DO UPDATE SET
					value=excluded.value,
					indicator_name=excluded.indicator_name
			`, m.SeriesID, m.IndicatorName, m.Date, m.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertAnalystEstimatesBatch(ctx context.Context, companyID int64, estimates []AnalystEstimate) error {
	if len(estimates) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, e := range estimates {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO analyst_estimates (company_id, period, strong_buy, buy, hold, sell, strong_sell)
				VALUES (?, ?, ?, ?, ?, ?, ?)
			`, companyID, e.Period, e.StrongBuy, e.Buy, e.Hold, e.Sell, e.StrongSell)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertEarningsCalendarBatch(ctx context.Context, companyID int64, events []EarningsCalendar) error {
	if len(events) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, ec := range events {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO earnings_calendar (company_id, date, quarter, year, eps_estimate, eps_actual, revenue_estimate, revenue_actual)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, companyID, ec.Date, ec.Quarter, ec.Year, ec.EPSEstimate, ec.EPSActual, ec.RevenueEstimate, ec.RevenueActual)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertCompanyNewsBatch(ctx context.Context, companyID int64, newsList []CompanyNews) error {
	if len(newsList) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, n := range newsList {
			publishedStr := n.PublishedAt.Format("2006-01-02 15:04:05")
			_, err := exec.ExecContext(ctx, `
				INSERT INTO company_news (company_id, news_id, headline, summary, source, url, published_at, sentiment_score)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, companyID, n.NewsID, n.Headline, n.Summary, n.Source, n.URL, publishedStr, n.SentimentScore)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type FundamentalItem struct {
	Period     string
	MetricName string
	Value      float64
}

func (db *DB) InsertFundamentalsBatch(ctx context.Context, companyID int64, items []FundamentalItem) error {
	if len(items) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, item := range items {
			_, err := exec.ExecContext(ctx, "INSERT INTO fundamentals (company_id, period, metric_name, value) VALUES (?, ?, ?, ?)", companyID, item.Period, item.MetricName, item.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Watchlist methods
func (db *DB) GetWatchlist(ctx context.Context) ([]Watchitem, error) {
	rows, err := db.ReadDB.QueryContext(ctx, "SELECT id, ticker, priority, status, last_updated, COALESCE(force_full_refresh, 0) FROM watchlist ORDER BY priority DESC, id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Watchitem
	for rows.Next() {
		var item Watchitem
		var lastUpdatedStr string
		var forceRefreshInt int
		if err := rows.Scan(&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr, &forceRefreshInt); err != nil {
			return nil, err
		}
		item.ForceFullRefresh = (forceRefreshInt != 0)
		item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
		if item.LastUpdated.IsZero() {
			item.LastUpdated, _ = time.Parse(time.RFC3339, lastUpdatedStr)
		}
		if !item.LastUpdated.IsZero() {
			item.NextUpdateTime = item.LastUpdated.Add(5 * time.Minute)
		} else {
			item.NextUpdateTime = time.Now()
		}
		list = append(list, item)
	}
	return list, nil
}

func (db *DB) GetJobStatusSummary(ctx context.Context) (JobStatusSummary, error) {
	rows, err := db.ReadDB.QueryContext(ctx, "SELECT status, COUNT(*) FROM watchlist GROUP BY status")
	if err != nil {
		return JobStatusSummary{}, err
	}
	defer rows.Close()

	var summary JobStatusSummary
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err == nil {
			summary.Total += count
			switch strings.ToLower(status) {
			case "pending":
				summary.Pending = count
			case "queued":
				summary.Queued = count
			case "processing":
				summary.Processing = count
			case "completed":
				summary.Completed = count
			case "error":
				summary.Error = count
			}
		}
	}
	return summary, nil
}

func (db *DB) GetRecentActionHistory(ctx context.Context, limit int) ([]ActionHistory, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := db.ReadDB.QueryContext(ctx, "SELECT id, timestamp, ticker, action_type, status, message FROM action_history ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []ActionHistory
	for rows.Next() {
		var h ActionHistory
		var ts string
		if err := rows.Scan(&h.ID, &ts, &h.Ticker, &h.ActionType, &h.Status, &h.Message); err == nil {
			h.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
			if h.Timestamp.IsZero() {
				h.Timestamp, _ = time.Parse(time.RFC3339, ts)
			}
			history = append(history, h)
		}
	}
	return history, nil
}

func (db *DB) AddWatchitem(ctx context.Context, ticker string, priority int) (*Watchitem, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	var item Watchitem
	err := db.WithTx(ctx, func(exec Execer) error {
		res, err := exec.ExecContext(ctx,
			"INSERT INTO watchlist (ticker, priority, status, last_updated, force_full_refresh) VALUES (?, ?, 'pending', CURRENT_TIMESTAMP, 1) ON CONFLICT(ticker) DO UPDATE SET priority=excluded.priority, status='pending', last_updated=CURRENT_TIMESTAMP, force_full_refresh=1",
			ticker, priority,
		)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}

		row := exec.QueryRowContext(ctx, "SELECT id, ticker, priority, status, last_updated, COALESCE(force_full_refresh, 0) FROM watchlist WHERE ticker = ?", ticker)
		var lastUpdatedStr string
		var forceRefreshInt int
		if err := row.Scan(&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr, &forceRefreshInt); err != nil {
			item.ID = id
			item.Ticker = ticker
			item.Priority = priority
			item.Status = "pending"
			item.LastUpdated = time.Now()
			item.ForceFullRefresh = true
		} else {
			item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
			item.ForceFullRefresh = (forceRefreshInt != 0)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// FetchAndQueueNextWatchitem atomically fetches the next eligible watchitem and marks it as queued.
func (db *DB) FetchAndQueueNextWatchitem(ctx context.Context) (*Watchitem, error) {
	var item Watchitem
	err := db.WithTx(ctx, func(exec Execer) error {
		query := `
			SELECT id, ticker, priority, status, last_updated, COALESCE(force_full_refresh, 0)
			FROM watchlist
			WHERE status = 'pending'
			   OR (status NOT IN ('queued', 'processing') AND last_updated <= datetime('now', '-5 minutes'))
			ORDER BY (CASE WHEN status = 'pending' THEN 0 ELSE 1 END) ASC, priority DESC, last_updated ASC
			LIMIT 1
		`
		var lastUpdatedStr string
		var forceRefreshInt int
		err := exec.QueryRowContext(ctx, query).Scan(
			&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr, &forceRefreshInt,
		)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
		if item.LastUpdated.IsZero() {
			item.LastUpdated, _ = time.Parse(time.RFC3339, lastUpdatedStr)
		}
		item.ForceFullRefresh = (forceRefreshInt != 0)

		_, err = exec.ExecContext(ctx, "UPDATE watchlist SET status = 'queued', last_updated = CURRENT_TIMESTAMP WHERE id = ?", item.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if item.ID == 0 {
		return nil, nil
	}
	return &item, nil
}

func (db *DB) SetWatchitemForceRefresh(ctx context.Context, ticker string, force bool) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	val := 0
	if force {
		val = 1
	}
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET force_full_refresh = ? WHERE ticker = ?", val, ticker)
		return err
	})
}

// API Key DB storage methods
func (db *DB) SetAPIKey(ctx context.Context, name, value string) error {
	name = strings.TrimSpace(name)
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, `
			INSERT INTO api_keys (key_name, key_value, updated_at)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(key_name) DO UPDATE SET key_value=excluded.key_value, updated_at=CURRENT_TIMESTAMP
		`, name, value)
		return err
	})
}

func (db *DB) GetAPIKey(ctx context.Context, name string) (string, error) {
	name = strings.TrimSpace(name)
	var val string
	err := db.ReadDB.QueryRowContext(ctx, "SELECT key_value FROM api_keys WHERE key_name = ?", name).Scan(&val)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return val, err
}

func (db *DB) GetAPIKeysMap(ctx context.Context) (map[string]string, error) {
	rows, err := db.ReadDB.QueryContext(ctx, "SELECT key_name, key_value FROM api_keys")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make(map[string]string)
	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err == nil {
			keys[name] = val
		}
	}
	return keys, nil
}

func (db *DB) DeleteAPIKey(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "DELETE FROM api_keys WHERE key_name = ?", name)
		return err
	})
}

func (db *DB) SetWatchitemForceRefreshAll(ctx context.Context, force bool) error {
	val := 0
	if force {
		val = 1
	}
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET force_full_refresh = ?, status = 'pending', last_updated = CURRENT_TIMESTAMP", val)
		return err
	})
}

func (db *DB) GetCategoryLastUpdated(ctx context.Context, companyID int64, category string) (time.Time, error) {
	var lastUpdatedStr string
	err := db.ReadDB.QueryRowContext(ctx, "SELECT last_updated FROM data_update_timestamps WHERE company_id = ? AND category = ?", companyID, category).Scan(&lastUpdatedStr)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
	if err != nil {
		t, _ = time.Parse(time.RFC3339, lastUpdatedStr)
	}
	return t, nil
}

func (db *DB) SetCategoryLastUpdated(ctx context.Context, companyID int64, category string) error {
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, `
			INSERT INTO data_update_timestamps (company_id, category, last_updated)
			VALUES (?, ?, CURRENT_TIMESTAMP)
			ON CONFLICT(company_id, category) DO UPDATE SET last_updated = CURRENT_TIMESTAMP
		`, companyID, category)
		return err
	})
}

func (db *DB) UpdateWatchitemPriority(ctx context.Context, ticker string, priority int) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET priority = ?, status = 'pending', last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", priority, ticker)
		return err
	})
}

func (db *DB) InsertDividendsBatch(ctx context.Context, companyID int64, divs []Dividend) error {
	if len(divs) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, d := range divs {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO dividends (company_id, ex_date, payment_date, record_date, amount, currency, frequency)
				VALUES (?, ?, ?, ?, ?, ?, ?)
			`, companyID, d.ExDate, d.PaymentDate, d.RecordDate, d.Amount, d.Currency, d.Frequency)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertStockSplitsBatch(ctx context.Context, companyID int64, splits []StockSplit) error {
	if len(splits) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, s := range splits {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO stock_splits (company_id, execution_date, from_factor, to_factor)
				VALUES (?, ?, ?, ?)
			`, companyID, s.ExecutionDate, s.FromFactor, s.ToFactor)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertHistoricalPricesBatch(ctx context.Context, companyID int64, prices []HistoricalPrice) error {
	if len(prices) == 0 {
		return nil
	}
	return db.WithTx(ctx, func(exec Execer) error {
		for _, p := range prices {
			_, err := exec.ExecContext(ctx, `
				INSERT INTO historical_prices (company_id, date, open_price, high_price, low_price, close_price, adj_close_price, volume)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(company_id, date) DO UPDATE SET
					open_price=excluded.open_price,
					high_price=excluded.high_price,
					low_price=excluded.low_price,
					close_price=excluded.close_price,
					adj_close_price=excluded.adj_close_price,
					volume=excluded.volume
			`, companyID, p.Date, p.OpenPrice, p.HighPrice, p.LowPrice, p.ClosePrice, p.AdjClosePrice, p.Volume)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *DB) InsertValuationRatios(ctx context.Context, companyID int64, vr *ValuationRatio) error {
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, `
			INSERT INTO valuation_ratios (
				company_id, pe_ratio, pb_ratio, ps_ratio, gross_margin, operating_margin, net_margin, roe, roa, debt_to_equity
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, companyID, vr.PERatio, vr.PBRatio, vr.PSRatio, vr.GrossMargin, vr.OperatingMargin, vr.NetMargin, vr.ROE, vr.ROA, vr.DebtToEquity)
		return err
	})
}

func (db *DB) DeleteWatchitem(ctx context.Context, ticker string) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "DELETE FROM watchlist WHERE ticker = ?", ticker)
		return err
	})
}

func (db *DB) FetchNextWatchitem(ctx context.Context) (*Watchitem, error) {
	var item Watchitem
	var lastUpdatedStr string
	// Select pending items first, or items not queued/processing whose last_updated is older than 5 minutes
	query := `
		SELECT id, ticker, priority, status, last_updated
		FROM watchlist
		WHERE status = 'pending'
		   OR (status NOT IN ('queued', 'processing') AND last_updated <= datetime('now', '-5 minutes'))
		ORDER BY (CASE WHEN status = 'pending' THEN 0 ELSE 1 END) ASC, priority DESC, last_updated ASC
		LIMIT 1
	`
	err := db.ReadDB.QueryRowContext(ctx, query).Scan(
		&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
	if item.LastUpdated.IsZero() {
		item.LastUpdated, _ = time.Parse(time.RFC3339, lastUpdatedStr)
	}
	return &item, nil
}

func (db *DB) FetchNextPendingWatchitem(ctx context.Context) (*Watchitem, error) {
	var item Watchitem
	var lastUpdatedStr string
	err := db.ReadDB.QueryRowContext(ctx, "SELECT id, ticker, priority, status, last_updated FROM watchlist WHERE status = 'pending' ORDER BY priority DESC, last_updated ASC LIMIT 1").Scan(
		&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
	if item.LastUpdated.IsZero() {
		item.LastUpdated, _ = time.Parse(time.RFC3339, lastUpdatedStr)
	}
	return &item, nil
}

func (db *DB) UpdateWatchitemStatus(ctx context.Context, ticker string, status string) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET status = ?, last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", status, ticker)
		return err
	})
}

// Company & Financial queries
func (db *DB) UpsertCompany(ctx context.Context, comp *Company) (int64, error) {
	comp.Ticker = strings.ToUpper(strings.TrimSpace(comp.Ticker))
	var id int64
	err := db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, `
			INSERT INTO companies (ticker, cik, isin, name, sector, exchange, outstanding_shares, logo_url)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(ticker) DO UPDATE SET
				cik = COALESCE(NULLIF(excluded.cik, ''), companies.cik),
				isin = COALESCE(NULLIF(excluded.isin, ''), companies.isin),
				name = COALESCE(NULLIF(excluded.name, ''), companies.name),
				sector = COALESCE(NULLIF(excluded.sector, ''), companies.sector),
				exchange = COALESCE(NULLIF(excluded.exchange, ''), companies.exchange),
				outstanding_shares = CASE WHEN excluded.outstanding_shares > 0 THEN excluded.outstanding_shares ELSE companies.outstanding_shares END,
				logo_url = COALESCE(NULLIF(excluded.logo_url, ''), companies.logo_url)
		`, comp.Ticker, comp.CIK, comp.ISIN, comp.Name, comp.Sector, comp.Exchange, comp.OutstandingShares, comp.LogoURL)
		if err != nil {
			return err
		}

		err = exec.QueryRowContext(ctx, "SELECT id FROM companies WHERE ticker = ?", comp.Ticker).Scan(&id)
		return err
	})
	return id, err
}

func (db *DB) InsertMarketData(ctx context.Context, companyID int64, md *MarketData) error {
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, `
			INSERT INTO market_data (
				company_id, current_price, volume, open_price, high_price, low_price, previous_close, market_cap, fifty_two_week_high, fifty_two_week_low
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, companyID, md.CurrentPrice, md.Volume, md.OpenPrice, md.HighPrice, md.LowPrice, md.PreviousClose, md.MarketCap, md.FiftyTwoWeekHigh, md.FiftyTwoWeekLow)
		return err
	})
}

func (db *DB) InsertFundamental(ctx context.Context, companyID int64, period, metricName string, value float64) error {
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "INSERT INTO fundamentals (company_id, period, metric_name, value) VALUES (?, ?, ?, ?)", companyID, period, metricName, value)
		return err
	})
}

func (db *DB) LogAction(ctx context.Context, ticker, actionType, status, message string) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "INSERT INTO action_history (ticker, action_type, status, message) VALUES (?, ?, ?, ?)", ticker, actionType, status, message)
		return err
	})
}

func (db *DB) GetConsolidatedData(ctx context.Context, ticker string) (*ConsolidatedCompanyData, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	var data ConsolidatedCompanyData
	var comp Company

	err := db.ReadDB.QueryRowContext(ctx, "SELECT id, ticker, cik, isin, name, sector, COALESCE(exchange, ''), COALESCE(outstanding_shares, 0), COALESCE(logo_url, '') FROM companies WHERE ticker = ?", ticker).Scan(
		&comp.ID, &comp.Ticker, &comp.CIK, &comp.ISIN, &comp.Name, &comp.Sector, &comp.Exchange, &comp.OutstandingShares, &comp.LogoURL,
	)
	if err == sql.ErrNoRows {
		// Return empty consolidated object with ticker set
		comp.Ticker = ticker
	} else if err != nil {
		return nil, err
	}
	data.Company = comp

	// Watchlist info
	var w Watchitem
	var lastUpdatedStr string
	err = db.ReadDB.QueryRowContext(ctx, "SELECT id, ticker, priority, status, last_updated FROM watchlist WHERE ticker = ?", ticker).Scan(
		&w.ID, &w.Ticker, &w.Priority, &w.Status, &lastUpdatedStr,
	)
	if err == nil {
		w.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
		data.Watchlist = &w
	}

	if comp.ID > 0 {
		// Market data
		mRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, timestamp, current_price, volume,
			       COALESCE(open_price, 0), COALESCE(high_price, 0), COALESCE(low_price, 0), COALESCE(previous_close, 0),
			       COALESCE(market_cap, 0), COALESCE(fifty_two_week_high, 0), COALESCE(fifty_two_week_low, 0)
			FROM market_data WHERE company_id = ? ORDER BY timestamp DESC LIMIT 50
		`, comp.ID)
		if err == nil {
			defer mRows.Close()
			for mRows.Next() {
				var m MarketData
				var ts string
				if err := mRows.Scan(
					&m.ID, &m.CompanyID, &ts, &m.CurrentPrice, &m.Volume,
					&m.OpenPrice, &m.HighPrice, &m.LowPrice, &m.PreviousClose,
					&m.MarketCap, &m.FiftyTwoWeekHigh, &m.FiftyTwoWeekLow,
				); err == nil {
					m.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
					data.MarketData = append(data.MarketData, m)
				}
			}
		}

		// Fundamentals
		fRows, err := db.ReadDB.QueryContext(ctx, "SELECT id, company_id, period, metric_name, value FROM fundamentals WHERE company_id = ? ORDER BY id DESC LIMIT 50", comp.ID)
		if err == nil {
			defer fRows.Close()
			for fRows.Next() {
				var f Fundamental
				if err := fRows.Scan(&f.ID, &f.CompanyID, &f.Period, &f.MetricName, &f.Value); err == nil {
					data.Fundamentals = append(data.Fundamentals, f)
				}
			}
		}

		// Valuation Ratios
		vrRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, pe_ratio, pb_ratio, ps_ratio, gross_margin, operating_margin, net_margin, roe, roa, debt_to_equity, timestamp
			FROM valuation_ratios WHERE company_id = ? ORDER BY timestamp DESC LIMIT 10
		`, comp.ID)
		if err == nil {
			defer vrRows.Close()
			for vrRows.Next() {
				var vr ValuationRatio
				var ts string
				if err := vrRows.Scan(
					&vr.ID, &vr.CompanyID, &vr.PERatio, &vr.PBRatio, &vr.PSRatio,
					&vr.GrossMargin, &vr.OperatingMargin, &vr.NetMargin, &vr.ROE, &vr.ROA, &vr.DebtToEquity, &ts,
				); err == nil {
					vr.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
					data.ValuationRatios = append(data.ValuationRatios, vr)
				}
			}
		}

		// Dividends
		divRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, ex_date, COALESCE(payment_date, ''), COALESCE(record_date, ''), amount, COALESCE(currency, 'USD'), COALESCE(frequency, 0)
			FROM dividends WHERE company_id = ? ORDER BY ex_date DESC LIMIT 50
		`, comp.ID)
		if err == nil {
			defer divRows.Close()
			for divRows.Next() {
				var d Dividend
				if err := divRows.Scan(&d.ID, &d.CompanyID, &d.ExDate, &d.PaymentDate, &d.RecordDate, &d.Amount, &d.Currency, &d.Frequency); err == nil {
					data.Dividends = append(data.Dividends, d)
				}
			}
		}

		// Stock Splits
		splitRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, execution_date, from_factor, to_factor
			FROM stock_splits WHERE company_id = ? ORDER BY execution_date DESC LIMIT 50
		`, comp.ID)
		if err == nil {
			defer splitRows.Close()
			for splitRows.Next() {
				var s StockSplit
				if err := splitRows.Scan(&s.ID, &s.CompanyID, &s.ExecutionDate, &s.FromFactor, &s.ToFactor); err == nil {
					data.StockSplits = append(data.StockSplits, s)
				}
			}
		}

		// Historical Prices
		hpRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, date, open_price, high_price, low_price, close_price, adj_close_price, volume
			FROM historical_prices WHERE company_id = ? ORDER BY date DESC LIMIT 100
		`, comp.ID)
		if err == nil {
			defer hpRows.Close()
			for hpRows.Next() {
				var hp HistoricalPrice
				if err := hpRows.Scan(&hp.ID, &hp.CompanyID, &hp.Date, &hp.OpenPrice, &hp.HighPrice, &hp.LowPrice, &hp.ClosePrice, &hp.AdjClosePrice, &hp.Volume); err == nil {
					data.Historical = append(data.Historical, hp)
				}
			}
		}

		// Analyst Estimates
		aeRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, period, strong_buy, buy, hold, sell, strong_sell
			FROM analyst_estimates WHERE company_id = ? ORDER BY period DESC LIMIT 10
		`, comp.ID)
		if err == nil {
			defer aeRows.Close()
			for aeRows.Next() {
				var ae AnalystEstimate
				if err := aeRows.Scan(&ae.ID, &ae.CompanyID, &ae.Period, &ae.StrongBuy, &ae.Buy, &ae.Hold, &ae.Sell, &ae.StrongSell); err == nil {
					data.AnalystEstimates = append(data.AnalystEstimates, ae)
				}
			}
		}

		// Earnings Calendar
		ecRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, date, COALESCE(quarter, 0), COALESCE(year, 0), COALESCE(eps_estimate, 0), COALESCE(eps_actual, 0), COALESCE(revenue_estimate, 0), COALESCE(revenue_actual, 0)
			FROM earnings_calendar WHERE company_id = ? ORDER BY date DESC LIMIT 10
		`, comp.ID)
		if err == nil {
			defer ecRows.Close()
			for ecRows.Next() {
				var ec EarningsCalendar
				if err := ecRows.Scan(&ec.ID, &ec.CompanyID, &ec.Date, &ec.Quarter, &ec.Year, &ec.EPSEstimate, &ec.EPSActual, &ec.RevenueEstimate, &ec.RevenueActual); err == nil {
					data.EarningsCalendar = append(data.EarningsCalendar, ec)
				}
			}
		}

		// Company News
		cnRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, COALESCE(news_id, 0), headline, COALESCE(summary, ''), COALESCE(source, ''), COALESCE(url, ''), published_at, COALESCE(sentiment_score, 0)
			FROM company_news WHERE company_id = ? ORDER BY published_at DESC LIMIT 20
		`, comp.ID)
		if err == nil {
			defer cnRows.Close()
			for cnRows.Next() {
				var cn CompanyNews
				var pubTs string
				if err := cnRows.Scan(&cn.ID, &cn.CompanyID, &cn.NewsID, &cn.Headline, &cn.Summary, &cn.Source, &cn.URL, &pubTs, &cn.SentimentScore); err == nil {
					cn.PublishedAt, _ = time.Parse("2006-01-02 15:04:05", pubTs)
					data.CompanyNews = append(data.CompanyNews, cn)
				}
			}
		}

		// Insider Transactions
		itRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, name, COALESCE(share_count, 0), COALESCE(change_shares, 0), filing_date, COALESCE(transaction_code, ''), COALESCE(transaction_price, 0)
			FROM insider_transactions WHERE company_id = ? ORDER BY filing_date DESC LIMIT 20
		`, comp.ID)
		if err == nil {
			defer itRows.Close()
			for itRows.Next() {
				var it InsiderTransaction
				if err := itRows.Scan(&it.ID, &it.CompanyID, &it.Name, &it.ShareCount, &it.ChangeShares, &it.FilingDate, &it.TransactionCode, &it.TransactionPrice); err == nil {
					data.InsiderTransactions = append(data.InsiderTransactions, it)
				}
			}
		}

		// Institutional Ownership
		ioRows, err := db.ReadDB.QueryContext(ctx, `
			SELECT id, company_id, investor_name, COALESCE(shares_held, 0), COALESCE(change_shares, 0), COALESCE(value, 0), period
			FROM institutional_ownership WHERE company_id = ? ORDER BY period DESC LIMIT 20
		`, comp.ID)
		if err == nil {
			defer ioRows.Close()
			for ioRows.Next() {
				var io InstitutionalOwnership
				if err := ioRows.Scan(&io.ID, &io.CompanyID, &io.InvestorName, &io.SharesHeld, &io.ChangeShares, &io.Value, &io.Period); err == nil {
					data.InstitutionalOwnership = append(data.InstitutionalOwnership, io)
				}
			}
		}
	}

	// Macro Indicators
	macroRows, err := db.ReadDB.QueryContext(ctx, `
		SELECT id, series_id, indicator_name, date, value
		FROM macro_indicators ORDER BY date DESC LIMIT 50
	`)
	if err == nil {
		defer macroRows.Close()
		for macroRows.Next() {
			var m MacroIndicator
			if err := macroRows.Scan(&m.ID, &m.SeriesID, &m.IndicatorName, &m.Date, &m.Value); err == nil {
				data.MacroIndicators = append(data.MacroIndicators, m)
			}
		}
	}

	// Action history
	hRows, err := db.ReadDB.QueryContext(ctx, "SELECT id, timestamp, ticker, action_type, status, message FROM action_history WHERE ticker = ? ORDER BY timestamp DESC LIMIT 50", ticker)
	if err == nil {
		defer hRows.Close()
		for hRows.Next() {
			var h ActionHistory
			var ts string
			if err := hRows.Scan(&h.ID, &ts, &h.Ticker, &h.ActionType, &h.Status, &h.Message); err == nil {
				h.Timestamp, _ = time.Parse("2006-01-02 15:04:05", ts)
				data.History = append(data.History, h)
			}
		}
	}

	if data.MarketData == nil {
		data.MarketData = []MarketData{}
	}
	if data.Fundamentals == nil {
		data.Fundamentals = []Fundamental{}
	}
	if data.ValuationRatios == nil {
		data.ValuationRatios = []ValuationRatio{}
	}
	if data.Dividends == nil {
		data.Dividends = []Dividend{}
	}
	if data.StockSplits == nil {
		data.StockSplits = []StockSplit{}
	}
	if data.Historical == nil {
		data.Historical = []HistoricalPrice{}
	}
	if data.AnalystEstimates == nil {
		data.AnalystEstimates = []AnalystEstimate{}
	}
	if data.EarningsCalendar == nil {
		data.EarningsCalendar = []EarningsCalendar{}
	}
	if data.CompanyNews == nil {
		data.CompanyNews = []CompanyNews{}
	}
	if data.InsiderTransactions == nil {
		data.InsiderTransactions = []InsiderTransaction{}
	}
	if data.InstitutionalOwnership == nil {
		data.InstitutionalOwnership = []InstitutionalOwnership{}
	}
	if data.MacroIndicators == nil {
		data.MacroIndicators = []MacroIndicator{}
	}
	if data.History == nil {
		data.History = []ActionHistory{}
	}

	return &data, nil
}
