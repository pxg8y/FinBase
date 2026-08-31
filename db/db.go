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
	ID          int64     `json:"id"`
	Ticker      string    `json:"ticker"`
	Priority    int       `json:"priority"`
	Status      string    `json:"status"`
	LastUpdated time.Time `json:"last_updated"`
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

// ConsolidatedCompanyData aggregates data for GET /api/data/company/{ticker}
type ConsolidatedCompanyData struct {
	Company      Company         `json:"company"`
	Watchlist    *Watchitem      `json:"watchlist,omitempty"`
	MarketData   []MarketData    `json:"market_data"`
	Fundamentals []Fundamental   `json:"fundamentals"`
	History      []ActionHistory `json:"history"`
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
		last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
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
	`
	return db.WithTx(context.Background(), func(exec Execer) error {
		if _, err := exec.ExecContext(context.Background(), schema); err != nil {
			return err
		}

		// Migrations for existing tables if columns are missing
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
	rows, err := db.ReadDB.QueryContext(ctx, "SELECT id, ticker, priority, status, last_updated FROM watchlist ORDER BY priority DESC, id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Watchitem
	for rows.Next() {
		var item Watchitem
		var lastUpdatedStr string
		if err := rows.Scan(&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr); err != nil {
			return nil, err
		}
		item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
		if item.LastUpdated.IsZero() {
			item.LastUpdated, _ = time.Parse(time.RFC3339, lastUpdatedStr)
		}
		list = append(list, item)
	}
	return list, nil
}

func (db *DB) AddWatchitem(ctx context.Context, ticker string, priority int) (*Watchitem, error) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	var item Watchitem
	err := db.WithTx(ctx, func(exec Execer) error {
		res, err := exec.ExecContext(ctx,
			"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, 'pending', CURRENT_TIMESTAMP) ON CONFLICT(ticker) DO UPDATE SET priority=excluded.priority, status='pending', last_updated=CURRENT_TIMESTAMP",
			ticker, priority,
		)
		if err != nil {
			return err
		}
		id, err := res.LastInsertId()
		if err != nil {
			return err
		}

		row := exec.QueryRowContext(ctx, "SELECT id, ticker, priority, status, last_updated FROM watchlist WHERE ticker = ?", ticker)
		var lastUpdatedStr string
		if err := row.Scan(&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr); err != nil {
			item.ID = id
			item.Ticker = ticker
			item.Priority = priority
			item.Status = "pending"
			item.LastUpdated = time.Now()
		} else {
			item.LastUpdated, _ = time.Parse("2006-01-02 15:04:05", lastUpdatedStr)
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
			SELECT id, ticker, priority, status, last_updated
			FROM watchlist
			WHERE status = 'pending'
			   OR (status NOT IN ('queued', 'processing') AND last_updated <= datetime('now', '-5 minutes'))
			ORDER BY (CASE WHEN status = 'pending' THEN 0 ELSE 1 END) ASC, priority DESC, last_updated ASC
			LIMIT 1
		`
		var lastUpdatedStr string
		err := exec.QueryRowContext(ctx, query).Scan(
			&item.ID, &item.Ticker, &item.Priority, &item.Status, &lastUpdatedStr,
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

func (db *DB) UpdateWatchitemPriority(ctx context.Context, ticker string, priority int) error {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	return db.WithTx(ctx, func(exec Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET priority = ?, status = 'pending', last_updated = CURRENT_TIMESTAMP WHERE ticker = ?", priority, ticker)
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
	if data.History == nil {
		data.History = []ActionHistory{}
	}

	return &data, nil
}
