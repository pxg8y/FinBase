package db

import (
	"context"
	"testing"
)

func TestDBInitializationAndPragmas(t *testing.T) {
	database, err := NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize memory DB: %v", err)
	}
	defer database.Close()

	// Verify tables were created
	ctx := context.Background()
	item, err := database.AddWatchitem(ctx, "AAPL", 10)
	if err != nil {
		t.Fatalf("Failed to add watchitem: %v", err)
	}
	if item.Ticker != "AAPL" || item.Priority != 10 {
		t.Errorf("Unexpected watchitem: %+v", item)
	}

	list, err := database.GetWatchlist(ctx)
	if err != nil {
		t.Fatalf("Failed to get watchlist: %v", err)
	}
	if len(list) != 1 || list[0].Ticker != "AAPL" {
		t.Errorf("Unexpected watchlist result: %+v", list)
	}

	// Test Company and Data insertion
	compID, err := database.UpsertCompany(ctx, &Company{
		Ticker:            "AAPL",
		CIK:               "0000320193",
		Name:              "Apple Inc.",
		Sector:            "Technology",
		Exchange:          "NASDAQ",
		OutstandingShares: 15000000000,
		LogoURL:           "https://example.com/logo.png",
	})
	if err != nil {
		t.Fatalf("Failed to upsert company: %v", err)
	}

	if err := database.InsertMarketData(ctx, compID, &MarketData{
		CurrentPrice:     150.25,
		Volume:           1000000,
		OpenPrice:        149.00,
		HighPrice:        151.00,
		LowPrice:         148.50,
		PreviousClose:    148.00,
		MarketCap:        2253750000000,
		FiftyTwoWeekHigh: 180.00,
		FiftyTwoWeekLow:  120.00,
	}); err != nil {
		t.Fatalf("Failed to insert market data: %v", err)
	}

	if err := database.InsertFundamental(ctx, compID, "2023-Q4", "Revenues", 89498000000); err != nil {
		t.Fatalf("Failed to insert fundamental data: %v", err)
	}

	batchItems := []FundamentalItem{
		{Period: "2023-Q1", MetricName: "NetIncome", Value: 24160000000},
		{Period: "2023-Q2", MetricName: "NetIncome", Value: 24160000000},
	}
	if err := database.InsertFundamentalsBatch(ctx, compID, batchItems); err != nil {
		t.Fatalf("Failed to insert fundamentals batch: %v", err)
	}

	queuedItem, err := database.FetchAndQueueNextWatchitem(ctx)
	if err != nil {
		t.Fatalf("Failed to fetch and queue next watchitem: %v", err)
	}
	if queuedItem == nil || queuedItem.Ticker != "AAPL" {
		t.Errorf("Expected queued item AAPL, got %+v", queuedItem)
	}

	if err := database.LogAction(ctx, "AAPL", "FETCH", "SUCCESS", "Fetched market and fundamental data"); err != nil {
		t.Fatalf("Failed to log action: %v", err)
	}

	data, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data: %v", err)
	}

	if data.Company.Name != "Apple Inc." || data.Company.Exchange != "NASDAQ" || data.Company.OutstandingShares != 15000000000 || data.Company.LogoURL != "https://example.com/logo.png" {
		t.Errorf("Unexpected company data: %+v", data.Company)
	}
	if len(data.MarketData) != 1 || data.MarketData[0].CurrentPrice != 150.25 || data.MarketData[0].OpenPrice != 149.00 || data.MarketData[0].MarketCap != 2253750000000 {
		t.Errorf("Unexpected market data: %+v", data.MarketData)
	}
	if len(data.Fundamentals) != 3 {
		t.Errorf("Unexpected fundamentals count: %+v", data.Fundamentals)
	}
	if len(data.History) != 1 || data.History[0].Status != "SUCCESS" {
		t.Errorf("Unexpected action history: %+v", data.History)
	}
}

func TestDBMigrate(t *testing.T) {
	t.Run("Idempotency", func(t *testing.T) {
		database, err := NewDB(":memory:")
		if err != nil {
			t.Fatalf("Failed to initialize memory DB: %v", err)
		}
		defer database.Close()

		// Call Migrate explicitly multiple times
		for i := 0; i < 3; i++ {
			if err := database.Migrate(); err != nil {
				t.Fatalf("Migrate() iteration %d failed: %v", i+1, err)
			}
		}

		// Verify core tables exist
		tables := []string{"watchlist", "companies", "market_data", "fundamentals", "action_history"}
		for _, table := range tables {
			var name string
			err := database.ReadDB.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
			if err != nil {
				t.Errorf("Expected table %s to exist, query returned: %v", table, err)
			}
		}
	})

	t.Run("ColumnAdditionMigration", func(t *testing.T) {
		// Test migration on a database with minimal table definitions (missing alter columns)
		database, err := NewDB(":memory:")
		if err != nil {
			t.Fatalf("Failed to initialize memory DB: %v", err)
		}
		defer database.Close()

		// Drop and re-create companies table with only old schema columns
		_, err = database.WriteDB.Exec(`
			DROP TABLE companies;
			CREATE TABLE companies (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				ticker TEXT UNIQUE NOT NULL,
				cik TEXT,
				isin TEXT,
				name TEXT,
				sector TEXT
			);
		`)
		if err != nil {
			t.Fatalf("Failed to set up legacy companies table: %v", err)
		}

		// Run Migrate to execute column additions
		if err := database.Migrate(); err != nil {
			t.Fatalf("Migrate() failed on legacy table setup: %v", err)
		}

		// Check if added columns exist in companies table
		rows, err := database.ReadDB.Query("PRAGMA table_info(companies)")
		if err != nil {
			t.Fatalf("Failed to query table_info for companies: %v", err)
		}
		defer rows.Close()

		columns := make(map[string]bool)
		for rows.Next() {
			var cid int
			var name, ctype string
			var notnull int
			var dfltValue interface{}
			var pk int
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
				t.Fatalf("Failed scanning table_info row: %v", err)
			}
			columns[name] = true
		}

		expectedCols := []string{"exchange", "outstanding_shares", "logo_url"}
		for _, col := range expectedCols {
			if !columns[col] {
				t.Errorf("Expected column %s to be added by Migrate(), but it was missing", col)
			}
		}
	})
}
