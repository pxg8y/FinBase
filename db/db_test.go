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

func TestFetchNextPendingWatchitem(t *testing.T) {
	database, err := NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize memory DB: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	// 1. Empty database - should return nil, nil
	item, err := database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error on empty DB: %v", err)
	}
	if item != nil {
		t.Errorf("Expected nil item for empty DB, got: %+v", item)
	}

	// 2. Only non-pending items - should return nil, nil
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
		"COMPLETED_TICKER", 100, "completed",
	)
	if err != nil {
		t.Fatalf("Failed to insert completed watchitem: %v", err)
	}
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
		"QUEUED_TICKER", 100, "queued",
	)
	if err != nil {
		t.Fatalf("Failed to insert queued watchitem: %v", err)
	}

	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error when only non-pending items exist: %v", err)
	}
	if item != nil {
		t.Errorf("Expected nil item when no pending items exist, got: %+v", item)
	}

	// 3. Insert pending items with different priorities and timestamps
	// LOW_PRIO: priority 5, last_updated 30 mins ago
	// HIGH_PRIO_NEW: priority 10, last_updated 5 mins ago
	// HIGH_PRIO_OLD: priority 10, last_updated 20 mins ago
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, 'pending', datetime('now', '-30 minutes'))",
		"LOW_PRIO", 5,
	)
	if err != nil {
		t.Fatalf("Failed to insert LOW_PRIO: %v", err)
	}
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, 'pending', datetime('now', '-5 minutes'))",
		"HIGH_PRIO_NEW", 10,
	)
	if err != nil {
		t.Fatalf("Failed to insert HIGH_PRIO_NEW: %v", err)
	}
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, 'pending', datetime('now', '-20 minutes'))",
		"HIGH_PRIO_OLD", 10,
	)
	if err != nil {
		t.Fatalf("Failed to insert HIGH_PRIO_OLD: %v", err)
	}

	// Should select HIGH_PRIO_OLD (highest priority = 10, older last_updated = -20 mins vs -5 mins)
	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error fetching next pending watchitem: %v", err)
	}
	if item == nil {
		t.Fatalf("Expected watchitem, got nil")
	}
	if item.Ticker != "HIGH_PRIO_OLD" {
		t.Errorf("Expected ticker HIGH_PRIO_OLD, got %s", item.Ticker)
	}
	if item.Priority != 10 {
		t.Errorf("Expected priority 10, got %d", item.Priority)
	}
	if item.Status != "pending" {
		t.Errorf("Expected status pending, got %s", item.Status)
	}
	if item.LastUpdated.IsZero() {
		t.Errorf("Expected valid LastUpdated timestamp, got zero time")
	}

	// 4. Update HIGH_PRIO_OLD status to 'processing'
	if err := database.UpdateWatchitemStatus(ctx, "HIGH_PRIO_OLD", "processing"); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// Next should be HIGH_PRIO_NEW
	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error fetching next pending watchitem: %v", err)
	}
	if item == nil || item.Ticker != "HIGH_PRIO_NEW" {
		t.Errorf("Expected HIGH_PRIO_NEW, got: %+v", item)
	}

	// 5. Update HIGH_PRIO_NEW status to 'completed'
	if err := database.UpdateWatchitemStatus(ctx, "HIGH_PRIO_NEW", "completed"); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// Next should be LOW_PRIO
	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error fetching next pending watchitem: %v", err)
	}
	if item == nil || item.Ticker != "LOW_PRIO" {
		t.Errorf("Expected LOW_PRIO, got: %+v", item)
	}

	// 6. Update LOW_PRIO status to 'failed'
	if err := database.UpdateWatchitemStatus(ctx, "LOW_PRIO", "failed"); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// No more pending items
	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error fetching next pending watchitem: %v", err)
	}
	if item != nil {
		t.Errorf("Expected nil when no pending items remain, got: %+v", item)
	}

	// 7. Verify RFC3339 formatted timestamp parsing
	_, err = database.WriteDB.ExecContext(ctx,
		"INSERT INTO watchlist (ticker, priority, status, last_updated) VALUES (?, ?, 'pending', '2025-01-15T12:00:00Z')",
		"RFC3339_TICKER", 1,
	)
	if err != nil {
		t.Fatalf("Failed to insert RFC3339 formatted watchitem: %v", err)
	}

	item, err = database.FetchNextPendingWatchitem(ctx)
	if err != nil {
		t.Fatalf("Unexpected error fetching RFC3339 item: %v", err)
	}
	if item == nil || item.Ticker != "RFC3339_TICKER" {
		t.Fatalf("Expected RFC3339_TICKER, got: %+v", item)
	}
	if item.LastUpdated.IsZero() {
		t.Errorf("Expected LastUpdated to be parsed for RFC3339 timestamp")
	}
}
