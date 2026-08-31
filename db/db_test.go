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
		Ticker: "AAPL",
		CIK:    "0000320193",
		Name:   "Apple Inc.",
		Sector: "Technology",
	})
	if err != nil {
		t.Fatalf("Failed to upsert company: %v", err)
	}

	if err := database.InsertMarketData(ctx, compID, 150.25, 1000000); err != nil {
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

	if data.Company.Name != "Apple Inc." {
		t.Errorf("Expected company name Apple Inc., got %s", data.Company.Name)
	}
	if len(data.MarketData) != 1 || data.MarketData[0].CurrentPrice != 150.25 {
		t.Errorf("Unexpected market data: %+v", data.MarketData)
	}
	if len(data.Fundamentals) != 3 {
		t.Errorf("Unexpected fundamentals count: %+v", data.Fundamentals)
	}
	if len(data.History) != 1 || data.History[0].Status != "SUCCESS" {
		t.Errorf("Unexpected action history: %+v", data.History)
	}
}
