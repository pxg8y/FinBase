package db

import (
	"context"
	"testing"
	"time"
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

	// Milestone 1 Test: Valuation & Ratios DB operations
	valRatio := &ValuationRatio{
		PERatio:         28.5,
		PBRatio:         45.2,
		PSRatio:         7.8,
		GrossMargin:     44.1,
		OperatingMargin: 30.2,
		NetMargin:       25.3,
		ROE:             160.5,
		ROA:             28.4,
		DebtToEquity:    1.8,
	}
	if err := database.InsertValuationRatios(ctx, compID, valRatio); err != nil {
		t.Fatalf("Failed to insert valuation ratios: %v", err)
	}

	dataWithRatios, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data with ratios: %v", err)
	}
	if len(dataWithRatios.ValuationRatios) != 1 {
		t.Fatalf("Expected 1 valuation ratio record, got %d", len(dataWithRatios.ValuationRatios))
	}
	r := dataWithRatios.ValuationRatios[0]
	if r.PERatio != 28.5 || r.GrossMargin != 44.1 || r.DebtToEquity != 1.8 {
		t.Errorf("Unexpected valuation ratio values: %+v", r)
	}

	// Milestone 2 Test: Dividends & Stock Splits DB operations
	divs := []Dividend{
		{ExDate: "2023-11-10", PaymentDate: "2023-11-16", RecordDate: "2023-11-13", Amount: 0.24, Currency: "USD", Frequency: 4},
	}
	if err := database.InsertDividendsBatch(ctx, compID, divs); err != nil {
		t.Fatalf("Failed to insert dividends batch: %v", err)
	}

	splits := []StockSplit{
		{ExecutionDate: "2020-08-31", FromFactor: 1, ToFactor: 4},
	}
	if err := database.InsertStockSplitsBatch(ctx, compID, splits); err != nil {
		t.Fatalf("Failed to insert stock splits batch: %v", err)
	}

	dataWithCorpActions, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data with corporate actions: %v", err)
	}
	if len(dataWithCorpActions.Dividends) != 1 || dataWithCorpActions.Dividends[0].Amount != 0.24 {
		t.Errorf("Unexpected dividends: %+v", dataWithCorpActions.Dividends)
	}
	if len(dataWithCorpActions.StockSplits) != 1 || dataWithCorpActions.StockSplits[0].ToFactor != 4 {
		t.Errorf("Unexpected stock splits: %+v", dataWithCorpActions.StockSplits)
	}

	// Milestone 4 & 5 Test: Analyst Estimates, Earnings Calendar, Company News DB operations
	estimates := []AnalystEstimate{
		{Period: "2023-11-01", StrongBuy: 10, Buy: 15, Hold: 5, Sell: 1, StrongSell: 0},
	}
	if err := database.InsertAnalystEstimatesBatch(ctx, compID, estimates); err != nil {
		t.Fatalf("Failed to insert analyst estimates batch: %v", err)
	}

	earnings := []EarningsCalendar{
		{Date: "2023-11-02", Quarter: 4, Year: 2023, EPSEstimate: 1.39, EPSActual: 1.46, RevenueEstimate: 89300000000, RevenueActual: 89500000000},
	}
	if err := database.InsertEarningsCalendarBatch(ctx, compID, earnings); err != nil {
		t.Fatalf("Failed to insert earnings calendar batch: %v", err)
	}

	news := []CompanyNews{
		{NewsID: 1001, Headline: "Apple Reports Fourth Quarter Results", Summary: "Apple today announced financial results", Source: "Business Wire", URL: "https://example.com/news/1", PublishedAt: time.Now()},
	}
	if err := database.InsertCompanyNewsBatch(ctx, compID, news); err != nil {
		t.Fatalf("Failed to insert company news batch: %v", err)
	}

	dataExt, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data with ext domains: %v", err)
	}
	if len(dataExt.AnalystEstimates) != 1 || dataExt.AnalystEstimates[0].StrongBuy != 10 {
		t.Errorf("Unexpected analyst estimates: %+v", dataExt.AnalystEstimates)
	}
	if len(dataExt.EarningsCalendar) != 1 || dataExt.EarningsCalendar[0].EPSActual != 1.46 {
		t.Errorf("Unexpected earnings calendar: %+v", dataExt.EarningsCalendar)
	}
	if len(dataExt.CompanyNews) != 1 || dataExt.CompanyNews[0].Headline != "Apple Reports Fourth Quarter Results" {
		t.Errorf("Unexpected company news: %+v", dataExt.CompanyNews)
	}

	// Milestone 6 & 7 Test: Insider Transactions, Institutional Ownership, Macro Indicators DB operations
	insiders := []InsiderTransaction{
		{Name: "Cook Timothy D", ShareCount: 3000000, ChangeShares: -50000, FilingDate: "2023-10-15", TransactionCode: "S", TransactionPrice: 178.50},
	}
	if err := database.InsertInsiderTransactionsBatch(ctx, compID, insiders); err != nil {
		t.Fatalf("Failed to insert insider transactions batch: %v", err)
	}

	institutionals := []InstitutionalOwnership{
		{InvestorName: "Vanguard Group Inc", SharesHeld: 120000000, ChangeShares: 1500000, Value: 21000000000, Period: "2023-Q3"},
	}
	if err := database.InsertInstitutionalOwnershipBatch(ctx, compID, institutionals); err != nil {
		t.Fatalf("Failed to insert institutional ownership batch: %v", err)
	}

	macros := []MacroIndicator{
		{SeriesID: "DGS10", IndicatorName: "10-Year Treasury Yield", Date: "2023-11-01", Value: 4.52},
	}
	if err := database.InsertMacroIndicatorsBatch(ctx, macros); err != nil {
		t.Fatalf("Failed to insert macro indicators batch: %v", err)
	}

	dataFinal, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data with final domains: %v", err)
	}
	if len(dataFinal.InsiderTransactions) != 1 || dataFinal.InsiderTransactions[0].Name != "Cook Timothy D" {
		t.Errorf("Unexpected insider transactions: %+v", dataFinal.InsiderTransactions)
	}
	if len(dataFinal.InstitutionalOwnership) != 1 || dataFinal.InstitutionalOwnership[0].InvestorName != "Vanguard Group Inc" {
		t.Errorf("Unexpected institutional ownership: %+v", dataFinal.InstitutionalOwnership)
	}
	if len(dataFinal.MacroIndicators) != 1 || dataFinal.MacroIndicators[0].Value != 4.52 {
		t.Errorf("Unexpected macro indicators: %+v", dataFinal.MacroIndicators)
	}
}

func TestDeleteWatchitem(t *testing.T) {
	database, err := NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize memory DB: %v", err)
	}
	defer database.Close()

	ctx := context.Background()

	// Add items to watchlist
	if _, err := database.AddWatchitem(ctx, "AAPL", 10); err != nil {
		t.Fatalf("Failed to add watchitem AAPL: %v", err)
	}
	if _, err := database.AddWatchitem(ctx, "MSFT", 5); err != nil {
		t.Fatalf("Failed to add watchitem MSFT: %v", err)
	}

	list, err := database.GetWatchlist(ctx)
	if err != nil {
		t.Fatalf("Failed to get watchlist: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("Expected 2 watchitems, got %d", len(list))
	}

	// Delete AAPL with whitespace and lowercase to test ticker normalization
	if err := database.DeleteWatchitem(ctx, "  aapl  "); err != nil {
		t.Fatalf("Failed to delete watchitem aapl: %v", err)
	}

	list, err = database.GetWatchlist(ctx)
	if err != nil {
		t.Fatalf("Failed to get watchlist after deletion: %v", err)
	}
	if len(list) != 1 || list[0].Ticker != "MSFT" {
		t.Errorf("Expected watchlist to contain only MSFT, got %+v", list)
	}

	// Deleting a non-existent item should not return error
	if err := database.DeleteWatchitem(ctx, "NONEXISTENT"); err != nil {
		t.Fatalf("Deleting non-existent watchitem returned error: %v", err)
	}
}

func TestPersistentDBFileAndRestart(t *testing.T) {
	tempDir := t.TempDir()
	dbFilePath := tempDir + "/test_finbase.db"

	ctx := context.Background()

	// 1. Initial creation and data write
	db1, err := NewDB(dbFilePath)
	if err != nil {
		t.Fatalf("Failed to initialize file DB: %v", err)
	}

	if _, err := db1.AddWatchitem(ctx, "NVDA", 15); err != nil {
		db1.Close()
		t.Fatalf("Failed to add watchitem to file DB: %v", err)
	}

	compID, err := db1.UpsertCompany(ctx, &Company{
		Ticker: "NVDA",
		Name:   "NVIDIA Corporation",
	})
	if err != nil {
		db1.Close()
		t.Fatalf("Failed to upsert company in file DB: %v", err)
	}

	if err := db1.InsertMarketData(ctx, compID, &MarketData{
		CurrentPrice: 120.50,
		Volume:       5000000,
	}); err != nil {
		db1.Close()
		t.Fatalf("Failed to insert market data: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("Failed to close file DB: %v", err)
	}

	// 2. Re-open existing DB file (simulate container restart)
	db2, err := NewDB(dbFilePath)
	if err != nil {
		t.Fatalf("Failed to reopen file DB: %v", err)
	}
	defer db2.Close()

	list, err := db2.GetWatchlist(ctx)
	if err != nil {
		t.Fatalf("Failed to query watchlist after reopen: %v", err)
	}

	if len(list) != 1 || list[0].Ticker != "NVDA" || list[0].Priority != 15 {
		t.Errorf("Unexpected watchlist after restart: %+v", list)
	}

	data, err := db2.GetConsolidatedData(ctx, "NVDA")
	if err != nil {
		t.Fatalf("Failed to query consolidated data after reopen: %v", err)
	}

	if data.Company.Name != "NVIDIA Corporation" {
		t.Errorf("Unexpected company name after restart: %s", data.Company.Name)
	}
	if len(data.MarketData) != 1 || data.MarketData[0].CurrentPrice != 120.50 {
		t.Errorf("Unexpected market data after restart: %+v", data.MarketData)
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
