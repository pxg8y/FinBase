package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"finbase/clients"
	"finbase/db"
)

type MockBroadcaster struct {
	mu      sync.Mutex
	logs    []string
	updates []string
}

func (m *MockBroadcaster) BroadcastLog(ticker, status, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, ticker+":"+status+":"+message)
}

func (m *MockBroadcaster) BroadcastUpdate(ticker string, data any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, ticker)
}

func TestExtractAndStoreSECFacts(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize db: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	compID, err := database.UpsertCompany(ctx, &db.Company{
		Ticker: "AAPL",
		CIK:    "0000320193",
		Name:   "Apple Inc.",
	})
	if err != nil {
		t.Fatalf("Failed to upsert company: %v", err)
	}

	facts := &clients.SECCompanyFacts{
		CIK:        320193,
		EntityName: "Apple Inc.",
		Facts: map[string]interface{}{
			"us-gaap": map[string]interface{}{
				"Revenues": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(89498000000),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
				"EarningsPerShareDiluted": map[string]interface{}{
					"units": map[string]interface{}{
						"USD/shares": []interface{}{
							map[string]interface{}{
								"val": float64(6.13),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
				"GrossProfit": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(170000000000),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
				"Liabilities": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(290000000000),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
				"NetCashProvidedByUsedInOperatingActivities": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(110543000000),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
				"PaymentsToAcquirePropertyPlantAndEquipment": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(10959000000),
								"fy":  float64(2023),
								"fp":  "FY",
							},
						},
					},
				},
			},
		},
	}

	count := extractAndStoreSECFacts(ctx, database, compID, facts)
	if count != 5 { // Revenues, EPS, GrossProfit, TotalLiabilities, FreeCashFlow
		t.Errorf("Expected 5 fundamentals stored, got %d", count)
	}

	data, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data: %v", err)
	}

	if len(data.Fundamentals) != 5 {
		t.Errorf("Expected 5 fundamentals in consolidated data, got %d", len(data.Fundamentals))
	}
}

func TestExtractAndStoreFMPFacts(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize db: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	compID, err := database.UpsertCompany(ctx, &db.Company{
		Ticker: "SHOP",
		Name:   "Shopify Inc.",
	})
	if err != nil {
		t.Fatalf("Failed to upsert company: %v", err)
	}

	stmts := []clients.FMPIncomeStatement{
		{
			CalendarYear:    "2023",
			Period:          "FY",
			Revenue:         7060000000,
			GrossProfit:     3500000000,
			OperatingIncome: 500000000,
			NetIncome:       132000000,
			EpsDiluted:      0.10,
		},
	}

	count := extractAndStoreFMPFacts(ctx, database, compID, stmts)
	if count != 5 {
		t.Errorf("Expected 5 FMP fundamentals stored, got %d", count)
	}
}

func TestWorkerPoolProcessing(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize db: %v", err)
	}
	defer database.Close()

	bgCtx := context.Background()

	// Seed watchlist
	_, err = database.AddWatchitem(bgCtx, "AAPL", 5)
	if err != nil {
		t.Fatalf("Failed to add watchitem: %v", err)
	}

	clientMgr := clients.NewClientManager("TestApp user@test.com", "test-finnhub", "test-figi", "test-tiingo", "test-twelve", "test-fmp")

	mockBroadcaster := &MockBroadcaster{}
	wp := NewWorkerPool(database, clientMgr, mockBroadcaster, 2)

	workerCtx, cancel := context.WithTimeout(bgCtx, 10*time.Second)
	defer cancel()

	wp.Start(workerCtx)

	// Wait for job to be processed
	time.Sleep(3 * time.Second)
	wp.Stop()

	// Check that action history or status updated
	list, err := database.GetWatchlist(bgCtx)
	if err != nil {
		t.Fatalf("Failed to fetch watchlist: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("Expected watchlist item")
	}

	if list[0].Status == "pending" {
		t.Errorf("Expected status to change from pending, got %s", list[0].Status)
	}
}

func TestDispatcherContinuousRescheduling(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize db: %v", err)
	}
	defer database.Close()

	ctx := context.Background()
	_, err = database.AddWatchitem(ctx, "AAPL", 10)
	if err != nil {
		t.Fatalf("Failed to add watchitem: %v", err)
	}

	// 1. Pending item is immediately returned
	item, err := database.FetchNextWatchitem(ctx)
	if err != nil || item == nil || item.Ticker != "AAPL" {
		t.Fatalf("Expected pending item AAPL, got %+v (err: %v)", item, err)
	}

	// Mark item as completed with last_updated in the past (>5 mins ago)
	if err := database.WithTx(ctx, func(exec db.Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET status = 'completed', last_updated = datetime('now', '-10 minutes') WHERE ticker = 'AAPL'")
		return err
	}); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 2. Item older than 5 minutes is returned for re-scheduling
	item, err = database.FetchNextWatchitem(ctx)
	if err != nil || item == nil || item.Ticker != "AAPL" {
		t.Fatalf("Expected completed item AAPL (older than 5 mins) to be re-eligible, got %+v (err: %v)", item, err)
	}

	// Mark item as completed recently (10 seconds ago)
	if err := database.WithTx(ctx, func(exec db.Execer) error {
		_, err := exec.ExecContext(ctx, "UPDATE watchlist SET status = 'completed', last_updated = datetime('now', '-10 seconds') WHERE ticker = 'AAPL'")
		return err
	}); err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 3. Recently completed item is NOT returned (prevents tight re-queue loop)
	item, err = database.FetchNextWatchitem(ctx)
	if err != nil {
		t.Fatalf("FetchNextWatchitem error: %v", err)
	}
	if item != nil {
		t.Fatalf("Expected nil for recently completed item to prevent tight loop, got %+v", item)
	}
}
