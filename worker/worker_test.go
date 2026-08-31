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
				"NetIncomeLoss": map[string]interface{}{
					"units": map[string]interface{}{
						"USD": []interface{}{
							map[string]interface{}{
								"val": float64(22956000000),
								"fy":  float64(2023),
								"fp":  "Q4",
							},
						},
					},
				},
			},
		},
	}

	count := extractAndStoreSECFacts(ctx, database, compID, facts)
	if count != 2 {
		t.Errorf("Expected 2 fundamentals stored, got %d", count)
	}

	data, err := database.GetConsolidatedData(ctx, "AAPL")
	if err != nil {
		t.Fatalf("Failed to get consolidated data: %v", err)
	}

	if len(data.Fundamentals) != 2 {
		t.Errorf("Expected 2 fundamentals in consolidated data, got %d", len(data.Fundamentals))
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

	clientMgr := clients.NewClientManager("TestApp user@test.com", "test-finnhub", "test-figi")

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
