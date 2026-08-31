package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestWorkerPoolProcessing(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize db: %v", err)
	}
	defer database.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Seed watchlist
	_, err = database.AddWatchitem(ctx, "AAPL", 5)
	if err != nil {
		t.Fatalf("Failed to add watchitem: %v", err)
	}

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer mockServer.Close()

	clientMgr := clients.NewClientManager("TestApp user@test.com", "test-finnhub", "test-figi")

	mockBroadcaster := &MockBroadcaster{}
	wp := NewWorkerPool(database, clientMgr, mockBroadcaster, 2)

	wp.Start(ctx)

	// Wait for job to be processed
	time.Sleep(3 * time.Second)
	wp.Stop()

	// Check that action history or status updated
	list, err := database.GetWatchlist(ctx)
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
