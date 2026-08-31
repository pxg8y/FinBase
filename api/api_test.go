package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"finbase/db"
)

func TestAPIWatchlistAndCompany(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer database.Close()

	broker := NewSSEBroker()
	server := NewServer(database, broker)

	// Test GET /api/watchlist (empty)
	req := httptest.NewRequest("GET", "/api/watchlist", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}

	// Test POST /api/watchlist
	postBody, _ := json.Marshal(map[string]any{
		"ticker":   "MSFT",
		"priority": 15,
	})
	req = httptest.NewRequest("POST", "/api/watchlist", bytes.NewBuffer(postBody))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Expected status 201 Created, got %d", rec.Code)
	}

	// Test GET /api/data/company/MSFT
	req = httptest.NewRequest("GET", "/api/data/company/MSFT", nil)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}

	var data db.ConsolidatedCompanyData
	if err := json.NewDecoder(rec.Body).Decode(&data); err != nil {
		t.Fatalf("Failed to decode consolidated company data: %v", err)
	}

	if data.Company.Ticker != "MSFT" {
		t.Errorf("Expected company ticker MSFT, got %s", data.Company.Ticker)
	}

	// Test DELETE /api/watchlist?ticker=MSFT
	req = httptest.NewRequest("DELETE", "/api/watchlist?ticker=MSFT", nil)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}
}
