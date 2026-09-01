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
	apiKey := "test-api-key-123"
	jwtSecret := []byte("test-jwt-secret-456")
	server := NewServer(database, broker, apiKey, jwtSecret)

	// Test unauthorized GET /api/watchlist (should fail 401)
	req := httptest.NewRequest("GET", "/api/watchlist", nil)
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("Expected status 401 Unauthorized for unauthenticated request, got %d", rec.Code)
	}

	// Test issue auth token
	req = httptest.NewRequest("POST", "/api/auth/token", nil)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK for auth token issuance, got %d", rec.Code)
	}
	var tokenResp struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&tokenResp); err != nil || tokenResp.Token == "" {
		t.Fatalf("Failed to decode token response: %v", err)
	}

	// Test authenticated GET /api/watchlist using API Key header
	req = httptest.NewRequest("GET", "/api/watchlist", nil)
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK with X-API-Key header, got %d", rec.Code)
	}

	// Test authenticated GET /api/watchlist using Bearer JWT
	req = httptest.NewRequest("GET", "/api/watchlist", nil)
	req.Header.Set("Authorization", "Bearer "+tokenResp.Token)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK with Bearer JWT, got %d", rec.Code)
	}

	// Test POST /api/watchlist
	postBody, _ := json.Marshal(map[string]any{
		"ticker":   "MSFT",
		"priority": 15,
	})
	req = httptest.NewRequest("POST", "/api/watchlist", bytes.NewBuffer(postBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("Expected status 201 Created, got %d", rec.Code)
	}

	// Test GET /api/data/company/MSFT
	req = httptest.NewRequest("GET", "/api/data/company/MSFT", nil)
	req.Header.Set("X-API-Key", apiKey)
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
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}
}
