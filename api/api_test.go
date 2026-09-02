package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"finbase/clients"
	"finbase/db"
	"finbase/env"
)

func TestAPIEnvKeysManagement(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer database.Close()

	clientMgr := clients.NewClientManager("", "", "", "", "", "")
	broker := NewSSEBroker()
	apiKey := "test-initial-api-key"
	jwtSecret := []byte("test-jwt-secret")

	envSvc := env.NewEnvService(database, clientMgr, apiKey)
	if err := envSvc.LoadAndApplyKeys(context.Background()); err != nil {
		t.Fatalf("LoadAndApplyKeys error: %v", err)
	}

	server := NewServer(database, broker, apiKey, jwtSecret)
	server.SetEnvService(envSvc)
	server.SetClientManager(clientMgr)

	testServer := httptest.NewServer(server)
	defer testServer.Close()

	// 1. GET /api/env/keys without auth -> 401
	resp, err := http.Get(testServer.URL + "/api/env/keys")
	if err != nil {
		t.Fatalf("HTTP GET error: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	// 2. GET /api/env/keys with API Key -> 200
	req, _ := http.NewRequest("GET", testServer.URL+"/api/env/keys", nil)
	req.Header.Set("X-API-Key", apiKey)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP GET with key error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", resp.StatusCode)
	}

	var keyInfos []env.KeyInfo
	json.NewDecoder(resp.Body).Decode(&keyInfos)
	resp.Body.Close()

	if len(keyInfos) == 0 {
		t.Errorf("Expected key status list, got empty")
	}

	// 3. POST /api/env/keys to change API_KEY -> 200
	setPayload := `{"name":"API_KEY","value":"updated-dynamic-api-key"}`
	req, _ = http.NewRequest("POST", testServer.URL+"/api/env/keys", strings.NewReader(setPayload))
	req.Header.Set("X-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP POST set key error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK on setting API key, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 4. Verify old API_KEY rejected and new API_KEY accepted
	req, _ = http.NewRequest("GET", testServer.URL+"/api/env/keys", nil)
	req.Header.Set("X-API-Key", apiKey)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected old API key to be rejected (401), got %d", resp.StatusCode)
	}

	req, _ = http.NewRequest("GET", testServer.URL+"/api/env/keys", nil)
	req.Header.Set("X-API-Key", "updated-dynamic-api-key")
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected new API key to be accepted (200), got %d", resp.StatusCode)
	}

	// 5. POST /api/env/keys/test endpoint
	testPayload := `{"name":"FINNHUB_API_KEY"}`
	req, _ = http.NewRequest("POST", testServer.URL+"/api/env/keys/test", strings.NewReader(testPayload))
	req.Header.Set("X-API-Key", "updated-dynamic-api-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP POST test key error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK on key test endpoint, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 6. DELETE /api/env/keys
	req, _ = http.NewRequest("DELETE", testServer.URL+"/api/env/keys?name=FINNHUB_API_KEY", nil)
	req.Header.Set("X-API-Key", "updated-dynamic-api-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP DELETE key error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK on key delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

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

	// Test unauthenticated GET /api/health (should succeed 200)
	healthReq := httptest.NewRequest("GET", "/api/health", nil)
	healthRec := httptest.NewRecorder()
	server.ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK for /api/health endpoint, got %d", healthRec.Code)
	}

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

	// Test POST /api/watchlist/refresh (force refresh endpoint)
	refreshBody, _ := json.Marshal(map[string]any{
		"ticker": "MSFT",
	})
	req = httptest.NewRequest("POST", "/api/watchlist/refresh", bytes.NewBuffer(refreshBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK for force refresh, got %d", rec.Code)
	}

	// Test DELETE /api/watchlist?ticker=MSFT
	req = httptest.NewRequest("DELETE", "/api/watchlist?ticker=MSFT", nil)
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK, got %d", rec.Code)
	}

	// Test GET /api/status
	req = httptest.NewRequest("GET", "/api/status", nil)
	req.Header.Set("X-API-Key", apiKey)
	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK for /api/status, got %d", rec.Code)
	}

	var statusResp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&statusResp); err != nil {
		t.Fatalf("Failed to decode status response: %v", err)
	}

	if _, ok := statusResp["providers"]; !ok {
		t.Errorf("Expected 'providers' in status response")
	}
	if _, ok := statusResp["worker_pool"]; !ok {
		t.Errorf("Expected 'worker_pool' in status response")
	}
	if _, ok := statusResp["job_summary"]; !ok {
		t.Errorf("Expected 'job_summary' in status response")
	}
	if _, ok := statusResp["recent_activity"]; !ok {
		t.Errorf("Expected 'recent_activity' in status response")
	}
}
