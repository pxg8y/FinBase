package env

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"finbase/clients"
	"finbase/db"
)

func TestEnvServiceSetGetDelete(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer database.Close()

	clientMgr := clients.NewClientManager("", "", "", "", "", "")
	envSvc := NewEnvService(database, clientMgr, "initial-key")

	ctx := context.Background()

	// 1. Initial statuses
	statuses, err := envSvc.GetKeyStatuses(ctx)
	if err != nil {
		t.Fatalf("GetKeyStatuses error: %v", err)
	}
	if len(statuses) != len(KnownKeys) {
		t.Errorf("Expected %d keys, got %d", len(KnownKeys), len(statuses))
	}

	// Verify initial system API key
	if envSvc.GetSystemAPIKey() != "initial-key" {
		t.Errorf("Expected initial-key, got %s", envSvc.GetSystemAPIKey())
	}

	// 2. Set API_KEY in DB
	err = envSvc.SetAPIKey(ctx, "API_KEY", "new-secret-key")
	if err != nil {
		t.Fatalf("SetAPIKey error: %v", err)
	}
	if envSvc.GetSystemAPIKey() != "new-secret-key" {
		t.Errorf("Expected new-secret-key, got %s", envSvc.GetSystemAPIKey())
	}

	// 3. Set FINNHUB_API_KEY
	err = envSvc.SetAPIKey(ctx, "FINNHUB_API_KEY", "test-finnhub-token")
	if err != nil {
		t.Fatalf("SetAPIKey finnhub error: %v", err)
	}
	if clientMgr.GetFinnhubAPIKey() != "test-finnhub-token" {
		t.Errorf("Expected test-finnhub-token in ClientManager, got %s", clientMgr.GetFinnhubAPIKey())
	}

	// 4. Verify no exposure in GetKeyStatuses
	statuses, err = envSvc.GetKeyStatuses(ctx)
	if err != nil {
		t.Fatalf("GetKeyStatuses error: %v", err)
	}
	for _, status := range statuses {
		if status.Name == "FINNHUB_API_KEY" {
			if !status.Configured {
				t.Errorf("FINNHUB_API_KEY should be marked configured")
			}
			if status.Source != "db" {
				t.Errorf("Expected source 'db', got '%s'", status.Source)
			}
		}
	}

	// 5. Delete API_KEY from DB
	err = envSvc.DeleteAPIKey(ctx, "API_KEY")
	if err != nil {
		t.Fatalf("DeleteAPIKey error: %v", err)
	}
	if envSvc.GetSystemAPIKey() != "" && envSvc.GetSystemAPIKey() != os.Getenv("API_KEY") {
		t.Errorf("Expected API key to revert to env or empty, got %s", envSvc.GetSystemAPIKey())
	}
}

func TestEnvServiceTestKeyMock(t *testing.T) {
	database, err := db.NewDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize DB: %v", err)
	}
	defer database.Close()

	// Mock server for SEC EDGAR
	secMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"cik":320193,"entityName":"Apple Inc.","facts":{}}`))
	}))
	defer secMock.Close()

	clientMgr := clients.NewClientManager("TestApp test@example.com", "", "", "", "", "")
	envSvc := NewEnvService(database, clientMgr, "")

	ctx := context.Background()

	// Test unconfigured key
	ok, msg := envSvc.TestKey(ctx, "FINNHUB_API_KEY")
	if ok {
		t.Errorf("Expected false for unconfigured Finnhub key, got true (%s)", msg)
	}

	// Test invalid key name
	ok, _ = envSvc.TestKey(ctx, "NON_EXISTENT_KEY")
	if ok {
		t.Errorf("Expected false for unknown key, got true")
	}
}
