package env

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"finbase/clients"
	"finbase/db"
)

// Supported API key/service names
const (
	KeyAPIKey           = "API_KEY"
	KeySECUserAgent     = "SEC_USER_AGENT"
	KeyFinnhubAPIKey    = "FINNHUB_API_KEY"
	KeyOpenFIGIAPIKey   = "OPENFIGI_API_KEY"
	KeyTiingoAPIKey     = "TIINGO_API_KEY"
	KeyTwelveDataAPIKey = "TWELVE_DATA_API_KEY"
	KeyFMPAPIKey        = "FMP_API_KEY"
	KeyFREDAPIKey       = "FRED_API_KEY"
)

var KnownKeys = []string{
	KeyAPIKey,
	KeySECUserAgent,
	KeyFinnhubAPIKey,
	KeyOpenFIGIAPIKey,
	KeyTiingoAPIKey,
	KeyTwelveDataAPIKey,
	KeyFMPAPIKey,
	KeyFREDAPIKey,
}

type KeyInfo struct {
	Name          string `json:"name"`
	Configured    bool   `json:"configured"`
	Source        string `json:"source,omitempty"` // "env" or "db"
	Functional    *bool  `json:"functional,omitempty"`
	StatusMessage string `json:"status_message,omitempty"`
}

type EnvService struct {
	db        *db.DB
	clientMgr *clients.ClientManager
	mu        sync.RWMutex
	apiKey    string // System main API_KEY used for authentication
}

func NewEnvService(database *db.DB, clientMgr *clients.ClientManager, initialAPIKey string) *EnvService {
	svc := &EnvService{
		db:        database,
		clientMgr: clientMgr,
		apiKey:    initialAPIKey,
	}
	return svc
}

// LoadAndApplyKeys loads keys from environment variables and DB, precedence given to DB overrides.
func (s *EnvService) LoadAndApplyKeys(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Initial values from process env
	envMap := map[string]string{
		KeyAPIKey:           os.Getenv(KeyAPIKey),
		KeySECUserAgent:     os.Getenv(KeySECUserAgent),
		KeyFinnhubAPIKey:    os.Getenv(KeyFinnhubAPIKey),
		KeyOpenFIGIAPIKey:   os.Getenv(KeyOpenFIGIAPIKey),
		KeyTiingoAPIKey:     os.Getenv(KeyTiingoAPIKey),
		KeyTwelveDataAPIKey: os.Getenv(KeyTwelveDataAPIKey),
		KeyFMPAPIKey:        os.Getenv(KeyFMPAPIKey),
		KeyFREDAPIKey:       os.Getenv(KeyFREDAPIKey),
	}

	if envMap[KeyAPIKey] != "" {
		s.apiKey = envMap[KeyAPIKey]
	}

	// 2. Override with values stored in DB
	dbKeys, err := s.db.GetAPIKeysMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to load API keys from DB: %w", err)
	}

	for k, v := range dbKeys {
		if v != "" {
			envMap[k] = v
		}
	}

	// Apply to ClientManager and local state
	if val, ok := envMap[KeyAPIKey]; ok && val != "" {
		s.apiKey = val
	}

	if s.clientMgr != nil {
		if val, ok := envMap[KeySECUserAgent]; ok {
			s.clientMgr.SetSECUserAgent(val)
		}
		if val, ok := envMap[KeyFinnhubAPIKey]; ok {
			s.clientMgr.SetFinnhubAPIKey(val)
		}
		if val, ok := envMap[KeyOpenFIGIAPIKey]; ok {
			s.clientMgr.SetOpenFIGIAPIKey(val)
		}
		if val, ok := envMap[KeyTiingoAPIKey]; ok {
			s.clientMgr.SetTiingoAPIKey(val)
		}
		if val, ok := envMap[KeyTwelveDataAPIKey]; ok {
			s.clientMgr.SetTwelveDataAPIKey(val)
		}
		if val, ok := envMap[KeyFMPAPIKey]; ok {
			s.clientMgr.SetFMPAPIKey(val)
		}
		if val, ok := envMap[KeyFREDAPIKey]; ok {
			s.clientMgr.SetFREDAPIKey(val)
		}
	}

	return nil
}

func (s *EnvService) GetSystemAPIKey() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.apiKey
}

// SetAPIKey sets an API key in the DB and updates the runtime services. Raw keys are NEVER returned.
func (s *EnvService) SetAPIKey(ctx context.Context, name, value string) error {
	name = strings.ToUpper(strings.TrimSpace(name))
	value = strings.TrimSpace(value)

	if name == "" || value == "" {
		return fmt.Errorf("key name and value must not be empty")
	}

	isKnown := false
	for _, k := range KnownKeys {
		if k == name {
			isKnown = true
			break
		}
	}
	if !isKnown {
		return fmt.Errorf("unsupported key name: %s", name)
	}

	if err := s.db.SetAPIKey(ctx, name, value); err != nil {
		return fmt.Errorf("failed to save key in DB: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if name == KeyAPIKey {
		s.apiKey = value
	}

	if s.clientMgr != nil {
		switch name {
		case KeySECUserAgent:
			s.clientMgr.SetSECUserAgent(value)
		case KeyFinnhubAPIKey:
			s.clientMgr.SetFinnhubAPIKey(value)
		case KeyOpenFIGIAPIKey:
			s.clientMgr.SetOpenFIGIAPIKey(value)
		case KeyTiingoAPIKey:
			s.clientMgr.SetTiingoAPIKey(value)
		case KeyTwelveDataAPIKey:
			s.clientMgr.SetTwelveDataAPIKey(value)
		case KeyFMPAPIKey:
			s.clientMgr.SetFMPAPIKey(value)
		case KeyFREDAPIKey:
			s.clientMgr.SetFREDAPIKey(value)
		}
	}

	return nil
}

// DeleteAPIKey removes an API key from DB and resets runtime state (reverting to env var if available).
func (s *EnvService) DeleteAPIKey(ctx context.Context, name string) error {
	name = strings.ToUpper(strings.TrimSpace(name))

	if err := s.db.DeleteAPIKey(ctx, name); err != nil {
		return fmt.Errorf("failed to delete key from DB: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	envVal := os.Getenv(name)

	if name == KeyAPIKey {
		s.apiKey = envVal
	}

	if s.clientMgr != nil {
		switch name {
		case KeySECUserAgent:
			s.clientMgr.SetSECUserAgent(envVal)
		case KeyFinnhubAPIKey:
			s.clientMgr.SetFinnhubAPIKey(envVal)
		case KeyOpenFIGIAPIKey:
			s.clientMgr.SetOpenFIGIAPIKey(envVal)
		case KeyTiingoAPIKey:
			s.clientMgr.SetTiingoAPIKey(envVal)
		case KeyTwelveDataAPIKey:
			s.clientMgr.SetTwelveDataAPIKey(envVal)
		case KeyFMPAPIKey:
			s.clientMgr.SetFMPAPIKey(envVal)
		case KeyFREDAPIKey:
			s.clientMgr.SetFREDAPIKey(envVal)
		}
	}

	return nil
}

// GetKeyStatuses returns key configuration state without exposing key values.
func (s *EnvService) GetKeyStatuses(ctx context.Context) ([]KeyInfo, error) {
	dbKeys, err := s.db.GetAPIKeysMap(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get keys from DB: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	statuses := make([]KeyInfo, 0, len(KnownKeys))

	for _, name := range KnownKeys {
		dbVal, inDB := dbKeys[name]
		envVal := os.Getenv(name)

		info := KeyInfo{
			Name:       name,
			Configured: false,
		}

		if inDB && dbVal != "" {
			info.Configured = true
			info.Source = "db"
		} else if envVal != "" {
			info.Configured = true
			info.Source = "env"
		} else if name == KeyAPIKey && s.apiKey != "" {
			info.Configured = true
			info.Source = "runtime"
		}

		statuses = append(statuses, info)
	}

	return statuses, nil
}

// TestKey tests whether a specific key is functional using ClientManager.
func (s *EnvService) TestKey(ctx context.Context, name string) (bool, string) {
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == KeyAPIKey {
		s.mu.RLock()
		hasKey := s.apiKey != ""
		s.mu.RUnlock()
		if hasKey {
			return true, "System API Key is set"
		}
		return false, "System API Key is not set"
	}

	if s.clientMgr == nil {
		return false, "ClientManager not initialized"
	}

	return s.clientMgr.TestKeyFunctionality(ctx, name)
}
