package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"finbase/clients"
	"finbase/db"
	"finbase/web"
	"finbase/worker"
)

// SSEEvent represents a Server-Sent Event payload.
type SSEEvent struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
}

type SSELogMessage struct {
	Timestamp time.Time `json:"timestamp"`
	Ticker    string    `json:"ticker"`
	Status    string    `json:"status"`
	Message   string    `json:"message"`
}

// SSEBroker handles client subscriptions and broadcasting SSE events.
type SSEBroker struct {
	clients    map[chan SSEEvent]bool
	newClients chan chan SSEEvent
	defClients chan chan SSEEvent
	broadcast  chan SSEEvent
	mu         sync.Mutex
}

func NewSSEBroker() *SSEBroker {
	broker := &SSEBroker{
		clients:    make(map[chan SSEEvent]bool),
		newClients: make(chan chan SSEEvent),
		defClients: make(chan chan SSEEvent),
		broadcast:  make(chan SSEEvent, 100),
	}
	go broker.listen()
	return broker
}

func (b *SSEBroker) listen() {
	for {
		select {
		case s := <-b.newClients:
			b.mu.Lock()
			b.clients[s] = true
			b.mu.Unlock()
		case s := <-b.defClients:
			b.mu.Lock()
			if _, ok := b.clients[s]; ok {
				delete(b.clients, s)
				close(s)
			}
			b.mu.Unlock()
		case event := <-b.broadcast:
			b.mu.Lock()
			for clientMessageChan := range b.clients {
				select {
				case clientMessageChan <- event:
				default:
					// Drop if slow client buffer full
				}
			}
			b.mu.Unlock()
		}
	}
}

func (b *SSEBroker) BroadcastLog(ticker, status, message string) {
	select {
	case b.broadcast <- SSEEvent{
		Event: "log",
		Data: SSELogMessage{
			Timestamp: time.Now(),
			Ticker:    ticker,
			Status:    status,
			Message:   message,
		},
	}:
	default:
		log.Printf("[SSEBroker] Broadcast buffer full, dropping log for %s", ticker)
	}
}

func (b *SSEBroker) BroadcastUpdate(ticker string, data any) {
	select {
	case b.broadcast <- SSEEvent{
		Event: "company_update",
		Data:  data,
	}:
	default:
		log.Printf("[SSEBroker] Broadcast buffer full, dropping update for %s", ticker)
	}
}

func (b *SSEBroker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	messageChan := make(chan SSEEvent, 10)
	b.newClients <- messageChan

	defer func() {
		b.defClients <- messageChan
	}()

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-messageChan:
			if !ok {
				return
			}
			dataJSON, err := json.Marshal(event.Data)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Event, string(dataJSON))
			flusher.Flush()
		}
	}
}

// Server holds API router dependencies
type Server struct {
	db         *db.DB
	broker     *SSEBroker
	clientMgr  *clients.ClientManager
	workerPool *worker.WorkerPool
	mux        *http.ServeMux
	apiKey     string
	jwtSecret  []byte
}

func NewServer(database *db.DB, broker *SSEBroker, apiKey string, jwtSecret []byte) *Server {
	s := &Server{
		db:        database,
		broker:    broker,
		mux:       http.NewServeMux(),
		apiKey:    apiKey,
		jwtSecret: jwtSecret,
	}
	s.routes()
	return s
}

func (s *Server) SetClientManager(clientMgr *clients.ClientManager) {
	s.clientMgr = clientMgr
}

func (s *Server) SetWorkerPool(workerPool *worker.WorkerPool) {
	s.workerPool = workerPool
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	// Public endpoint to issue short-lived JWT for dashboard
	s.mux.HandleFunc("/api/auth/token", s.handleAuthToken)

	// Protected API routes
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/watchlist", s.handleWatchlist)
	apiMux.HandleFunc("/api/watchlist/refresh", s.handleWatchlistRefresh)
	apiMux.HandleFunc("/api/data/company/", s.handleCompanyData)
	apiMux.HandleFunc("/api/status", s.handleStatus)
	apiMux.Handle("/api/sse", s.broker)

	s.mux.Handle("/api/", AuthMiddleware(s.apiKey, s.jwtSecret, apiMux))

	// Embedded static web dashboard
	fileServer := http.FileServer(http.FS(web.Content))
	s.mux.Handle("/", fileServer)
}

func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	token, err := GenerateJWT(s.jwtSecret, 15*time.Minute)
	if err != nil {
		http.Error(w, `{"error":"failed to generate token"}`, http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]any{
		"token":      token,
		"expires_in": 900,
	})
}

func (s *Server) handleWatchlist(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		list, err := s.db.GetWatchlist(r.Context())
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(list)

	case http.MethodPost:
		var req struct {
			Ticker   string `json:"ticker"`
			Priority int    `json:"priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Ticker == "" {
			http.Error(w, `{"error":"invalid request payload"}`, http.StatusBadRequest)
			return
		}
		item, err := s.db.AddWatchitem(r.Context(), req.Ticker, req.Priority)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(item)

	case http.MethodPut:
		var req struct {
			Ticker   string `json:"ticker"`
			Priority int    `json:"priority"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Ticker == "" {
			http.Error(w, `{"error":"invalid request payload"}`, http.StatusBadRequest)
			return
		}
		if err := s.db.UpdateWatchitemPriority(r.Context(), req.Ticker, req.Priority); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})

	case http.MethodDelete:
		ticker := r.URL.Query().Get("ticker")
		if ticker == "" {
			http.Error(w, `{"error":"ticker parameter required"}`, http.StatusBadRequest)
			return
		}
		if err := s.db.DeleteWatchitem(r.Context(), ticker); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})

	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleWatchlistRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		Ticker string `json:"ticker"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	if req.Ticker == "" {
		req.Ticker = r.URL.Query().Get("ticker")
	}

	if req.Ticker != "" {
		if err := s.db.SetWatchitemForceRefresh(r.Context(), req.Ticker, true); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err := s.db.UpdateWatchitemStatus(r.Context(), req.Ticker, "pending"); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "queued", "ticker": req.Ticker})
	} else {
		if err := s.db.SetWatchitemForceRefreshAll(r.Context(), true); err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "queued", "message": "all watchlist items marked for force refresh"})
	}
}

func (s *Server) handleCompanyData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	ticker := r.URL.Path[len("/api/data/company/"):]
	if ticker == "" {
		http.Error(w, `{"error":"ticker required"}`, http.StatusBadRequest)
		return
	}

	data, err := s.db.GetConsolidatedData(r.Context(), ticker)
	if err != nil {
		log.Printf("Error fetching company data: %v", err)
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(data)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var providerStatuses []clients.APIProviderStatus
	if s.clientMgr != nil {
		providerStatuses = s.clientMgr.GetProviderStatuses()
	} else {
		providerStatuses = []clients.APIProviderStatus{}
	}

	var workerStatus worker.WorkerPoolStatus
	if s.workerPool != nil {
		workerStatus = s.workerPool.GetStatus()
	} else {
		workerStatus = worker.WorkerPoolStatus{CurrentJobs: []worker.ActiveJob{}}
	}

	jobSummary, err := s.db.GetJobStatusSummary(r.Context())
	if err != nil {
		log.Printf("Error fetching job summary: %v", err)
	}

	recentHistory, err := s.db.GetRecentActionHistory(r.Context(), 50)
	if err != nil {
		log.Printf("Error fetching action history: %v", err)
		recentHistory = []db.ActionHistory{}
	}

	json.NewEncoder(w).Encode(map[string]any{
		"providers":        providerStatuses,
		"worker_pool":      workerStatus,
		"job_summary":      jobSummary,
		"recent_activity":  recentHistory,
		"server_time":      time.Now(),
	})
}
