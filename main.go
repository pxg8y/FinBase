package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"finbase/api"
	"finbase/clients"
	"finbase/db"
	"finbase/env"
	"finbase/worker"
)

func main() {
	healthCheck := flag.Bool("healthcheck", false, "Run internal health check")
	flag.Parse()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if *healthCheck {
		client := http.Client{Timeout: 5 * time.Second}
		url := fmt.Sprintf("http://localhost:%s/api/health", port)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("Health check failed: %v\n", err)
			os.Exit(1)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Health check failed with status code: %d\n", resp.StatusCode)
			os.Exit(1)
		}
		fmt.Println("Health check passed.")
		os.Exit(0)
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "/app/data/finbase.db"
	}

	secUserAgent := os.Getenv("SEC_USER_AGENT")
	finnhubAPIKey := os.Getenv("FINNHUB_API_KEY")
	openFIGIAPIKey := os.Getenv("OPENFIGI_API_KEY")
	tiingoAPIKey := os.Getenv("TIINGO_API_KEY")
	twelveDataAPIKey := os.Getenv("TWELVE_DATA_API_KEY")
	fmpAPIKey := os.Getenv("FMP_API_KEY")
	fredAPIKey := os.Getenv("FRED_API_KEY")

	log.Printf("Initializing FinBase FDAAE Engine...")
	database, err := db.NewDB(dbPath)
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer database.Close()

	clientMgr := clients.NewClientManager(secUserAgent, finnhubAPIKey, openFIGIAPIKey, tiingoAPIKey, twelveDataAPIKey, fmpAPIKey)
	clientMgr.SetFREDAPIKey(fredAPIKey)

	apiKey := os.Getenv("API_KEY")
	envSvc := env.NewEnvService(database, clientMgr, apiKey)
	if err := envSvc.LoadAndApplyKeys(context.Background()); err != nil {
		log.Printf("Warning: failed loading API keys from DB: %v", err)
	}

	broker := api.NewSSEBroker()

	wp := worker.NewWorkerPool(database, clientMgr, broker, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wp.Start(ctx)

	jwtSecretStr := os.Getenv("JWT_SECRET")
	var jwtSecret []byte
	if jwtSecretStr != "" {
		jwtSecret = []byte(jwtSecretStr)
	} else {
		jwtSecret = []byte("finbase-default-jwt-secret-change-in-production")
	}

	server := api.NewServer(database, broker, apiKey, jwtSecret)
	server.SetEnvService(envSvc)
	server.SetClientManager(clientMgr)
	server.SetWorkerPool(wp)

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: server,
	}

	go func() {
		log.Printf("Server listening on port %s", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("Shutting down FinBase server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	wp.Stop()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	fmt.Println("FinBase server stopped gracefully.")
}
