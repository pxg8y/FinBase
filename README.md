# FinBase



Technical Blueprint: Financial Data Aggregation & Analysis Engine (FDAAE)
This document provides the strict technical specifications for building a high-performance, low-RAM Docker container for financial data aggregation. It is designed to be fed directly to an AI code generator.
1. Architecture & Tech Stack
 * Language: Go (Golang) compiled as a statically linked binary.
 * Database: Embedded SQLite. Use the pure Go driver modernc.org/sqlite (no CGO required).
 * Frontend: Vanilla HTML/JS/CSS embedded directly into the Go binary using //go:embed. No separate web server (like Nginx) or Node.js required for the application itself.
 * Deployment: Docker multi-stage build resulting in a scratch base image.
2. Database Design & Tuning (SQLite)
To handle concurrent API queries and background workers without SQLITE_BUSY locks, the database initialization must execute these specific PRAGMAs:
 * PRAGMA journal_mode = WAL;
 * PRAGMA synchronous = NORMAL;
 * PRAGMA busy_timeout = 5000;
Connection Pooling Strategy:
You must implement two strictly separated database/sql connection pools:
 * Read Pool: SetMaxOpenConns(100) for fast concurrent dashboard/API reads.
 * Write Pool: SetMaxOpenConns(1) with BEGIN IMMEDIATE for background workers to guarantee single-writer serialization.
Schema (Core Tables):
| Table | Primary Key | Key Columns |
|---|---|---|
| watchlist | id | ticker (UNIQUE), priority (INT), status, last_updated |
| companies | id | ticker (UNIQUE), cik, isin, name, sector |
| market_data | id | company_id (FK), timestamp, current_price, volume |
| fundamentals | id | company_id (FK), period, metric_name, value |
| action_history | id | timestamp, ticker, action_type, status, message |
3. Data Sources & Rate Limiting
The system orchestrates free data sources. Strict rate limiting per API is mandatory to avoid IP bans.
 * OpenFIGI API (Identifier Mapping):
   * Purpose: Maps tickers to SEC CIKs.
   * Limit: 25 requests per 6 seconds with a free API key.
 * SEC EDGAR API (data.sec.gov):
   * Purpose: Fundamental data (XBRL financial facts).
   * Limit: 10 requests per second maximum.
   * Limiter Pattern: Use a Leaky Bucket (go.uber.org/ratelimit) to enforce hard intervals between requests.
   * Auth: Requires a custom User-Agent header formatted as App_Name User_Email.
   * Data Formatting: CIKs must be padded with leading zeros to exactly 10 digits.
 * Finnhub API:
   * Purpose: Real-time prices and basic profiles.
   * Limit: 60 requests per minute.
   * Limiter Pattern: Use a Token Bucket (golang.org/x/time/rate) to allow bursts.
   * Auth: Key sent in the X-Finnhub-Token header.
4. Concurrency Model (Worker Pool)
 * Job Dispatcher: A routine that queries the watchlist table ordered by priority DESC, last_updated ASC and pushes jobs to a Go channel.
 * Worker Pool: A fixed number of worker goroutines (e.g., matching CPU cores) read from the channel. They execute the API fetch logic, utilizing the respective rate limiters and circuit breakers for external calls.
5. API & Real-Time Dashboard (SSE)
 * Internal REST API (JSON):
   * POST/PUT/DELETE/GET /api/watchlist: Manage tickers and priority.
   * GET /api/data/company/{ticker}: Retrieve structured, consolidated data (no raw dumps).
 * Dashboard Streaming:
   * Use Server-Sent Events (SSE) on /api/sse instead of WebSockets. SSE is unidirectional, memory-efficient, and natively supported by browsers via EventSource.
   * Implementation: Set headers Content-Type: text/event-stream, Cache-Control: no-cache, Connection: keep-alive and use Go's http.Flusher to push live worker logs and data updates.
6. Memory Optimization & Dockerization
 * RAM Limits: Utilize the GOMEMLIMIT environment variable (e.g., GOMEMLIMIT=200MiB) to force aggressive garbage collection before OOM kills occur.
 * Allocations: Use sync.Pool for heavy JSON parsing buffers to reuse memory and prevent heap allocation spikes.
 * Docker Container:
   * Stage 1: Build with CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s".
   * Stage 2: FROM scratch. Crucially, copy /etc/ssl/certs/ca-certificates.crt from the builder to enable HTTPS requests.
 * Configuration: All variables must be injected via a .env file (SEC_USER_AGENT, FINNHUB_API_KEY, OPENFIGI_API_KEY, DB_PATH, GOMEMLIMIT).
 * Persistence: Mount a Docker Volume for DB_PATH to ensure SQLite data survives container restarts.
7. Automated Container Maintenance & Deployment
To eliminate external dependencies (such as external reverse proxies), deployment uses direct Docker container management with isolated pre-flight testing.
The deployment script pulls code, builds the new Docker image, starts a temporary test container on port 9001 to run health checks, swaps out the production container running on port 9000 (`finbase`), and performs general cleanup of unused images.

deploy.sh (Implementation):
#!/bin/bash
set -e

echo "🚀 Starting automated deployment..."

# 1. Git Pull (force overwrite local changes)
echo "📦 Pulling latest changes from Git..."
git fetch --all
git reset --hard origin/main

# 2. Build the Docker Image
IMAGE_NAME="finbase:latest"
echo "🔨 Building Docker image $IMAGE_NAME..."
docker build -t "$IMAGE_NAME" .

# 3. Test New Image in Temporary Container
TEST_CONTAINER="finbase-test"
TEST_PORT=9001

echo "🌱 Starting temporary test container $TEST_CONTAINER on port $TEST_PORT..."
docker stop "$TEST_CONTAINER" 2>/dev/null || true
docker rm "$TEST_CONTAINER" 2>/dev/null || true

docker run -d \
    --name "$TEST_CONTAINER" \
    --env-file .env \
    -e PORT=9000 \
    -v finbase_data:/app/data \
    -p $TEST_PORT:9000 \
    "$IMAGE_NAME"

# 4. Health Check Wait Loop on Test Container
echo "⏳ Waiting for test container to become healthy on port $TEST_PORT..."
MAX_RETRIES=10
RETRY_COUNT=0
HEALTHY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -sf http://localhost:$TEST_PORT/api/watchlist > /dev/null; then
        HEALTHY=true
        break
    fi
    echo "   ...waiting 3 seconds..."
    sleep 3
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ "$HEALTHY" = false ]; then
    echo "❌ Health check failed on test container. Rolling back."
    docker stop "$TEST_CONTAINER" 2>/dev/null || true
    docker rm "$TEST_CONTAINER" 2>/dev/null || true
    exit 1
fi

echo "✅ Test container is healthy!"

# 5. Stop and Remove Test Container
echo "🧹 Removing test container..."
docker stop "$TEST_CONTAINER"
docker rm "$TEST_CONTAINER"

# 6. Swap Production Container
PROD_CONTAINER="finbase"
PROD_PORT=9000

if [ "$(docker ps -aq -f name=^/${PROD_CONTAINER}$)" ]; then
    echo "🛑 Stopping and removing existing production container $PROD_CONTAINER..."
    docker stop "$PROD_CONTAINER" 2>/dev/null || true
    docker rm "$PROD_CONTAINER" 2>/dev/null || true
fi

echo "🚀 Starting production container $PROD_CONTAINER on port $PROD_PORT..."
docker run -d \
    --name "$PROD_CONTAINER" \
    --env-file .env \
    -e PORT=$PROD_PORT \
    -v finbase_data:/app/data \
    -p $PROD_PORT:$PROD_PORT \
    "$IMAGE_NAME"

# 7. Final Health Check on Production Container
echo "⏳ Verifying production container health on port $PROD_PORT..."
RETRY_COUNT=0
HEALTHY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -sf http://localhost:$PROD_PORT/api/watchlist > /dev/null; then
        HEALTHY=true
        break
    fi
    echo "   ...waiting 3 seconds..."
    sleep 3
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ "$HEALTHY" = false ]; then
    echo "❌ Production health check failed!"
    exit 1
fi

echo "✅ Production container is healthy!"

# 8. Cleanup Unused Docker Images
echo "🧹 Performing Docker image cleanup..."
docker image prune -f

echo "🎉 Deployment completed successfully!"

(Note for the AI code generator: The deploy.sh script requires execution permissions chmod +x deploy.sh. The Go application must be designed to execute any required database/sql schema migrations immediately upon startup before binding to the HTTP port.)
