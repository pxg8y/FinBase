#!/bin/bash
set -e

echo "🚀 Starting automated deployment..."

# 1. Git Pull (force overwrite local changes unless SKIP_GIT_PULL is set)
echo "📦 Pulling latest changes from Git..."
if [ "${SKIP_GIT_PULL}" != "true" ]; then
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git fetch --all 2>/dev/null || true
        git reset --hard origin/main 2>/dev/null || true
    fi
fi

# 2. Build Binary & Docker Image
IMAGE_NAME="finbase:latest"
echo "🔨 Compiling static Go binary and preparing CA certificates..."
CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o fdaae main.go

if [ -f /etc/ssl/certs/ca-certificates.crt ]; then
    cp /etc/ssl/certs/ca-certificates.crt ./ca-certificates.crt
elif [ -f /etc/pki/tls/certs/ca-bundle.crt ]; then
    cp /etc/pki/tls/certs/ca-bundle.crt ./ca-certificates.crt
elif [ -f /etc/ssl/ca-bundle.pem ]; then
    cp /etc/ssl/ca-bundle.pem ./ca-certificates.crt
elif [ -f /etc/ssl/cert.pem ]; then
    cp /etc/ssl/cert.pem ./ca-certificates.crt
else
    echo "📥 Downloading Mozilla CA certificates bundle..."
    curl -sf https://curl.se/ca/cacert.pem -o ./ca-certificates.crt || touch ./ca-certificates.crt
fi

echo "🔨 Building Docker image $IMAGE_NAME..."
docker build -t "$IMAGE_NAME" .
rm -f ./ca-certificates.crt ./fdaae

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
    if curl -sf http://localhost:$TEST_PORT/api/health > /dev/null; then
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
    if curl -sf http://localhost:$PROD_PORT/api/health > /dev/null; then
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
