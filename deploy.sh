#!/bin/bash
set -e

echo "🚀 Starting automated Blue/Green deployment..."

# 1. Git Pull (force overwrite local changes)
echo "📦 Pulling latest changes from Git..."
git fetch --all
git reset --hard origin/main

# 2. Identify Current Environment
if [ "$(docker ps -q -f name=fdaae-blue)" ]; then
    CURRENT_ENV="blue"
    NEW_ENV="green"
    NEW_PORT=8082
else
    CURRENT_ENV="green"
    NEW_ENV="blue"
    NEW_PORT=8081
fi

echo "🔄 Current active environment: $CURRENT_ENV. Deploying to $NEW_ENV on port $NEW_PORT..."

# 3. Build the new Docker Image
echo "🔨 Building new Docker image..."
docker build -t fdaae-app:latest .

# 4. Start the New Container (Database migrations happen automatically on Go app startup)
echo "🌱 Starting $NEW_ENV container..."
docker run -d \
    --name fdaae-$NEW_ENV \
    --env-file .env \
    -e PORT=$NEW_PORT \
    -v fdaae_data:/app/data \
    -p $NEW_PORT:$NEW_PORT \
    fdaae-app:latest

# 5. Health Check Wait Loop
echo "⏳ Waiting for $NEW_ENV container to become healthy..."
MAX_RETRIES=10
RETRY_COUNT=0
HEALTHY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    # Assuming the API has a basic /api/watchlist or /ping endpoint
    if curl -sf http://localhost:$NEW_PORT/api/watchlist > /dev/null; then
        HEALTHY=true
        break
    fi
    echo "   ...waiting 3 seconds..."
    sleep 3
    RETRY_COUNT=$((RETRY_COUNT+1))
done

if [ "$HEALTHY" = false ]; then
    echo "❌ Health check failed. Rolling back $NEW_ENV deployment."
    docker stop fdaae-$NEW_ENV
    docker rm fdaae-$NEW_ENV
    exit 1
fi

echo "✅ $NEW_ENV container is healthy!"

# 6. Swap Traffic via Reverse Proxy
# (Example assumes a local Nginx configuration file routing to upstream)
echo "🔀 Swapping traffic to $NEW_ENV..."
sed -i "s/server localhost:[0-9]*/server localhost:$NEW_PORT/" /etc/nginx/conf.d/fdaae.conf
nginx -s reload

# 7. Teardown Old Container
if [ "$CURRENT_ENV" != "$NEW_ENV" ] && [ "$(docker ps -q -f name=fdaae-$CURRENT_ENV)" ]; then
    echo "🛑 Stopping and removing old $CURRENT_ENV container..."
    docker stop fdaae-$CURRENT_ENV
    docker rm fdaae-$CURRENT_ENV
fi

echo "🎉 Zero-downtime deployment completed successfully!"
