#!/bin/bash
set -e

export PATH="$PATH:/usr/local/go/bin:/go/bin:$HOME/go/bin"

echo "🚀 Starting automated deployment..."

# 0. Check and Install Prerequisites
check_and_install_prereqs() {
    echo "🔍 Checking prerequisites..."

    # Check/install git, curl, ca-certificates if missing
    local packages_to_install=()
    if ! command -v git >/dev/null 2>&1; then
        packages_to_install+=("git")
    fi
    if ! command -v curl >/dev/null 2>&1; then
        packages_to_install+=("curl")
    fi
    if [ ! -f /etc/ssl/certs/ca-certificates.crt ]; then
        packages_to_install+=("ca-certificates")
    fi

    if [ ${#packages_to_install[@]} -ne 0 ]; then
        echo "📦 Missing basic system tools: ${packages_to_install[*]}."
        if command -v apt-get >/dev/null 2>&1; then
            echo "   Installing via apt-get..."
            sudo apt-get update -y || apt-get update -y
            sudo apt-get install -y "${packages_to_install[@]}" || apt-get install -y "${packages_to_install[@]}"
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y "${packages_to_install[@]}" || yum install -y "${packages_to_install[@]}"
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install -y "${packages_to_install[@]}" || dnf install -y "${packages_to_install[@]}"
        elif command -v apk >/dev/null 2>&1; then
            sudo apk add --no-cache "${packages_to_install[@]}" || apk add --no-cache "${packages_to_install[@]}"
        else
            echo "❌ Package manager not found. Please install ${packages_to_install[*]} manually."
            exit 1
        fi
    fi

    # Check Docker
    if ! command -v docker >/dev/null 2>&1; then
        echo "📦 Docker not found. Attempting to install Docker..."
        if command -v apt-get >/dev/null 2>&1; then
            sudo apt-get update -y || apt-get update -y
            sudo apt-get install -y docker.io || apt-get install -y docker.io
        elif command -v yum >/dev/null 2>&1; then
            sudo yum install -y docker || yum install -y docker
        elif command -v dnf >/dev/null 2>&1; then
            sudo dnf install -y docker || dnf install -y docker
        elif command -v apk >/dev/null 2>&1; then
            sudo apk add --no-cache docker || apk add --no-cache docker
        else
            echo "❌ Package manager not found. Please install docker manually."
            exit 1
        fi
    fi

    # Ensure Docker service is running if systemctl or service is present
    if command -v systemctl >/dev/null 2>&1; then
        sudo systemctl start docker 2>/dev/null || systemctl start docker 2>/dev/null || true
    elif command -v service >/dev/null 2>&1; then
        sudo service docker start 2>/dev/null || service docker start 2>/dev/null || true
    fi

    # Check Go
    if ! command -v go >/dev/null 2>&1; then
        echo "📦 Go not found. Attempting to install Go..."
        GO_VERSION="1.22.5"
        ARCH=$(uname -m)
        case "$ARCH" in
            x86_64) GO_ARCH="amd64" ;;
            aarch64|arm64) GO_ARCH="arm64" ;;
            armv7l) GO_ARCH="armv6l" ;;
            *) GO_ARCH="amd64" ;;
        esac

        GO_TAR="go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
        GO_URL="https://go.dev/dl/${GO_TAR}"

        echo "   Downloading Go ${GO_VERSION} for ${GO_ARCH}..."
        if curl -fsSL "$GO_URL" -o "/tmp/${GO_TAR}"; then
            echo "   Extracting Go to /usr/local..."
            if command -v sudo >/dev/null 2>&1; then
                sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf "/tmp/${GO_TAR}"
            else
                rm -rf /usr/local/go && tar -C /usr/local -xzf "/tmp/${GO_TAR}"
            fi
            rm -f "/tmp/${GO_TAR}"
            export PATH="$PATH:/usr/local/go/bin"
        elif command -v apt-get >/dev/null 2>&1; then
            echo "   Falling back to apt-get golang-go..."
            sudo apt-get update -y || apt-get update -y
            sudo apt-get install -y golang-go || apt-get install -y golang-go
        fi
    fi

    # Final verification of tools
    for tool in git curl docker go; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            echo "❌ Required tool '$tool' is still missing or not in PATH."
            exit 1
        fi
    done

    echo "✅ All prerequisites checked and verified!"
}

check_and_install_prereqs

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
