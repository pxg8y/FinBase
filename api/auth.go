package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type JWTClaims struct {
	Sub string `json:"sub"`
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
}

func base64URLEncode(data []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(data), "=")
}

func base64URLDecode(s string) ([]byte, error) {
	if l := len(s) % 4; l > 0 {
		s += strings.Repeat("=", 4-l)
	}
	return base64.URLEncoding.DecodeString(s)
}

// GenerateJWT creates a signed short-lived JWT token for dashboard sessions.
func GenerateJWT(secret []byte, duration time.Duration) (string, error) {
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	encodedHeader := base64URLEncode(headerJSON)

	now := time.Now()
	claims := JWTClaims{
		Sub: "dashboard",
		Iat: now.Unix(),
		Exp: now.Add(duration).Unix(),
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encodedClaims := base64URLEncode(claimsJSON)

	unsignedToken := encodedHeader + "." + encodedClaims
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsignedToken))
	signature := base64URLEncode(mac.Sum(nil))

	return unsignedToken + "." + signature, nil
}

// ValidateJWT verifies the signature and expiration of a JWT token.
func ValidateJWT(tokenStr string, secret []byte) bool {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return false
	}

	unsignedToken := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(unsignedToken))
	expectedSignature := base64URLEncode(mac.Sum(nil))

	if !hmac.Equal([]byte(parts[2]), []byte(expectedSignature)) {
		return false
	}

	claimsBytes, err := base64URLDecode(parts[1])
	if err != nil {
		return false
	}

	var claims JWTClaims
	if err := json.Unmarshal(claimsBytes, &claims); err != nil {
		return false
	}

	if time.Now().Unix() > claims.Exp {
		return false
	}

	return true
}

// AuthMiddleware validates either API Key (external) or short-lived JWT (dashboard).
func AuthMiddleware(apiKeyProvider func() string, jwtSecret []byte, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		currentAPIKey := ""
		if apiKeyProvider != nil {
			currentAPIKey = apiKeyProvider()
		}

		// 1. Check query parameter `token` or `api_key` (e.g. for SSE browser connections)
		tokenQuery := r.URL.Query().Get("token")
		if tokenQuery != "" {
			if ValidateJWT(tokenQuery, jwtSecret) || (currentAPIKey != "" && tokenQuery == currentAPIKey) {
				next.ServeHTTP(w, r)
				return
			}
		}

		apiKeyQuery := r.URL.Query().Get("api_key")
		if apiKeyQuery != "" && currentAPIKey != "" && apiKeyQuery == currentAPIKey {
			next.ServeHTTP(w, r)
			return
		}

		// 2. Check X-API-Key header
		headerKey := r.Header.Get("X-API-Key")
		if headerKey != "" && currentAPIKey != "" && headerKey == currentAPIKey {
			next.ServeHTTP(w, r)
			return
		}

		// 3. Check Authorization header (Bearer token or Bearer API Key)
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if (currentAPIKey != "" && token == currentAPIKey) || ValidateJWT(token, jwtSecret) {
				next.ServeHTTP(w, r)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"unauthorized"}`))
	})
}
