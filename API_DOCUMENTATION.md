# FinBase API Documentation

This document provides complete documentation for all internal API endpoints provided by the FinBase (FDAAE) application.

---

## Base URL
All endpoints are relative to the root server URL (e.g., `http://localhost:8080`).

---

## Authentication
All `/api/*` endpoints require authentication, except for `/api/auth/token` which issues dashboard session tokens.

 Authentication can be provided via two mechanisms:
 1. **API Key Authentication** (External clients):
    - Header: `X-API-Key: <YOUR_API_KEY>`
    - Header: `Authorization: Bearer <YOUR_API_KEY>`
    - Query Parameter: `?api_key=<YOUR_API_KEY>`
 2. **Short-lived JWT Authentication** (Web Dashboard / Browser):
    - Token generation endpoint: `POST /api/auth/token` (returns a signed 15-minute JWT)
    - Header: `Authorization: Bearer <JWT_TOKEN>`
    - Query Parameter: `?token=<JWT_TOKEN>` (used for Server-Sent Events `/api/sse`)

If no valid authentication is provided, API endpoints respond with `401 Unauthorized`.

---

## Error Handling
All error responses return a standard JSON object containing an `error` message string:
```json
{
  "error": "Error description"
}
```

---

## Endpoints Summary

| Endpoint | Method | Description |
|---|---|---|
| `/api/watchlist` | `GET` | Retrieve all items in the watchlist, ordered by priority (descending) then ID (ascending). |
| `/api/watchlist` | `POST` | Add a new ticker to the watchlist or update priority if it already exists. |
| `/api/watchlist` | `PUT` | Update the priority of an existing ticker in the watchlist. |
| `/api/watchlist` | `DELETE` | Remove a ticker from the watchlist. |
| `/api/data/company/{ticker}` | `GET` | Retrieve consolidated company profile, market data, financial fundamentals, valuation ratios, corporate actions, historical prices, analyst estimates, earnings calendar, news, insider transactions, institutional ownership, macro indicators, and action logs. |
| `/api/sse` | `GET` | Real-time Server-Sent Events (SSE) stream for worker logs and live company updates. |
| `/` | `GET` | Serve the embedded static web dashboard (HTML/CSS/JS). |

---

## Detailed Endpoint Documentation

### 1. Watchlist Management

#### GET `/api/watchlist`
Retrieves the list of all tracked stock tickers in the watchlist.

* **Method:** `GET`
* **Response Headers:**
  * `Content-Type: application/json`
* **Query Parameters:** None
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Watchlist retrieved successfully. Returns `[]` if empty.
  * `500 Internal Server Error`: Database query failure.
* **Response Body (`application/json`):**
```json
[
  {
    "id": 1,
    "ticker": "AAPL",
    "priority": 10,
    "status": "completed",
    "last_updated": "2026-08-31T17:08:04Z"
  },
  {
    "id": 2,
    "ticker": "MSFT",
    "priority": 5,
    "status": "pending",
    "last_updated": "2026-08-31T17:00:00Z"
  }
]
```

---

#### POST `/api/watchlist`
Adds a ticker to the watchlist with a specified priority. If the ticker already exists, its priority is updated and its status is reset to `pending`. Ticker symbols are trimmed and converted to uppercase automatically.

* **Method:** `POST`
* **Request Headers:**
  * `Content-Type: application/json`
* **Response Headers:**
  * `Content-Type: application/json`
* **Request Body (`application/json`):**
```json
{
  "ticker": "AAPL",
  "priority": 10
}
```
* **Response Status Codes:**
  * `201 Created`: Ticker successfully added or updated in watchlist.
  * `400 Bad Request`: Invalid request payload or empty `ticker`.
    * Response: `{"error":"invalid request payload"}`
  * `500 Internal Server Error`: Database transaction error.
    * Response: `{"error":"<error message>"}`
* **Response Body (`application/json`):**
```json
{
  "id": 1,
  "ticker": "AAPL",
  "priority": 10,
  "status": "pending",
  "last_updated": "2026-08-31T17:08:04Z"
}
```

---

#### PUT `/api/watchlist`
Updates the priority of an existing ticker in the watchlist and resets its status to `pending`.

* **Method:** `PUT`
* **Request Headers:**
  * `Content-Type: application/json`
* **Response Headers:**
  * `Content-Type: application/json`
* **Request Body (`application/json`):**
```json
{
  "ticker": "AAPL",
  "priority": 15
}
```
* **Response Status Codes:**
  * `200 OK`: Watchlist item priority updated successfully.
  * `400 Bad Request`: Invalid request payload or empty `ticker`.
    * Response: `{"error":"invalid request payload"}`
  * `500 Internal Server Error`: Database update error.
    * Response: `{"error":"<error message>"}`
* **Response Body (`application/json`):**
```json
{
  "status": "success"
}
```

---

#### DELETE `/api/watchlist`
Removes a ticker from the watchlist.

* **Method:** `DELETE`
* **Query Parameters:**
  * `ticker` (required, string): Ticker symbol to remove (e.g., `?ticker=AAPL`).
* **Response Headers:**
  * `Content-Type: application/json`
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Ticker deleted from watchlist.
  * `400 Bad Request`: Missing required `ticker` query parameter.
    * Response: `{"error":"ticker parameter required"}`
  * `500 Internal Server Error`: Database deletion error.
    * Response: `{"error":"<error message>"}`
* **Response Body (`application/json`):**
```json
{
  "status": "deleted"
}
```

---

#### Unsupported Methods on `/api/watchlist`
Calling `/api/watchlist` with HTTP methods other than `GET`, `POST`, `PUT`, or `DELETE` (e.g., `PATCH`) will return:
* **Response Status Code:** `405 Method Not Allowed`
* **Response Body (`application/json`):**
```json
{
  "error": "method not allowed"
}
```

---

### 2. Consolidated Company Data

#### GET `/api/data/company/{ticker}`
Retrieves consolidated company details including basic company metadata, watchlist status, recent market data, fundamentals (XBRL facts), valuation ratios, corporate actions (dividends and stock splits), historical price time series, analyst estimates, earnings calendar events, company news with sentiment scores, insider transactions, institutional ownership holdings, macroeconomic indicators, and recent background worker action logs.

* **Method:** `GET`
* **Response Headers:**
  * `Content-Type: application/json`
* **URL Parameters:**
  * `{ticker}` (required, string): Stock ticker symbol (e.g., `AAPL`). Case-insensitive.
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Consolidated data retrieved successfully. (Note: If the requested ticker is not present in the database, HTTP 200 is returned with `company.ticker` set to the uppercase ticker parameter and empty arrays for all collection fields).
  * `400 Bad Request`: Ticker parameter omitted in path (`/api/data/company/`).
    * Response: `{"error":"ticker required"}`
  * `405 Method Not Allowed`: HTTP method is not `GET`.
    * Response: `{"error":"method not allowed"}`
  * `500 Internal Server Error`: Database read failure.
    * Response: `{"error":"<error message>"}`
* **Response Body (`application/json`):**
```json
{
  "company": {
    "id": 1,
    "ticker": "AAPL",
    "cik": "0000320193",
    "isin": "",
    "name": "Apple Inc.",
    "sector": "Technology",
    "exchange": "NASDAQ",
    "outstanding_shares": 15400000000,
    "logo_url": "https://static2.finnhub.io/logo/AAPL.png"
  },
  "watchlist": {
    "id": 1,
    "ticker": "AAPL",
    "priority": 10,
    "status": "completed",
    "last_updated": "2026-08-31T17:08:04Z"
  },
  "market_data": [
    {
      "id": 10,
      "company_id": 1,
      "timestamp": "2026-08-31T17:08:04Z",
      "current_price": 185.50,
      "volume": 45000000,
      "open_price": 184.00,
      "high_price": 186.20,
      "low_price": 183.80,
      "previous_close": 183.50,
      "market_cap": 2856700000000,
      "fifty_two_week_high": 199.62,
      "fifty_two_week_low": 164.08
    }
  ],
  "fundamentals": [
    {
      "id": 100,
      "company_id": 1,
      "period": "2023-FY",
      "metric_name": "Revenues",
      "value": 383285000000
    },
    {
      "id": 101,
      "company_id": 1,
      "period": "2023-FY",
      "metric_name": "NetIncome",
      "value": 96995000000
    }
  ],
  "valuation_ratios": [
    {
      "id": 1,
      "company_id": 1,
      "pe_ratio": 29.5,
      "pb_ratio": 45.2,
      "ps_ratio": 7.4,
      "gross_margin": 0.441,
      "operating_margin": 0.298,
      "net_margin": 0.253,
      "roe": 1.56,
      "roa": 0.28,
      "debt_to_equity": 1.8,
      "timestamp": "2026-08-31T17:08:04Z"
    }
  ],
  "dividends": [
    {
      "id": 1,
      "company_id": 1,
      "ex_date": "2024-02-09",
      "payment_date": "2024-02-15",
      "record_date": "2024-02-12",
      "amount": 0.24,
      "currency": "USD",
      "frequency": 4
    }
  ],
  "stock_splits": [
    {
      "id": 1,
      "company_id": 1,
      "execution_date": "2020-08-31",
      "from_factor": 1,
      "to_factor": 4
    }
  ],
  "historical_prices": [
    {
      "id": 1,
      "company_id": 1,
      "date": "2026-08-30",
      "open_price": 184.00,
      "high_price": 186.20,
      "low_price": 183.80,
      "close_price": 185.50,
      "adj_close_price": 185.50,
      "volume": 45000000
    }
  ],
  "analyst_estimates": [
    {
      "id": 1,
      "company_id": 1,
      "period": "2024-Q1",
      "strong_buy": 12,
      "buy": 18,
      "hold": 6,
      "sell": 1,
      "strong_sell": 0
    }
  ],
  "earnings_calendar": [
    {
      "id": 1,
      "company_id": 1,
      "date": "2024-02-01",
      "quarter": 1,
      "year": 2024,
      "eps_estimate": 2.10,
      "eps_actual": 2.18,
      "revenue_estimate": 117900000000,
      "revenue_actual": 119580000000
    }
  ],
  "company_news": [
    {
      "id": 1,
      "company_id": 1,
      "news_id": 123456,
      "headline": "Apple Announces Q1 Earnings Results",
      "summary": "Apple reported quarterly revenue of $119.6 billion...",
      "source": "Reuters",
      "url": "https://example.com/news/123456",
      "published_at": "2026-08-31T12:00:00Z",
      "sentiment_score": 0.75
    }
  ],
  "insider_transactions": [
    {
      "id": 1,
      "company_id": 1,
      "name": "COOK TIMOTHY D",
      "share_count": 3280000,
      "change_shares": -51100,
      "filing_date": "2024-01-15",
      "transaction_code": "S",
      "transaction_price": 185.00
    }
  ],
  "institutional_ownership": [
    {
      "id": 1,
      "company_id": 1,
      "investor_name": "Vanguard Group Inc",
      "shares_held": 1300000000,
      "change_shares": 5000000,
      "value": 241150000000,
      "period": "2023-Q4"
    }
  ],
  "macro_indicators": [
    {
      "id": 1,
      "series_id": "DGS10",
      "indicator_name": "10-Year Treasury Constant Maturity Rate",
      "date": "2026-08-30",
      "value": 4.25
    }
  ],
  "history": [
    {
      "id": 50,
      "timestamp": "2026-08-31T17:08:04Z",
      "ticker": "AAPL",
      "action_type": "JOB_COMPLETE",
      "status": "SUCCESS",
      "message": "Finished processing job for AAPL"
    }
  ]
}
```

---

### 3. Server-Sent Events (SSE)

#### GET `/api/sse`
Establishes a unidirectional streaming HTTP connection for real-time updates. Pushes background worker log events and updated consolidated company data objects.

* **Method:** `GET`
* **Response Headers:**
  * `Content-Type: text/event-stream`
  * `Cache-Control: no-cache`
  * `Connection: keep-alive`
  * `Access-Control-Allow-Origin: *`
* **Response Status Codes:**
  * `200 OK`: SSE streaming connection established.
  * `400 Bad Request`: HTTP response writer does not support streaming (`http.Flusher`). Response plain text: `Streaming unsupported!`.
* **Event Stream Formats:**

1. **`log` Event:**
```text
event: log
data: {"timestamp":"2026-08-31T17:08:04Z","ticker":"AAPL","status":"PROCESSING","message":"Starting processing for ticker AAPL"}
```

2. **`company_update` Event:**
```text
event: company_update
data: {"company":{"id":1,"ticker":"AAPL","cik":"0000320193",...},"market_data":[...],"fundamentals":[...],"valuation_ratios":[...],"dividends":[...],"stock_splits":[...],"historical_prices":[...],"analyst_estimates":[...],"earnings_calendar":[...],"company_news":[...],"insider_transactions":[...],"institutional_ownership":[...],"macro_indicators":[...],"history":[...]}
```

---

### 4. Embedded Web Dashboard

#### GET `/`
Serves the embedded static web dashboard (HTML/CSS/JS) embedded into the binary.

* **Method:** `GET`
* **Query Parameters:** None
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Static asset served successfully.
