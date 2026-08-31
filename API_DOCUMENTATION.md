# FinBase API Documentation

This document provides complete documentation for all internal API endpoints provided by the FinBase (FDAAE) application.

---

## Base URL
All endpoints are relative to the root server URL (e.g., `http://localhost:8080`).

---

## Endpoints Summary

| Endpoint | Method | Description |
|---|---|---|
| `/api/watchlist` | `GET` | Retrieve all items in the watchlist, ordered by priority (descending) then ID (ascending). |
| `/api/watchlist` | `POST` | Add a new ticker to the watchlist or update priority if it already exists. |
| `/api/watchlist` | `PUT` | Update the priority of an existing ticker in the watchlist. |
| `/api/watchlist` | `DELETE` | Remove a ticker from the watchlist. |
| `/api/data/company/{ticker}` | `GET` | Retrieve consolidated company profile, market data, fundamental financial facts, and action logs. |
| `/api/sse` | `GET` | Real-time Server-Sent Events (SSE) stream for worker logs and live company updates. |
| `/` | `GET` | Serve the embedded static web dashboard (HTML/CSS/JS). |

---

## Detailed Endpoint Documentation

### 1. Watchlist Management

#### GET `/api/watchlist`
Retrieves the list of all tracked stock tickers in the watchlist.

* **Method:** `GET`
* **Request Headers:** None
* **Query Parameters:** None
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Watchlist retrieved successfully.
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
Adds a ticker to the watchlist with a specified priority. If the ticker already exists, its priority is updated and its status is reset to `pending`.

* **Method:** `POST`
* **Request Headers:**
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
  * `400 Bad Request`: Missing payload or empty `ticker`.
  * `500 Internal Server Error`: Database transaction error.
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
* **Request Body (`application/json`):**
```json
{
  "ticker": "AAPL",
  "priority": 15
}
```
* **Response Status Codes:**
  * `200 OK`: Watchlist item priority updated successfully.
  * `400 Bad Request`: Missing payload or empty `ticker`.
  * `500 Internal Server Error`: Database update error.
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
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Ticker deleted from watchlist.
  * `400 Bad Request`: Missing required `ticker` query parameter.
  * `500 Internal Server Error`: Database deletion error.
* **Response Body (`application/json`):**
```json
{
  "status": "deleted"
}
```

---

### 2. Consolidated Company Data

#### GET `/api/data/company/{ticker}`
Retrieves consolidated company details including basic company metadata, watchlist status, recent market data, fundamentals (XBRL facts), and recent background worker action logs.

* **Method:** `GET`
* **URL Parameters:**
  * `{ticker}` (required, string): Stock ticker symbol (e.g., `AAPL`). Case-insensitive.
* **Request Body:** None
* **Response Status Codes:**
  * `200 OK`: Consolidated data retrieved successfully.
  * `400 Bad Request`: Ticker parameter omitted.
  * `405 Method Not Allowed`: HTTP method is not `GET`.
  * `500 Internal Server Error`: Database read failure.
* **Response Body (`application/json`):**
```json
{
  "company": {
    "id": 1,
    "ticker": "AAPL",
    "cik": "0000320193",
    "isin": "",
    "name": "Apple Inc.",
    "sector": "Technology"
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
      "volume": 0
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
* **Headers Received:**
  * `Content-Type: text/event-stream`
  * `Cache-Control: no-cache`
  * `Connection: keep-alive`
  * `Access-Control-Allow-Origin: *`
* **Event Stream Formats:**

1. **`log` Event:**
```text
event: log
data: {"timestamp":"2026-08-31T17:08:04Z","ticker":"AAPL","status":"PROCESSING","message":"Starting processing for ticker AAPL"}
```

2. **`company_update` Event:**
```text
event: company_update
data: {"company":{"id":1,"ticker":"AAPL","cik":"0000320193",...},"market_data":[...],"fundamentals":[...],"history":[...]}
```
