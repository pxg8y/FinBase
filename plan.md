# Execution Plan: FinBase Data Pipeline Expansion (7 New Domains)

This document outlines the software engineering plan for extending FinBase (FDAAE) with 7 new data domains using free-tier financial data APIs.

## Architecture & Shift-Left Strategy

1. **Strict Rate Limiting & Circuit Breaking:**
   - Reuse existing token bucket (`golang.org/x/time/rate`) and leaky bucket (`go.uber.org/ratelimit`) rate limiters.
   - Attach independent `CircuitBreaker` (5 threshold, 30s open cooldown) instances to every new client call.
2. **Database Migration Strategy:**
   - Pure Go SQLite (`modernc.org/sqlite`) with WAL mode.
   - Schema additions are appended inside `Migrate()` in `db/db.go`.
3. **Test-Driven Development (TDD):**
   - For every domain/milestone:
     - Write comprehensive unit tests in `clients/clients_test.go` and `db/db_test.go` first.
     - Mock HTTP responses (200 OK, 429 Rate Limit, 500 Error for circuit breaker testing).
     - Verify DB insertion, upserts, and handling of empty/malformed responses.
     - Implement client methods, database functions, and worker processing logic.
     - Verify using `go test -count=1 -v ./...`.

---

## Milestones Summary

### Phase 1: Planning & Documentation (Current Status: Complete)
- Deep analysis of `TICKER_PARAMETERS.md` and codebase completed.
- `TICKER_PARAMETERS.md` updated with full data dictionary, schemas, and API rate limits.
- `plan.md` created.

### Pause & User Approval Checkpoint
- Before implementing any code for Milestone 1 or subsequent milestones, explicit user approval must be requested.

---

## Milestone Roadmaps

### Milestone 1: Valuation & Ratios (Finnhub Basic Financials)
- **Data Source:** Finnhub Basic Financials (`/stock/metric?symbol={ticker}&metric=all`).
- **Database:** `valuation_ratios` table.
- **Client Method:** `cm.FetchFinnhubValuationRatios(ctx, ticker)`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests for 200 OK, 429 handling, 500 circuit breaker open state, missing fields, and DB persistence.

### Milestone 2: Dividends & Corporate Actions (Finnhub / Tiingo)
- **Data Source:** Finnhub (`/stock/dividerd`, `/stock/split`) with Tiingo daily price event fallback.
- **Database:** `dividends` and `stock_splits` tables.
- **Client Methods:** `cm.FetchFinnhubDividends()`, `cm.FetchFinnhubSplits()`, `cm.FetchTiingoDividends()`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests for dividend/split event parsing, fallback execution, rate limiters, and DB persistence.

### Milestone 3: Historical EOD Time Series (Tiingo)
- **Data Source:** Tiingo Daily Prices (`/tiingo/daily/{ticker}/prices`).
- **Database:** `historical_prices` table with `UNIQUE(company_id, date) ON CONFLICT REPLACE`.
- **Client Method:** `cm.FetchTiingoHistoricalPrices(ctx, ticker, startDate, endDate)`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests for time series fetching, batch DB upsert, rate limit wait, and error handling.

### Milestone 4: Analyst Estimates & Earnings Calendars (Finnhub)
- **Data Source:** Finnhub (`/stock/recommendation`, `/calendar/earnings`).
- **Database:** `analyst_estimates` and `earnings_calendar` tables.
- **Client Methods:** `cm.FetchFinnhubAnalystEstimates()`, `cm.FetchFinnhubEarningsCalendar()`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests covering recommendation trends, earnings surprise fields, 429 rate limit errors, and DB operations.

### Milestone 5: Company-specific News & Sentiment (Finnhub)
- **Data Source:** Finnhub (`/company-news?symbol={ticker}&from=...&to=...`).
- **Database:** `company_news` table.
- **Client Method:** `cm.FetchFinnhubCompanyNews(ctx, ticker, from, to)`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests for news article extraction, sentiment score calculation/parsing, circuit breaker cooldown, and news deduplication.

### Milestone 6: Insider Trading & Institutional Ownership (Finnhub / SEC EDGAR)
- **Data Source:** Finnhub (`/stock/insider-transactions`) and SEC EDGAR Form 4/13F.
- **Database:** `insider_transactions` and `institutional_ownership` tables.
- **Client Methods:** `cm.FetchFinnhubInsiderTransactions()`, `cm.FetchFinnhubInstitutionalOwnership()`.
- **Worker Pipeline:** Integrated in `wp.processJob()`.
- **Testing:** Unit tests for insider share change mapping, institutional holder parsing, 429 limits, and DB storage.

### Milestone 7: Macroeconomic Context Data (FRED API)
- **Data Source:** FRED API (`/fred/series/observations?series_id=...`). Supports `FRED_API_KEY`.
- **Indicators:** 10-Year Treasury Yield (`DGS10`), Consumer Price Index (`CPIAUCSL`).
- **Database:** `macro_indicators` table with `UNIQUE(series_id, date) ON CONFLICT REPLACE`.
- **Client Method:** `cm.FetchFREDSeries(ctx, seriesID)`.
- **Configuration:** Updated `main.go` to pass `FRED_API_KEY`.
- **Worker Pipeline:** Integrated in `wp.processJob()` or scheduled worker task.
- **Testing:** Unit tests for FRED API parsing, missing API key handling, rate limiting, and DB batch insertion.

---

## Final Verification & Submission
- Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.
- Run `go test -count=1 -v ./...` across all packages.
- Final code commit and submission.
