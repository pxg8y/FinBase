# Ticker Collected Parameters & API Source Bindings

This document outlines all data parameters collected for stock tickers within the FinBase (FDAAE) application, detailing their data sources, priority fallback strategies, API limitations, rate limits, authentication bindings, and formatting requirements.

---

## 1. Parameters Collected per Ticker

The system aggregates parameters across three main domain categories: **Company Profile**, **Market Data**, and **Financial Fundamentals**.

| Category | Parameter Name | Data Type | Database Field | Primary Data Source | Fallback Data Source |
|---|---|---|---|---|---|
| **Company Profile** | Stock Ticker Symbol | `string` | `companies.ticker` | User Input / Watchlist | N/A |
| **Company Profile** | SEC Central Index Key (CIK) | `string` | `companies.cik` | SEC Tickers Mapping (`company_tickers.json`) | Existing DB Record |
| **Company Profile** | International Securities Identification Number (ISIN) | `string` | `companies.isin` | OpenFIGI Mapping API (`figi` / metadata) | N/A |
| **Company Profile** | Company Name | `string` | `companies.name` | Finnhub Stock Profile 2 (`name`) | OpenFIGI Mapping API (`name`) |
| **Company Profile** | Industry / Market Sector | `string` | `companies.sector` | Finnhub Stock Profile 2 (`finnhubIndustry`) | OpenFIGI Mapping API (`marketSector`) |
| **Market Data** | Real-Time / Current Price | `float64` | `market_data.current_price` | Finnhub Quote API (`c`) | N/A |
| **Market Data** | Trading Volume | `int64` | `market_data.volume` | Finnhub Quote API (`v`) / Default | N/A |
| **Market Data** | Fetch Timestamp | `datetime` | `market_data.timestamp` | System Timestamp (`CURRENT_TIMESTAMP`) | N/A |
| **Fundamentals** | Reporting Period | `string` | `fundamentals.period` | SEC EDGAR XBRL Facts (`fy`, `fp`) | N/A |
| **Fundamentals** | Revenues | `float64` | `fundamentals.value` (Metric: `Revenues`) | SEC EDGAR XBRL (`Revenues`, `SalesRevenueNet`, `RevenueFromContractWithCustomerExcludingAssessedTax`) | N/A |
| **Fundamentals** | Net Income / Loss | `float64` | `fundamentals.value` (Metric: `NetIncome`) | SEC EDGAR XBRL (`NetIncomeLoss`) | N/A |
| **Fundamentals** | Operating Income / Loss | `float64` | `fundamentals.value` (Metric: `OperatingIncome`) | SEC EDGAR XBRL (`OperatingIncomeLoss`) | N/A |
| **Fundamentals** | Total Assets | `float64` | `fundamentals.value` (Metric: `TotalAssets`) | SEC EDGAR XBRL (`Assets`) | N/A |

---

## 2. External Data Sources & API Bindings

The table below summarizes the binding limits, rate limiters, circuit breakers, header constraints, and data formatting rules for each third-party data source binding.

| Data Source | Base URL / Endpoint | Collected Parameters | Rate Limit & Algorithm | Circuit Breaker Cooldown | Authentication & Headers | Special Formatting / Constraints |
|---|---|---|---|---|---|---|
| **Finnhub Stock Profile 2** | `https://finnhub.io/api/v1/stock/profile2` | Company Name, Industry Sector | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON object. |
| **Finnhub Quote** | `https://finnhub.io/api/v1/quote` | Current Price (`c`), Timestamp (`t`) | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON quote metrics. |
| **OpenFIGI Mapping** | `https://api.openfigi.com/v3/mapping` | Composite FIGI, Company Name, Market Sector | 25 requests per 6 seconds (Hard interval limiter `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `X-OPENFIGI-KEY: <OPENFIGI_API_KEY>` (Optional for free tier) | Request body payload requires array of objects `[{"idType":"TICKER", "idValue":"{ticker}"}]`. |
| **SEC Tickers Mapping** | `https://www.sec.gov/files/company_tickers.json` | CIK mapping for Tickers | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | High rate strictly enforced by SEC. IP ban risk if User-Agent format is invalid. |
| **SEC EDGAR XBRL Facts** | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json` | Fundamental Financial Facts (Revenues, Net Income, Operating Income, Assets) | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | **Strict CIK Requirement:** CIK MUST be padded with leading zeros to **exactly 10 digits** (e.g. CIK `320193` -> `0000320193`). |

---

## 3. Data Processing & Pipeline Lifecycle

When a ticker job is dispatched from the watchlist:
1. **Finnhub Profile Fetch:** Attempts to fetch `Company Name` and `FinnhubIndustry`.
2. **OpenFIGI Mapping Fetch:** Attempts to fetch fallback `Name` and `MarketSector` if Finnhub profile returns empty values.
3. **SEC CIK Lookup:** Queries SEC `company_tickers.json` for matching ticker symbol and formats CIK to 10 digits using zero-padding logic (`FormatCIK`). Falls back to previously cached DB CIK if lookup fails.
4. **Company Record Upsert:** Writes consolidated company profile metadata (`ticker`, `cik`, `isin`, `name`, `sector`) to the SQLite `companies` table using `ON CONFLICT(ticker) DO UPDATE`.
5. **Finnhub Quote Fetch:** Obtains real-time price and stores record in `market_data` table.
6. **SEC XBRL Fundamentals Fetch:** Uses 10-digit padded CIK to query `data.sec.gov`. Parses `us-gaap` concepts for `Revenues`, `NetIncomeLoss`, `OperatingIncomeLoss`, and `Assets`, and batch inserts the latest historical values into the `fundamentals` table.
7. **SSE Event Broadcast:** Emits live status logs (`PROCESSING`, `SUCCESS`, or `ERROR`) and pushes updated consolidated data JSON (`company_update`) over `/api/sse` connection.
