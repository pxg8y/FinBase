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
| **Company Profile** | Stock Exchange | `string` | `companies.exchange` | Finnhub Stock Profile 2 (`exchange`) | OpenFIGI Mapping API (`exchCode`) |
| **Company Profile** | Outstanding Shares | `float64` | `companies.outstanding_shares` | Finnhub Stock Profile 2 (`shareOutstanding`) | N/A |
| **Company Profile** | Logo URL | `string` | `companies.logo_url` | Finnhub Stock Profile 2 (`logo`) | N/A |
| **Market Data** | Real-Time / Current Price | `float64` | `market_data.current_price` | Finnhub Quote API (`c`) | Tiingo API (`close`) / Twelve Data API (`close`) |
| **Market Data** | Open Price | `float64` | `market_data.open_price` | Finnhub Quote API (`o`) | Tiingo API (`open`) / Twelve Data API (`open`) |
| **Market Data** | High Price | `float64` | `market_data.high_price` | Finnhub Quote API (`h`) | Tiingo API (`high`) / Twelve Data API (`high`) |
| **Market Data** | Low Price | `float64` | `market_data.low_price` | Finnhub Quote API (`l`) | Tiingo API (`low`) / Twelve Data API (`low`) |
| **Market Data** | Previous Close Price | `float64` | `market_data.previous_close` | Finnhub Quote API (`pc`) | Twelve Data API (`previous_close`) |
| **Market Data** | Market Capitalization | `float64` | `market_data.market_cap` | Local Calculation (`current_price * outstanding_shares`) | N/A |
| **Market Data** | 52-Week High | `float64` | `market_data.fifty_two_week_high` | Twelve Data API (`fifty_two_week.high`) | N/A |
| **Market Data** | 52-Week Low | `float64` | `market_data.fifty_two_week_low` | Twelve Data API (`fifty_two_week.low`) | N/A |
| **Market Data** | Trading Volume | `int64` | `market_data.volume` | Finnhub Quote API / Tiingo / Twelve Data | Default / N/A |
| **Market Data** | Fetch Timestamp | `datetime` | `market_data.timestamp` | System Timestamp (`CURRENT_TIMESTAMP`) | N/A |
| **Fundamentals** | Reporting Period | `string` | `fundamentals.period` | SEC EDGAR XBRL Facts (`fy`, `fp`) | FMP Income Statement (`calendarYear`, `period`) |
| **Fundamentals** | Revenues | `float64` | `fundamentals.value` (Metric: `Revenues`) | SEC EDGAR XBRL (`Revenues`, `SalesRevenueNet`, `RevenueFromContractWithCustomerExcludingAssessedTax`) | FMP Income Statement (`revenue`) |
| **Fundamentals** | Net Income / Loss | `float64` | `fundamentals.value` (Metric: `NetIncome`) | SEC EDGAR XBRL (`NetIncomeLoss`) | FMP Income Statement (`netIncome`) |
| **Fundamentals** | Operating Income / Loss | `float64` | `fundamentals.value` (Metric: `OperatingIncome`) | SEC EDGAR XBRL (`OperatingIncomeLoss`) | FMP Income Statement (`operatingIncome`) |
| **Fundamentals** | Total Assets | `float64` | `fundamentals.value` (Metric: `TotalAssets`) | SEC EDGAR XBRL (`Assets`) | N/A |
| **Fundamentals** | Earnings Per Share (EPS) | `float64` | `fundamentals.value` (Metric: `EPS`) | SEC EDGAR XBRL (`EarningsPerShareBasic`, `EarningsPerShareDiluted`) | FMP Income Statement (`epsdiluted` / `eps`) |
| **Fundamentals** | Gross Profit | `float64` | `fundamentals.value` (Metric: `GrossProfit`) | SEC EDGAR XBRL (`GrossProfit`) | FMP Income Statement (`grossProfit`) |
| **Fundamentals** | Total Liabilities | `float64` | `fundamentals.value` (Metric: `TotalLiabilities`) | SEC EDGAR XBRL (`Liabilities`) | N/A |
| **Fundamentals** | Free Cash Flow | `float64` | `fundamentals.value` (Metric: `FreeCashFlow`) | SEC Native Calculation (`OperatingCashFlow - CapEx`) | N/A |

---

## 2. External Data Sources & API Bindings

The table below summarizes the binding limits, rate limiters, circuit breakers, header constraints, and data formatting rules for each third-party data source binding.

| Data Source | Base URL / Endpoint | Collected Parameters | Rate Limit & Algorithm | Circuit Breaker Cooldown | Authentication & Headers | Special Formatting / Constraints |
|---|---|---|---|---|---|---|
| **Finnhub Stock Profile 2** | `https://finnhub.io/api/v1/stock/profile2` | Company Name, Industry Sector, Exchange, Share Outstanding, Logo URL | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON object. |
| **Finnhub Quote** | `https://finnhub.io/api/v1/quote` | Current Price (`c`), Open (`o`), High (`h`), Low (`l`), Previous Close (`pc`), Timestamp (`t`) | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON quote metrics. |
| **Tiingo Daily Prices** | `https://api.tiingo.com/tiingo/daily/{ticker}/prices` | Current Price (`close`), Open (`open`), High (`high`), Low (`low`), Volume (`volume`) | 500 requests / hour (`golang.org/x/time/rate`) | 5 failures -> 30s open cooldown | `Authorization: Token <TIINGO_API_KEY>` or `?token={key}` | Fallback for Market Data when Finnhub is rate-limited (HTTP 429) or circuit breaker is open. |
| **Twelve Data Quote** | `https://api.twelvedata.com/quote` | Current Price (`close`), Open (`open`), High (`high`), Low (`low`), Previous Close (`previous_close`), 52-Week High/Low | 8 requests / min (`go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | Query param `?apikey=<TWELVE_DATA_API_KEY>` | 3rd priority fallback for Market Data when Tiingo fails. Returns string-encoded numbers. |
| **OpenFIGI Mapping** | `https://api.openfigi.com/v3/mapping` | Composite FIGI, Company Name, Market Sector, Exchange Code | 25 requests per 6 seconds (Hard interval limiter `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `X-OPENFIGI-KEY: <OPENFIGI_API_KEY>` (Optional for free tier) | Request body payload requires array of objects `[{"idType":"TICKER", "idValue":"{ticker}"}]`. |
| **SEC Tickers Mapping** | `https://www.sec.gov/files/company_tickers.json` | CIK mapping for Tickers | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | High rate strictly enforced by SEC. IP ban risk if User-Agent format is invalid. |
| **SEC EDGAR XBRL Facts** | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json` | Fundamental Financial Facts (Revenues, Net Income, Operating Income, Assets, EPS, Gross Profit, Total Liabilities, Operating Cash Flow, CapEx) | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | **Strict CIK Requirement:** CIK MUST be padded with leading zeros to **exactly 10 digits** (e.g. CIK `320193` -> `0000320193`). |
| **Financial Modeling Prep (FMP)** | `https://financialmodelingprep.com/api/v3/income-statement/{ticker}` | Fundamental Financial Statements (Revenue, Gross Profit, Operating Income, Net Income, EPS) | 250 requests / day (`golang.org/x/time/rate`) | 5 failures -> 30s open cooldown | Query param `?apikey=<FMP_KEY>` | Fallback for Financial Fundamentals when ticker lacks valid 10-digit CIK (non-US stock) or SEC EDGAR API fails. |

---

## 3. Data Processing & Pipeline Lifecycle

When a ticker job is dispatched from the watchlist:
1. **Finnhub Profile Fetch:** Attempts to fetch `Company Name`, `FinnhubIndustry`, `Exchange`, `ShareOutstanding`, and `Logo URL`.
2. **OpenFIGI Mapping Fetch:** Attempts to fetch fallback `Name`, `MarketSector`, and `ExchCode` if Finnhub profile returns empty values.
3. **SEC CIK Lookup:** Queries SEC `company_tickers.json` for matching ticker symbol and formats CIK to 10 digits using zero-padding logic (`FormatCIK`). Falls back to previously cached DB CIK if lookup fails.
4. **Company Record Upsert:** Writes consolidated company profile metadata (`ticker`, `cik`, `isin`, `name`, `sector`, `exchange`, `outstanding_shares`, `logo_url`) to the SQLite `companies` table using `ON CONFLICT(ticker) DO UPDATE`.
5. **Market Data Waterfall Routing:** Queries market quote in priority order:
   - **1st Priority:** Finnhub Quote API.
   - **2nd Priority (Fallback on HTTP 429, Circuit Breaker Open, or error):** Tiingo API (`/tiingo/daily/{ticker}/prices`).
   - **3rd Priority (Fallback on Tiingo error):** Twelve Data API (`/quote?symbol={ticker}`).
6. **Local Market Cap Calculation:** Calculates `market_cap = current_price * outstanding_shares` locally without an additional API call, and stores price metrics in `market_data` table.
7. **Fundamentals Fetching & SEC -> FMP Fallback:**
   - **SEC EDGAR:** If a valid 10-digit CIK exists, queries `data.sec.gov`. Extracts `us-gaap` concepts for `Revenues`, `NetIncome`, `OperatingIncome`, `TotalAssets`, `EPS`, `GrossProfit`, `TotalLiabilities`, and calculates native `FreeCashFlow = OperatingCashFlow - CapEx`.
   - **FMP Fallback:** If CIK is missing/invalid (e.g. non-US tickers) or SEC EDGAR API fails, calls Financial Modeling Prep (FMP) `/api/v3/income-statement/{ticker}` and maps response metrics to internal schema.
8. **SSE Event Broadcast:** Emits live status logs (`PROCESSING`, `SUCCESS`, or `ERROR`) and pushes updated consolidated data JSON (`company_update`) over `/api/sse` connection.
