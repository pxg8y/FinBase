# Ticker Collected Parameters & API Source Bindings

This document outlines all data parameters collected for stock tickers within the FinBase (FDAAE) application, detailing their data sources, priority fallback strategies, API limitations, rate limits, authentication bindings, and formatting requirements.

---

## 1. Parameters Collected per Ticker

The system aggregates parameters across ten domain categories: **Company Profile**, **Market Data**, **Financial Fundamentals**, **Valuation & Ratios**, **Dividends & Corporate Actions**, **Historical EOD Time Series**, **Analyst Estimates & Earnings**, **Company News & Sentiment**, **Insider & Institutional Ownership**, and **Macroeconomic Context Data**.

| Category | Parameter Name | Data Type | Database Table / Field | Primary Data Source | Fallback Data Source |
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
| **Fundamentals** | Revenues | `float64` | `fundamentals.value` (Metric: `Revenues`) | SEC EDGAR XBRL (`Revenues`, `SalesRevenueNet`) | FMP Income Statement (`revenue`) |
| **Fundamentals** | Net Income / Loss | `float64` | `fundamentals.value` (Metric: `NetIncome`) | SEC EDGAR XBRL (`NetIncomeLoss`) | FMP Income Statement (`netIncome`) |
| **Fundamentals** | Operating Income / Loss | `float64` | `fundamentals.value` (Metric: `OperatingIncome`) | SEC EDGAR XBRL (`OperatingIncomeLoss`) | FMP Income Statement (`operatingIncome`) |
| **Fundamentals** | Total Assets | `float64` | `fundamentals.value` (Metric: `TotalAssets`) | SEC EDGAR XBRL (`Assets`) | N/A |
| **Fundamentals** | Earnings Per Share (EPS) | `float64` | `fundamentals.value` (Metric: `EPS`) | SEC EDGAR XBRL (`EarningsPerShareBasic`, `EarningsPerShareDiluted`) | FMP Income Statement (`epsdiluted` / `eps`) |
| **Fundamentals** | Gross Profit | `float64` | `fundamentals.value` (Metric: `GrossProfit`) | SEC EDGAR XBRL (`GrossProfit`) | FMP Income Statement (`grossProfit`) |
| **Fundamentals** | Total Liabilities | `float64` | `fundamentals.value` (Metric: `TotalLiabilities`) | SEC EDGAR XBRL (`Liabilities`) | N/A |
| **Fundamentals** | Free Cash Flow | `float64` | `fundamentals.value` (Metric: `FreeCashFlow`) | SEC Native Calculation (`OperatingCashFlow - CapEx`) | N/A |
| **Valuation & Ratios** | P/E, P/B, P/S Ratios, Margins, ROE, ROA | `float64` | `valuation_ratios` table | Finnhub Basic Financials (`/stock/metric?symbol={ticker}&metric=all`) | N/A |
| **Dividends & Actions** | Cash Dividends, Ex-Date, Pay Date, Amounts | `float64`, `string` | `dividends` table | Finnhub Dividends (`/stock/dividend?symbol={ticker}`) | Tiingo Daily Prices (`/tiingo/daily/{ticker}/prices`) |
| **Dividends & Actions** | Stock Splits (Ratio, Execution Date) | `float64`, `string` | `stock_splits` table | Finnhub Splits (`/stock/split?symbol={ticker}`) | N/A |
| **Historical EOD** | Daily OHLCV & Adjusted Close Series | `float64`, `int64`, `string` | `historical_prices` table | Tiingo Daily Historical Prices (`/tiingo/daily/{ticker}/prices?startDate=...`) | N/A |
| **Analyst & Earnings** | Recommendation Trends & EPS/Revenue Estimates | `float64`, `int`, `string` | `analyst_estimates` table | Finnhub Recommendation Trends (`/stock/recommendation?symbol={ticker}`) | N/A |
| **Analyst & Earnings** | Earnings Calendar & EPS/Revenue Surprises | `float64`, `string`, `int` | `earnings_calendar` table | Finnhub Earnings Calendar (`/calendar/earnings?symbol={ticker}`) | N/A |
| **Company News** | News Headine, Summary, Source, URL, Sentiment | `string`, `float64` | `company_news` table | Finnhub Company News (`/company-news?symbol={ticker}&from=...&to=...`) | N/A |
| **Insider & Institutional** | Insider Transactions (Filing Date, Shares, Price) | `string`, `float64` | `insider_transactions` table | Finnhub Insider Transactions (`/stock/insider-transactions?symbol={ticker}`) | SEC EDGAR Form 4 |
| **Insider & Institutional** | Institutional Holdings (13F Form Filings) | `string`, `float64` | `institutional_ownership` table | Finnhub / SEC EDGAR Form 13F | N/A |
| **Macro Context** | Treasury Yields (10Y DGS10), CPI (CPIAUCSL) | `float64`, `string` | `macro_indicators` table | FRED API (`/fred/series/observations?series_id=...&api_key=...`) | N/A |

---

## 2. External Data Sources & API Bindings

The table below summarizes the binding limits, rate limiters, circuit breakers, header constraints, and data formatting rules for each third-party data source binding (including the 7 new domains).

| Data Source | Base URL / Endpoint | Collected Parameters | Rate Limit & Algorithm | Circuit Breaker Cooldown | Authentication & Headers | Special Formatting / Constraints |
|---|---|---|---|---|---|---|
| **Finnhub Stock Profile 2** | `https://finnhub.io/api/v1/stock/profile2` | Company Name, Industry Sector, Exchange, Share Outstanding, Logo URL | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON object. |
| **Finnhub Quote** | `https://finnhub.io/api/v1/quote` | Current Price (`c`), Open (`o`), High (`h`), Low (`l`), Previous Close (`pc`), Timestamp (`t`) | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Symbol parameter `?symbol={ticker}`. Returns JSON quote metrics. |
| **Finnhub Basic Financials** | `https://finnhub.io/api/v1/stock/metric` | Valuation Ratios (P/E, P/B, P/S), Operating/Gross/Net Margins, ROE, ROA, Debt-to-Equity | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query params `?symbol={ticker}&metric=all`. Returns nested metric JSON map under `metric` key. |
| **Finnhub Dividends** | `https://finnhub.io/api/v1/stock/dividerd` | Ex-Date, Pay Date, Record Date, Amount, Currency, Frequency | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query param `?symbol={ticker}`. Returns JSON array of dividend objects. |
| **Finnhub Stock Splits** | `https://finnhub.io/api/v1/stock/split` | Execution Date, From Factor, To Factor | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query param `?symbol={ticker}`. Returns JSON array of split events. |
| **Finnhub Analyst Recommendations** | `https://finnhub.io/api/v1/stock/recommendation` | Strong Buy, Buy, Hold, Sell, Strong Sell count per period | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query param `?symbol={ticker}`. Returns list of analyst recommendation trends. |
| **Finnhub Earnings Calendar** | `https://finnhub.io/api/v1/calendar/earnings` | Date, Quarter, Year, EPS Estimate, EPS Actual, Revenue Estimate, Revenue Actual | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query param `?symbol={ticker}`. Returns `earningsCalendar` array. |
| **Finnhub Company News** | `https://finnhub.io/api/v1/company-news` | News Headline, Summary, Source, URL, Published Timestamp, Sentiment | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query params `?symbol={ticker}&from={YYYY-MM-DD}&to={YYYY-MM-DD}`. Returns JSON array of news articles. |
| **Finnhub Insider Transactions** | `https://finnhub.io/api/v1/stock/insider-transactions` | Name, Share Count, Change, Filing Date, Transaction Code, Transaction Price | 60 requests / min (Token Bucket `golang.org/x/time/rate`, 1 req/sec with burst capacity 5) | 5 failures -> 30s open cooldown | `X-Finnhub-Token: <FINNHUB_API_KEY>` | Query param `?symbol={ticker}`. Returns `data` array of insider transactions. |
| **Tiingo Daily Prices & Historical EOD** | `https://api.tiingo.com/tiingo/daily/{ticker}/prices` | OHLCV Time Series, Adjusted Close, Dividend Amounts, Split Ratios | 500 requests / hour (`golang.org/x/time/rate`) | 5 failures -> 30s open cooldown | `Authorization: Token <TIINGO_API_KEY>` or `?token={key}` | Query params `?startDate={YYYY-MM-DD}&endDate={YYYY-MM-DD}`. Fallback for market data and dividend actions. |
| **Twelve Data Quote** | `https://api.twelvedata.com/quote` | Current Price (`close`), Open (`open`), High (`high`), Low (`low`), Previous Close (`previous_close`), 52-Week High/Low | 8 requests / min (`go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | Query param `?apikey=<TWELVE_DATA_API_KEY>` | 3rd priority fallback for Market Data when Tiingo fails. Returns string-encoded numbers. |
| **OpenFIGI Mapping** | `https://api.openfigi.com/v3/mapping` | Composite FIGI, Company Name, Market Sector, Exchange Code | 25 requests per 6 seconds (Hard interval limiter `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `X-OPENFIGI-KEY: <OPENFIGI_API_KEY>` (Optional for free tier) | Request body payload requires array of objects `[{"idType":"TICKER", "idValue":"{ticker}"}]`. |
| **SEC Tickers Mapping** | `https://www.sec.gov/files/company_tickers.json` | CIK mapping for Tickers | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | High rate strictly enforced by SEC. IP ban risk if User-Agent format is invalid. |
| **SEC EDGAR XBRL Facts** | `https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json` | Fundamental Financial Facts (Revenues, Net Income, Operating Income, Assets, EPS, Gross Profit, Total Liabilities, Operating Cash Flow, CapEx) | 10 requests / sec max (Leaky Bucket `go.uber.org/ratelimit`) | 5 failures -> 30s open cooldown | `User-Agent: SampleApp user@example.com` (Mandatory), `Accept-Encoding: gzip, deflate` | **Strict CIK Requirement:** CIK MUST be padded with leading zeros to **exactly 10 digits** (e.g. CIK `320193` -> `0000320193`). |
| **Financial Modeling Prep (FMP)** | `https://financialmodelingprep.com/api/v3/income-statement/{ticker}` | Fundamental Financial Statements (Revenue, Gross Profit, Operating Income, Net Income, EPS) | 250 requests / day (`golang.org/x/time/rate`) | 5 failures -> 30s open cooldown | Query param `?apikey=<FMP_KEY>` | Fallback for Financial Fundamentals when ticker lacks valid 10-digit CIK (non-US stock) or SEC EDGAR API fails. |
| **FRED API (Macro Data)** | `https://api.stlouisfed.org/fred/series/observations` | Treasury Yields (10Y `DGS10`), Consumer Price Index (`CPIAUCSL`), Fed Funds Rate | 120 requests / min (`golang.org/x/time/rate`) | 5 failures -> 30s open cooldown | Query param `?api_key=<FRED_API_KEY>&file_type=json` | Requires `series_id` parameter (e.g. `DGS10`, `CPIAUCSL`). Returns JSON `observations` array with `date` and `value`. |

---

## 3. Extended Database Schema Specifications

To support the 7 new data domains, the SQLite database schema is expanded with seven new tables:

```sql
CREATE TABLE IF NOT EXISTS valuation_ratios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    pe_ratio REAL DEFAULT 0,
    pb_ratio REAL DEFAULT 0,
    ps_ratio REAL DEFAULT 0,
    gross_margin REAL DEFAULT 0,
    operating_margin REAL DEFAULT 0,
    net_margin REAL DEFAULT 0,
    roe REAL DEFAULT 0,
    roa REAL DEFAULT 0,
    debt_to_equity REAL DEFAULT 0,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS dividends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    ex_date TEXT NOT NULL,
    payment_date TEXT,
    record_date TEXT,
    amount REAL NOT NULL,
    currency TEXT DEFAULT 'USD',
    frequency INTEGER DEFAULT 0,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stock_splits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    execution_date TEXT NOT NULL,
    from_factor REAL NOT NULL,
    to_factor REAL NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS historical_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    adj_close_price REAL NOT NULL,
    volume INTEGER NOT NULL,
    UNIQUE(company_id, date) ON CONFLICT REPLACE,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS analyst_estimates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    period TEXT NOT NULL,
    strong_buy INTEGER DEFAULT 0,
    buy INTEGER DEFAULT 0,
    hold INTEGER DEFAULT 0,
    sell INTEGER DEFAULT 0,
    strong_sell INTEGER DEFAULT 0,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS earnings_calendar (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    quarter INTEGER,
    year INTEGER,
    eps_estimate REAL DEFAULT 0,
    eps_actual REAL DEFAULT 0,
    revenue_estimate REAL DEFAULT 0,
    revenue_actual REAL DEFAULT 0,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS company_news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    news_id INTEGER,
    headline TEXT NOT NULL,
    summary TEXT,
    source TEXT,
    url TEXT,
    published_at DATETIME NOT NULL,
    sentiment_score REAL DEFAULT 0,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS insider_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    share_count REAL DEFAULT 0,
    change_shares REAL DEFAULT 0,
    filing_date TEXT NOT NULL,
    transaction_code TEXT,
    transaction_price REAL DEFAULT 0,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS institutional_ownership (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id INTEGER NOT NULL,
    investor_name TEXT NOT NULL,
    shares_held REAL DEFAULT 0,
    change_shares REAL DEFAULT 0,
    value REAL DEFAULT 0,
    period TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL NOT NULL,
    UNIQUE(series_id, date) ON CONFLICT REPLACE
);
```

---

## 4. Extended Data Processing & Pipeline Lifecycle

When a ticker job is dispatched from the watchlist:
1. **Finnhub Profile Fetch:** Attempts to fetch `Company Name`, `FinnhubIndustry`, `Exchange`, `ShareOutstanding`, and `Logo URL`.
2. **OpenFIGI Mapping Fetch:** Attempts to fetch fallback `Name`, `MarketSector`, and `ExchCode` if Finnhub profile returns empty values.
3. **SEC CIK Lookup:** Queries SEC `company_tickers.json` for matching ticker symbol and formats CIK to 10 digits using zero-padding logic (`FormatCIK`). Falls back to previously cached DB CIK if lookup fails.
4. **Company Record Upsert:** Writes consolidated company profile metadata (`ticker`, `cik`, `isin`, `name`, `sector`, `exchange`, `outstanding_shares`, `logo_url`) to the SQLite `companies` table using `ON CONFLICT(ticker) DO UPDATE`.
5. **Market Data Waterfall Routing:** Queries market quote in priority order: Finnhub -> Tiingo -> Twelve Data.
6. **Local Market Cap Calculation & Insert:** Calculates `market_cap = current_price * outstanding_shares` locally and stores price metrics in `market_data` table.
7. **Fundamentals Fetching (SEC EDGAR -> FMP Fallback):** Fetches financial XBRL facts or fallback FMP income statements and stores metrics in `fundamentals` table.
8. **Valuation & Ratios Fetching:** Queries Finnhub Basic Financials (`/stock/metric`) to fetch valuation ratios and profitability margins, upserting to `valuation_ratios`.
9. **Dividends & Corporate Actions:** Queries Finnhub `/stock/dividerd` and `/stock/split` (falling back to Tiingo price event fields if needed), saving records to `dividends` and `stock_splits`.
10. **Historical EOD Time Series:** Queries Tiingo Daily Historical API for OHLCV time series data and performs batch upsert into `historical_prices`.
11. **Analyst Estimates & Earnings Calendars:** Queries Finnhub recommendation trends and earnings calendar data, saving to `analyst_estimates` and `earnings_calendar`.
12. **Company News & Sentiment:** Queries Finnhub company news for the recent window, saving articles into `company_news`.
13. **Insider Trading & Institutional Ownership:** Queries Finnhub insider transactions or SEC EDGAR filings, saving entries into `insider_transactions` and `institutional_ownership`.
14. **Macroeconomic Context Data Collection:** Queries FRED API for macro indicators (`DGS10` 10-Year Treasury Yield, `CPIAUCSL` CPI), upserting into `macro_indicators`.
15. **SSE Event Broadcast:** Emits live status logs (`PROCESSING`, `SUCCESS`, or `ERROR`) and pushes updated consolidated data JSON (`company_update`) over `/api/sse` connection.
