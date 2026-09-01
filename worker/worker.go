package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"finbase/clients"
	"finbase/db"
)

type SSEBroadcaster interface {
	BroadcastLog(ticker, status, message string)
	BroadcastUpdate(ticker string, data any)
}

type Job struct {
	Ticker            string
	Priority          int
	ForceFullRefresh bool
}

type ActiveJob struct {
	WorkerID  int       `json:"worker_id"`
	Ticker    string    `json:"ticker"`
	StartedAt time.Time `json:"started_at"`
}

type WorkerPoolStatus struct {
	TotalWorkers int         `json:"total_workers"`
	ActiveWorkers int        `json:"active_workers"`
	QueueDepth   int         `json:"queue_depth"`
	CurrentJobs  []ActiveJob `json:"current_jobs"`
}

type WorkerPool struct {
	db          *db.DB
	clients     *clients.ClientManager
	broadcaster SSEBroadcaster

	jobChan    chan Job
	numWorkers int
	stopChan   chan struct{}
	wg         sync.WaitGroup

	mu         sync.RWMutex
	activeJobs map[int]ActiveJob
}

func NewWorkerPool(database *db.DB, clientMgr *clients.ClientManager, broadcaster SSEBroadcaster, numWorkers int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
		if numWorkers <= 0 {
			numWorkers = 4
		}
	}
	return &WorkerPool{
		db:          database,
		clients:     clientMgr,
		broadcaster: broadcaster,
		jobChan:     make(chan Job, 100),
		numWorkers:  numWorkers,
		stopChan:    make(chan struct{}),
		activeJobs:  make(map[int]ActiveJob),
	}
}

func (wp *WorkerPool) GetStatus() WorkerPoolStatus {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	currentJobs := make([]ActiveJob, 0, len(wp.activeJobs))
	for _, aj := range wp.activeJobs {
		currentJobs = append(currentJobs, aj)
	}

	return WorkerPoolStatus{
		TotalWorkers:  wp.numWorkers,
		ActiveWorkers: len(wp.activeJobs),
		QueueDepth:    len(wp.jobChan),
		CurrentJobs:   currentJobs,
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	// Start Dispatcher
	go wp.dispatcher(ctx)

	// Start Global Macro Worker
	go wp.macroWorker(ctx)

	// Start Worker goroutines
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i+1)
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.stopChan)
	wp.wg.Wait()
}

func (wp *WorkerPool) dispatcher(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		case <-ticker.C:
			// Atomically fetch highest priority, oldest updated watchitem and queue it
			item, err := wp.db.FetchAndQueueNextWatchitem(ctx)
			if err != nil {
				log.Printf("[Dispatcher] Error fetching watchitem: %v", err)
				continue
			}
			if item != nil {
				oldStatus := item.Status
				select {
				case wp.jobChan <- Job{Ticker: item.Ticker, Priority: item.Priority, ForceFullRefresh: item.ForceFullRefresh}:
				default:
					// Job channel full, revert status to previous status
					_ = wp.db.UpdateWatchitemStatus(ctx, item.Ticker, oldStatus)
				}
			}
		}
	}
}

func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}
			wp.mu.Lock()
			wp.activeJobs[id] = ActiveJob{
				WorkerID:  id,
				Ticker:    job.Ticker,
				StartedAt: time.Now(),
			}
			wp.mu.Unlock()

			wp.processJob(ctx, job)

			wp.mu.Lock()
			delete(wp.activeJobs, id)
			wp.mu.Unlock()
		}
	}
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}

func (wp *WorkerPool) shouldUpdateCategory(ctx context.Context, companyID int64, category string, ttl time.Duration, force bool) bool {
	if force || companyID == 0 {
		return true
	}
	lastUpdated, err := wp.db.GetCategoryLastUpdated(ctx, companyID, category)
	if err != nil || lastUpdated.IsZero() {
		return true
	}
	return time.Since(lastUpdated) >= ttl
}

func (wp *WorkerPool) macroWorker(ctx context.Context) {
	ttlMacro := getEnvDuration("TTL_MACRO", 24*time.Hour)
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	runMacroFetch := func() {
		yields, err := wp.clients.FetchFREDSeries(ctx, "DGS10", "10-Year Treasury Constant Maturity Rate")
		if err == nil && len(yields) > 0 {
			var dbMacro []db.MacroIndicator
			for _, m := range yields {
				dbMacro = append(dbMacro, db.MacroIndicator{
					SeriesID:      m.SeriesID,
					IndicatorName: m.IndicatorName,
					Date:          m.Date,
					Value:         m.Value,
				})
			}
			_ = wp.db.InsertMacroIndicatorsBatch(ctx, dbMacro)
		}

		cpi, err := wp.clients.FetchFREDSeries(ctx, "CPIAUCSL", "Consumer Price Index for All Urban Consumers")
		if err == nil && len(cpi) > 0 {
			var dbMacro []db.MacroIndicator
			for _, m := range cpi {
				dbMacro = append(dbMacro, db.MacroIndicator{
					SeriesID:      m.SeriesID,
					IndicatorName: m.IndicatorName,
					Date:          m.Date,
					Value:         m.Value,
				})
			}
			_ = wp.db.InsertMacroIndicatorsBatch(ctx, dbMacro)
		}
	}

	// Initial fetch
	runMacroFetch()

	for {
		select {
		case <-ctx.Done():
			return
		case <-wp.stopChan:
			return
		case <-ticker.C:
			_ = ttlMacro
			runMacroFetch()
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, job Job) {
	tickerStr := job.Ticker
	force := job.ForceFullRefresh

	logMsg := fmt.Sprintf("Starting processing for ticker %s (force_refresh=%v)", tickerStr, force)
	log.Printf("[Worker] %s", logMsg)
	_ = wp.db.UpdateWatchitemStatus(ctx, tickerStr, "processing")
	if wp.broadcaster != nil {
		wp.broadcaster.BroadcastLog(tickerStr, "PROCESSING", logMsg)
	}

	ttlMarketData := getEnvDuration("TTL_MARKET_DATA", 5*time.Minute)
	ttlNews := getEnvDuration("TTL_NEWS", 12*time.Hour)
	ttlValuation := getEnvDuration("TTL_VALUATION", 24*time.Hour)
	ttlHistorical := getEnvDuration("TTL_HISTORICAL", 24*time.Hour)
	ttlAnalyst := getEnvDuration("TTL_ANALYST", 72*time.Hour)
	ttlCompanyProfile := getEnvDuration("TTL_COMPANY_PROFILE", 720*time.Hour)
	ttlQuarterly := getEnvDuration("TTL_QUARTERLY", 168*time.Hour)

	var cik, companyName, sector, exchange, logoURL string
	var outstandingShares float64
	var compID int64

	existingData, _ := wp.db.GetConsolidatedData(ctx, tickerStr)
	if existingData != nil && existingData.Company.ID > 0 {
		compID = existingData.Company.ID
		cik = existingData.Company.CIK
		companyName = existingData.Company.Name
		sector = existingData.Company.Sector
		exchange = existingData.Company.Exchange
		outstandingShares = existingData.Company.OutstandingShares
		logoURL = existingData.Company.LogoURL
	}

	// 1. Company Profile
	if wp.shouldUpdateCategory(ctx, compID, "profile", ttlCompanyProfile, force) {
		profile, err := wp.clients.FetchFinnhubProfile(ctx, tickerStr)
		if err == nil && profile != nil {
			companyName = profile.Name
			sector = profile.FinnhubIndustry
			exchange = profile.Exchange
			logoURL = profile.Logo
			if profile.ShareOutstanding > 0 {
				if profile.ShareOutstanding < 1e8 {
					outstandingShares = profile.ShareOutstanding * 1e6
				} else {
					outstandingShares = profile.ShareOutstanding
				}
			}
		}

		figiRes, err := wp.clients.FetchOpenFIGI(ctx, tickerStr)
		if err == nil && figiRes != nil && len(figiRes.Data) > 0 {
			if companyName == "" {
				companyName = figiRes.Data[0].Name
			}
			if sector == "" {
				sector = figiRes.Data[0].MarketSector
			}
			if exchange == "" {
				exchange = figiRes.Data[0].ExchCode
			}
		}

		secCIK, err := wp.clients.FetchSECCIKForTicker(ctx, tickerStr)
		if err == nil && secCIK != "" {
			cik = clients.FormatCIK(secCIK)
		} else if cik == "" {
			if existingData != nil && existingData.Company.CIK != "" {
				cik = clients.FormatCIK(existingData.Company.CIK)
			}
		}

		newCompID, err := wp.db.UpsertCompany(ctx, &db.Company{
			Ticker:            tickerStr,
			CIK:               cik,
			Name:              companyName,
			Sector:            sector,
			Exchange:          exchange,
			OutstandingShares: outstandingShares,
			LogoURL:           logoURL,
		})
		if err != nil {
			errMsg := fmt.Sprintf("Failed to upsert company %s: %v", tickerStr, err)
			log.Printf("[Worker] %s", errMsg)
			_ = wp.db.LogAction(ctx, tickerStr, "UPSERT_COMPANY", "ERROR", errMsg)
			_ = wp.db.UpdateWatchitemStatus(ctx, tickerStr, "error")
			if wp.broadcaster != nil {
				wp.broadcaster.BroadcastLog(tickerStr, "ERROR", errMsg)
			}
			return
		}
		compID = newCompID
		_ = wp.db.SetCategoryLastUpdated(ctx, compID, "profile")
	}

	// 2. Market Data
	if wp.shouldUpdateCategory(ctx, compID, "market_data", ttlMarketData, force) {
		quote, err := wp.clients.FetchMarketDataWaterfall(ctx, tickerStr)
		if err == nil && quote != nil {
			marketCap := quote.CurrentPrice * outstandingShares
			md := &db.MarketData{
				CurrentPrice:     quote.CurrentPrice,
				OpenPrice:        quote.OpenPrice,
				HighPrice:        quote.HighPrice,
				LowPrice:         quote.LowPrice,
				PreviousClose:    quote.PreviousClose,
				Volume:           quote.Volume,
				FiftyTwoWeekHigh: quote.FiftyTwoWeekHigh,
				FiftyTwoWeekLow:  quote.FiftyTwoWeekLow,
				MarketCap:        marketCap,
			}
			if err := wp.db.InsertMarketData(ctx, compID, md); err != nil {
				log.Printf("[Worker] Error inserting market data for %s: %v", tickerStr, err)
			} else {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_QUOTE", "SUCCESS", fmt.Sprintf("Price: $%.2f (Source: %s, Market Cap: $%.0f)", quote.CurrentPrice, quote.Source, marketCap))
				_ = wp.db.SetCategoryLastUpdated(ctx, compID, "market_data")
			}
		} else if err != nil {
			log.Printf("[Worker] Error fetching market data waterfall for %s: %v", tickerStr, err)
			_ = wp.db.LogAction(ctx, tickerStr, "FETCH_QUOTE", "ERROR", fmt.Sprintf("Market data failed: %v", err))
		}
	}

	// 3. Fundamentals
	if wp.shouldUpdateCategory(ctx, compID, "fundamentals", ttlQuarterly, force) {
		secSuccess := false
		if cik != "" && cik != "0000000000" {
			facts, err := wp.clients.FetchSECFacts(ctx, cik)
			if err == nil && facts != nil {
				count := extractAndStoreSECFacts(ctx, wp.db, compID, facts)
				if count > 0 {
					secSuccess = true
					_ = wp.db.LogAction(ctx, tickerStr, "FETCH_SEC_FACTS", "SUCCESS", fmt.Sprintf("Fetched XBRL facts for CIK %s (%d metrics stored)", cik, count))
				}
			} else if err != nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_SEC_FACTS", "WARN", fmt.Sprintf("SEC EDGAR failed for CIK %s: %v", cik, err))
			}
		}

		if !secSuccess {
			fmpStmts, err := wp.clients.FetchFMPIncomeStatement(ctx, tickerStr)
			if err == nil && len(fmpStmts) > 0 {
				count := extractAndStoreFMPFacts(ctx, wp.db, compID, fmpStmts)
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_FMP_FACTS", "SUCCESS", fmt.Sprintf("Fetched FMP fundamentals (%d metrics stored)", count))
			} else if err != nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_FMP_FACTS", "ERROR", fmt.Sprintf("FMP fallback failed for %s: %v", tickerStr, err))
			}
		}
		_ = wp.db.SetCategoryLastUpdated(ctx, compID, "fundamentals")
	}

	// 4. Valuation & Ratios
	if wp.shouldUpdateCategory(ctx, compID, "valuation", ttlValuation, force) {
		ratios, err := wp.clients.FetchFinnhubValuationRatios(ctx, tickerStr)
		if err == nil && ratios != nil {
			vr := &db.ValuationRatio{
				PERatio:         ratios.PERatio,
				PBRatio:         ratios.PBRatio,
				PSRatio:         ratios.PSRatio,
				GrossMargin:     ratios.GrossMargin,
				OperatingMargin: ratios.OperatingMargin,
				NetMargin:       ratios.NetMargin,
				ROE:             ratios.ROE,
				ROA:             ratios.ROA,
				DebtToEquity:    ratios.DebtToEquity,
			}
			if err := wp.db.InsertValuationRatios(ctx, compID, vr); err != nil {
				log.Printf("[Worker] Error inserting valuation ratios for %s: %v", tickerStr, err)
			} else {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_VALUATION_RATIOS", "SUCCESS", fmt.Sprintf("P/E: %.2f, P/B: %.2f, Net Margin: %.2f%%", ratios.PERatio, ratios.PBRatio, ratios.NetMargin))
				_ = wp.db.SetCategoryLastUpdated(ctx, compID, "valuation")
			}
		} else if err != nil {
			log.Printf("[Worker] Error fetching valuation ratios for %s: %v", tickerStr, err)
		}
	}

	// 5. Dividends & Corporate Actions
	if wp.shouldUpdateCategory(ctx, compID, "dividends_splits", ttlQuarterly, force) {
		divs, err := wp.clients.FetchFinnhubDividends(ctx, tickerStr)
		if err == nil && len(divs) > 0 {
			var dbDivs []db.Dividend
			for _, d := range divs {
				dbDivs = append(dbDivs, db.Dividend{
					ExDate:      d.ExDate,
					PaymentDate: d.PaymentDate,
					RecordDate:  d.RecordDate,
					Amount:      d.Amount,
					Currency:    d.Currency,
					Frequency:   d.Frequency,
				})
			}
			if err := wp.db.InsertDividendsBatch(ctx, compID, dbDivs); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_DIVIDENDS", "SUCCESS", fmt.Sprintf("Stored %d dividend events", len(dbDivs)))
			}
		}

		splits, err := wp.clients.FetchFinnhubSplits(ctx, tickerStr)
		if err == nil && len(splits) > 0 {
			var dbSplits []db.StockSplit
			for _, s := range splits {
				dbSplits = append(dbSplits, db.StockSplit{
					ExecutionDate: s.ExecutionDate,
					FromFactor:    s.FromFactor,
					ToFactor:      s.ToFactor,
				})
			}
			if err := wp.db.InsertStockSplitsBatch(ctx, compID, dbSplits); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_SPLITS", "SUCCESS", fmt.Sprintf("Stored %d split events", len(dbSplits)))
			}
		}
		_ = wp.db.SetCategoryLastUpdated(ctx, compID, "dividends_splits")
	}

	// 6. Historical EOD Time Series
	now := time.Now()
	if wp.shouldUpdateCategory(ctx, compID, "historical", ttlHistorical, force) {
		startDate := now.AddDate(-1, 0, 0).Format("2006-01-02")
		endDate := now.Format("2006-01-02")
		histPrices, err := wp.clients.FetchTiingoHistoricalPrices(ctx, tickerStr, startDate, endDate)
		if err == nil && len(histPrices) > 0 {
			var dbHist []db.HistoricalPrice
			for _, p := range histPrices {
				dbHist = append(dbHist, db.HistoricalPrice{
					Date:          p.Date,
					OpenPrice:     p.OpenPrice,
					HighPrice:     p.HighPrice,
					LowPrice:      p.LowPrice,
					ClosePrice:    p.ClosePrice,
					AdjClosePrice: p.AdjClosePrice,
					Volume:        p.Volume,
				})
			}
			if err := wp.db.InsertHistoricalPricesBatch(ctx, compID, dbHist); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_HISTORICAL_PRICES", "SUCCESS", fmt.Sprintf("Stored %d historical price bars", len(dbHist)))
				_ = wp.db.SetCategoryLastUpdated(ctx, compID, "historical")
			}
		} else if err != nil {
			log.Printf("[Worker] Error fetching historical prices for %s: %v", tickerStr, err)
		}
	}

	// 7. Analyst Estimates & Earnings Calendars
	if wp.shouldUpdateCategory(ctx, compID, "analyst_earnings", ttlAnalyst, force) {
		estimates, err := wp.clients.FetchFinnhubAnalystEstimates(ctx, tickerStr)
		if err == nil && len(estimates) > 0 {
			var dbEst []db.AnalystEstimate
			for _, e := range estimates {
				dbEst = append(dbEst, db.AnalystEstimate{
					Period:     e.Period,
					StrongBuy:  e.StrongBuy,
					Buy:        e.Buy,
					Hold:       e.Hold,
					Sell:       e.Sell,
					StrongSell: e.StrongSell,
				})
			}
			if err := wp.db.InsertAnalystEstimatesBatch(ctx, compID, dbEst); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_ANALYST_ESTIMATES", "SUCCESS", fmt.Sprintf("Stored %d analyst estimate periods", len(dbEst)))
			}
		}

		earnings, err := wp.clients.FetchFinnhubEarningsCalendar(ctx, tickerStr)
		if err == nil && len(earnings) > 0 {
			var dbEC []db.EarningsCalendar
			for _, ec := range earnings {
				dbEC = append(dbEC, db.EarningsCalendar{
					Date:            ec.Date,
					Quarter:         ec.Quarter,
					Year:            ec.Year,
					EPSEstimate:     ec.EPSEstimate,
					EPSActual:       ec.EPSActual,
					RevenueEstimate: ec.RevenueEstimate,
					RevenueActual:   ec.RevenueActual,
				})
			}
			if err := wp.db.InsertEarningsCalendarBatch(ctx, compID, dbEC); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_EARNINGS_CALENDAR", "SUCCESS", fmt.Sprintf("Stored %d earnings events", len(dbEC)))
			}
		}
		_ = wp.db.SetCategoryLastUpdated(ctx, compID, "analyst_earnings")
	}

	// 8. Company News & Sentiment
	if wp.shouldUpdateCategory(ctx, compID, "news", ttlNews, force) {
		endDate := now.Format("2006-01-02")
		newsFrom := now.AddDate(0, 0, -30).Format("2006-01-02")
		newsList, err := wp.clients.FetchFinnhubCompanyNews(ctx, tickerStr, newsFrom, endDate)
		if err == nil && len(newsList) > 0 {
			var dbNews []db.CompanyNews
			for _, n := range newsList {
				dbNews = append(dbNews, db.CompanyNews{
					NewsID:         n.NewsID,
					Headline:       n.Headline,
					Summary:        n.Summary,
					Source:         n.Source,
					URL:            n.URL,
					PublishedAt:    n.PublishedAt,
					SentimentScore: n.SentimentScore,
				})
			}
			if err := wp.db.InsertCompanyNewsBatch(ctx, compID, dbNews); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_COMPANY_NEWS", "SUCCESS", fmt.Sprintf("Stored %d news items", len(dbNews)))
				_ = wp.db.SetCategoryLastUpdated(ctx, compID, "news")
			}
		}
	}

	// 9. Insider Trading & Institutional Ownership
	if wp.shouldUpdateCategory(ctx, compID, "insider_institutional", ttlQuarterly, force) {
		insiderTxs, err := wp.clients.FetchFinnhubInsiderTransactions(ctx, tickerStr)
		if err == nil && len(insiderTxs) > 0 {
			var dbInsider []db.InsiderTransaction
			for _, it := range insiderTxs {
				dbInsider = append(dbInsider, db.InsiderTransaction{
					Name:             it.Name,
					ShareCount:       it.ShareCount,
					ChangeShares:     it.ChangeShares,
					FilingDate:       it.FilingDate,
					TransactionCode:  it.TransactionCode,
					TransactionPrice: it.TransactionPrice,
				})
			}
			if err := wp.db.InsertInsiderTransactionsBatch(ctx, compID, dbInsider); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_INSIDER_TRANSACTIONS", "SUCCESS", fmt.Sprintf("Stored %d insider transactions", len(dbInsider)))
			}
		}

		instOwnership, err := wp.clients.FetchFinnhubInstitutionalOwnership(ctx, tickerStr)
		if err == nil && len(instOwnership) > 0 {
			var dbInst []db.InstitutionalOwnership
			for _, io := range instOwnership {
				dbInst = append(dbInst, db.InstitutionalOwnership{
					InvestorName: io.InvestorName,
					SharesHeld:   io.SharesHeld,
					ChangeShares: io.ChangeShares,
					Value:        io.Value,
					Period:       io.Period,
				})
			}
			if err := wp.db.InsertInstitutionalOwnershipBatch(ctx, compID, dbInst); err == nil {
				_ = wp.db.LogAction(ctx, tickerStr, "FETCH_INSTITUTIONAL_OWNERSHIP", "SUCCESS", fmt.Sprintf("Stored %d institutional ownership entries", len(dbInst)))
			}
		}
		_ = wp.db.SetCategoryLastUpdated(ctx, compID, "insider_institutional")
	}

	// Reset force_full_refresh flag for this ticker
	_ = wp.db.SetWatchitemForceRefresh(ctx, tickerStr, false)

	// Mark status completed
	_ = wp.db.UpdateWatchitemStatus(ctx, tickerStr, "completed")
	_ = wp.db.LogAction(ctx, tickerStr, "JOB_COMPLETE", "SUCCESS", fmt.Sprintf("Finished processing job for %s", tickerStr))

	if wp.broadcaster != nil {
		wp.broadcaster.BroadcastLog(tickerStr, "SUCCESS", fmt.Sprintf("Completed aggregation for %s", tickerStr))
		consolidated, err := wp.db.GetConsolidatedData(ctx, tickerStr)
		if err == nil {
			wp.broadcaster.BroadcastUpdate(tickerStr, consolidated)
		}
	}
}

func extractAndStoreSECFacts(ctx context.Context, database *db.DB, companyID int64, facts *clients.SECCompanyFacts) int {
	if facts == nil || facts.Facts == nil {
		return 0
	}
	usGaap, ok := facts.Facts["us-gaap"].(map[string]interface{})
	if !ok {
		return 0
	}

	metricsToExtract := map[string]string{
		"Revenues":                                            "Revenues",
		"SalesRevenueNet":                                     "Revenues",
		"RevenueFromContractWithCustomerExcludingAssessedTax": "Revenues",
		"NetIncomeLoss":                                       "NetIncome",
		"OperatingIncomeLoss":                                 "OperatingIncome",
		"Assets":                                              "TotalAssets",
		"EarningsPerShareBasic":                               "EPS",
		"EarningsPerShareDiluted":                             "EPS",
		"GrossProfit":                                         "GrossProfit",
		"Liabilities":                                         "TotalLiabilities",
	}

	var batch []db.FundamentalItem

	// Helper to extract series for a given concept
	extractSeries := func(conceptKey string) map[string]float64 {
		result := make(map[string]float64)
		conceptData, ok := usGaap[conceptKey].(map[string]interface{})
		if !ok {
			return result
		}
		units, ok := conceptData["units"].(map[string]interface{})
		if !ok {
			return result
		}
		for _, unitItems := range units {
			itemList, ok := unitItems.([]interface{})
			if !ok {
				continue
			}
			startIdx := 0
			if len(itemList) > 5 {
				startIdx = len(itemList) - 5
			}
			for i := startIdx; i < len(itemList); i++ {
				item, ok := itemList[i].(map[string]interface{})
				if !ok {
					continue
				}
				val, valOk := item["val"].(float64)
				fy, fyOk := item["fy"].(float64)
				fp, fpOk := item["fp"].(string)

				if !valOk {
					continue
				}
				period := "N/A"
				if fyOk && fpOk {
					period = fmt.Sprintf("%.0f-%s", fy, fp)
				} else if fyOk {
					period = fmt.Sprintf("%.0f", fy)
				} else if fpOk {
					period = fp
				}
				result[period] = val
			}
		}
		return result
	}

	for conceptKey, label := range metricsToExtract {
		series := extractSeries(conceptKey)
		for period, val := range series {
			batch = append(batch, db.FundamentalItem{
				Period:     period,
				MetricName: label,
				Value:      val,
			})
		}
	}

	// Calculate Free Cash Flow natively: Operating Cash Flow - CapEx
	ocfConcepts := []string{
		"NetCashProvidedByUsedInOperatingActivities",
		"NetCashProvidedByOperatingActivities",
		"NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
	}
	capexConcepts := []string{
		"PaymentsToAcquirePropertyPlantAndEquipment",
		"PaymentsForPropertyPlantAndEquipment",
	}

	ocfMap := make(map[string]float64)
	for _, concept := range ocfConcepts {
		res := extractSeries(concept)
		for k, v := range res {
			ocfMap[k] = v
		}
	}

	capexMap := make(map[string]float64)
	for _, concept := range capexConcepts {
		res := extractSeries(concept)
		for k, v := range res {
			capexMap[k] = v
		}
	}

	for period, ocf := range ocfMap {
		if capex, exists := capexMap[period]; exists {
			batch = append(batch, db.FundamentalItem{
				Period:     period,
				MetricName: "FreeCashFlow",
				Value:      ocf - capex,
			})
		}
	}

	if len(batch) > 0 {
		if err := database.InsertFundamentalsBatch(ctx, companyID, batch); err == nil {
			return len(batch)
		}
	}
	return 0
}

func extractAndStoreFMPFacts(ctx context.Context, database *db.DB, companyID int64, stmts []clients.FMPIncomeStatement) int {
	var batch []db.FundamentalItem

	for _, stmt := range stmts {
		period := stmt.Period
		if stmt.CalendarYear != "" && stmt.Period != "" {
			period = fmt.Sprintf("%s-%s", stmt.CalendarYear, stmt.Period)
		} else if stmt.Date != "" {
			period = stmt.Date
		}
		if period == "" {
			period = "N/A"
		}

		epsVal := stmt.EpsDiluted
		if epsVal == 0 {
			epsVal = stmt.Eps
		}

		if stmt.Revenue != 0 {
			batch = append(batch, db.FundamentalItem{Period: period, MetricName: "Revenues", Value: stmt.Revenue})
		}
		if stmt.GrossProfit != 0 {
			batch = append(batch, db.FundamentalItem{Period: period, MetricName: "GrossProfit", Value: stmt.GrossProfit})
		}
		if stmt.OperatingIncome != 0 {
			batch = append(batch, db.FundamentalItem{Period: period, MetricName: "OperatingIncome", Value: stmt.OperatingIncome})
		}
		if stmt.NetIncome != 0 {
			batch = append(batch, db.FundamentalItem{Period: period, MetricName: "NetIncome", Value: stmt.NetIncome})
		}
		if epsVal != 0 {
			batch = append(batch, db.FundamentalItem{Period: period, MetricName: "EPS", Value: epsVal})
		}
	}

	if len(batch) > 0 {
		if err := database.InsertFundamentalsBatch(ctx, companyID, batch); err == nil {
			return len(batch)
		}
	}
	return 0
}
