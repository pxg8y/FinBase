package worker

import (
	"context"
	"fmt"
	"log"
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
	Ticker   string
	Priority int
}

type WorkerPool struct {
	db          *db.DB
	clients     *clients.ClientManager
	broadcaster SSEBroadcaster

	jobChan    chan Job
	numWorkers int
	stopChan   chan struct{}
	wg         sync.WaitGroup
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
	}
}

func (wp *WorkerPool) Start(ctx context.Context) {
	// Start Dispatcher
	go wp.dispatcher(ctx)

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
				case wp.jobChan <- Job{Ticker: item.Ticker, Priority: item.Priority}:
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
			wp.processJob(ctx, job)
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, job Job) {
	tickerStr := job.Ticker

	logMsg := fmt.Sprintf("Starting processing for ticker %s", tickerStr)
	log.Printf("[Worker] %s", logMsg)
	_ = wp.db.UpdateWatchitemStatus(ctx, tickerStr, "processing")
	if wp.broadcaster != nil {
		wp.broadcaster.BroadcastLog(tickerStr, "PROCESSING", logMsg)
	}

	var cik, companyName, sector, exchange, logoURL string
	var outstandingShares float64

	// 1. Try Finnhub Profile first for company basic info
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

	// 2. Try OpenFIGI for identifier mapping / sector backup
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

	// 3. SEC CIK lookup with fallback to DB
	secCIK, err := wp.clients.FetchSECCIKForTicker(ctx, tickerStr)
	if err == nil && secCIK != "" {
		cik = clients.FormatCIK(secCIK)
	} else {
		// Fallback: Check if CIK already exists in database for this ticker
		if existing, err := wp.db.GetConsolidatedData(ctx, tickerStr); err == nil && existing != nil && existing.Company.CIK != "" {
			cik = clients.FormatCIK(existing.Company.CIK)
		}
	}

	// Upsert Company record in DB
	compID, err := wp.db.UpsertCompany(ctx, &db.Company{
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

	// 4. Fetch market data using Waterfall fallback (Finnhub -> Tiingo -> Twelve Data)
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
		}
	} else if err != nil {
		log.Printf("[Worker] Error fetching market data waterfall for %s: %v", tickerStr, err)
		_ = wp.db.LogAction(ctx, tickerStr, "FETCH_QUOTE", "ERROR", fmt.Sprintf("Market data failed: %v", err))
	}

	// 5. Fetch SEC EDGAR XBRL facts if CIK available; fallback to FMP if CIK missing/invalid or SEC fails
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
		// Fallback to Financial Modeling Prep (FMP)
		fmpStmts, err := wp.clients.FetchFMPIncomeStatement(ctx, tickerStr)
		if err == nil && len(fmpStmts) > 0 {
			count := extractAndStoreFMPFacts(ctx, wp.db, compID, fmpStmts)
			_ = wp.db.LogAction(ctx, tickerStr, "FETCH_FMP_FACTS", "SUCCESS", fmt.Sprintf("Fetched FMP fundamentals (%d metrics stored)", count))
		} else if err != nil {
			_ = wp.db.LogAction(ctx, tickerStr, "FETCH_FMP_FACTS", "ERROR", fmt.Sprintf("FMP fallback failed for %s: %v", tickerStr, err))
		}
	}

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
