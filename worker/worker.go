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
			// Fetch highest priority, oldest updated watchitem
			item, err := wp.db.FetchNextWatchitem(ctx)
			if err != nil {
				log.Printf("[Dispatcher] Error fetching watchitem: %v", err)
				continue
			}
			if item != nil {
				oldStatus := item.Status
				if err := wp.db.UpdateWatchitemStatus(ctx, item.Ticker, "queued"); err != nil {
					log.Printf("[Dispatcher] Error updating status for ticker %s: %v", item.Ticker, err)
					continue
				}
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

	var cik, companyName, sector string

	// 1. Try Finnhub Profile first for company basic info
	profile, err := wp.clients.FetchFinnhubProfile(ctx, tickerStr)
	if err == nil && profile != nil {
		companyName = profile.Name
		sector = profile.FinnhubIndustry
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
	}

	// 3. SEC CIK lookup
	secCIK, err := wp.clients.FetchSECCIKForTicker(ctx, tickerStr)
	if err == nil {
		cik = clients.FormatCIK(secCIK)
	} else if cik != "" {
		cik = clients.FormatCIK(cik)
	}

	// Upsert Company record in DB
	compID, err := wp.db.UpsertCompany(ctx, &db.Company{
		Ticker: tickerStr,
		CIK:    cik,
		Name:   companyName,
		Sector: sector,
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

	// 4. Fetch market data from Finnhub Quote
	quote, err := wp.clients.FetchFinnhubQuote(ctx, tickerStr)
	if err == nil && quote != nil {
		if err := wp.db.InsertMarketData(ctx, compID, quote.CurrentPrice, 0); err != nil {
			log.Printf("[Worker] Error inserting market data for %s: %v", tickerStr, err)
		} else {
			_ = wp.db.LogAction(ctx, tickerStr, "FETCH_QUOTE", "SUCCESS", fmt.Sprintf("Price: $%.2f", quote.CurrentPrice))
		}
	}

	// 5. Fetch SEC EDGAR XBRL facts if CIK available
	if cik != "" {
		facts, err := wp.clients.FetchSECFacts(ctx, cik)
		if err == nil && facts != nil {
			count := extractAndStoreSECFacts(ctx, wp.db, compID, facts)
			_ = wp.db.LogAction(ctx, tickerStr, "FETCH_SEC_FACTS", "SUCCESS", fmt.Sprintf("Fetched XBRL facts for CIK %s (%d metrics stored)", cik, count))
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
	}

	insertedCount := 0
	for conceptKey, label := range metricsToExtract {
		conceptData, ok := usGaap[conceptKey].(map[string]interface{})
		if !ok {
			continue
		}
		units, ok := conceptData["units"].(map[string]interface{})
		if !ok {
			continue
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

				if err := database.InsertFundamental(ctx, companyID, period, label, val); err == nil {
					insertedCount++
				}
			}
		}
	}
	return insertedCount
}
