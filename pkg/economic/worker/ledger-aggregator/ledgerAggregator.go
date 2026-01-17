package ledgeraggregator

import (
	"context"
	"fmt"
	"grapthway/pkg/crypto"
	penaltybox "grapthway/pkg/economic/worker/penalty-box"
	"grapthway/pkg/ledger"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/model"
	"log"
	"sync"
	"time"
)

type LedgerAggregator struct {
	ledgerClient *ledger.Client
	penaltyBox   *penaltybox.DeveloperPenaltyBox
	feeChannel   chan model.FeeInfo
	lock         sync.Mutex
	debitBatch   map[string]uint64
	rewardBatch  map[string]uint64
	batchSize    int
	maxDelay     time.Duration
	ticker       *time.Ticker
	ctx          context.Context
	nodeIdentity *crypto.Identity
}

func NewLedgerAggregator(ctx context.Context, client *ledger.Client, pbox *penaltybox.DeveloperPenaltyBox, batchSize int, maxDelay time.Duration, nodeIdentity *crypto.Identity) *LedgerAggregator {
	agg := &LedgerAggregator{
		ledgerClient: client,
		penaltyBox:   pbox,
		feeChannel:   make(chan model.FeeInfo, 100000),
		debitBatch:   make(map[string]uint64),
		rewardBatch:  make(map[string]uint64),
		batchSize:    batchSize,
		maxDelay:     maxDelay,
		ticker:       time.NewTicker(maxDelay),
		ctx:          ctx,
		nodeIdentity: nodeIdentity,
	}
	return agg
}

func (agg *LedgerAggregator) Start() {
	log.Println("LEDGER AGGREGATOR: Starting service...")
	go func() {
		for {
			select {
			case fee := <-agg.feeChannel:
				agg.lock.Lock()
				agg.debitBatch[fee.DeveloperID] += fee.Cost
				agg.rewardBatch[fee.NodeOperatorID] += (fee.Cost * 8) / 10 // 80% reward
				batchCount := len(agg.debitBatch)
				agg.lock.Unlock()

				if batchCount >= agg.batchSize {
					agg.flushBatch()
				}
			case <-agg.ticker.C:
				agg.flushBatch()
			case <-agg.ctx.Done():
				agg.ticker.Stop()
				log.Println("LEDGER AGGREGATOR: Shutting down.")
				return
			}
		}
	}()
}

func (agg *LedgerAggregator) flushBatch() {
	agg.lock.Lock()
	if len(agg.debitBatch) == 0 {
		agg.lock.Unlock()
		return
	}

	debitsToProcess := agg.debitBatch
	agg.debitBatch = make(map[string]uint64)
	agg.rewardBatch = make(map[string]uint64)
	agg.lock.Unlock()

	log.Printf("LEDGER AGGREGATOR: Processing batch with %d developer debits.", len(debitsToProcess))

	devIDs := make([]string, 0, len(debitsToProcess))
	for devID := range debitsToProcess {
		devIDs = append(devIDs, devID)
	}

	accountStates, err := agg.ledgerClient.GetLiveAccountStatesBatch(devIDs)
	if err != nil {
		log.Printf("LEDGER AGGREGATOR: ERROR: Failed to get batch account states: %v. Re-queuing debits.", err)
		agg.lock.Lock()
		for devID, amount := range debitsToProcess {
			agg.debitBatch[devID] += amount
		}
		agg.lock.Unlock()
		return
	}

	go func(debits map[string]uint64) {
		now := time.Now()
		var totalReward uint64

		for devID, totalDebit := range debits {
			state, ok := accountStates[devID]
			if !ok || state.Balance < totalDebit {
				agg.penaltyBox.Add(devID)
				continue
			}

			totalReward += (totalDebit * 8) / 10

			debitTx := types.Transaction{
				ID:        fmt.Sprintf("tx-debit-%s-%d", devID, now.UnixNano()+int64(len(devID))),
				Type:      model.DebitTransaction,
				From:      devID,
				Amount:    totalDebit,
				Timestamp: now,
				CreatedAt: now,
			}

			ctx, cancel := context.WithTimeout(agg.ctx, 2*time.Second)
			if _, err := agg.ledgerClient.ProcessAggregatorBatchTransaction(ctx, debitTx); err != nil {
				log.Printf("LEDGER AGGREGATOR: ERROR: Failed to submit debit transaction for %s: %v", devID, err)
			}
			cancel()
		}

		if totalReward > 0 {
			rewardTx := types.Transaction{
				ID:        fmt.Sprintf("tx-reward-%s-%d", agg.nodeIdentity.Address, now.UnixNano()),
				Type:      model.RewardTransaction,
				To:        agg.nodeIdentity.Address,
				Amount:    totalReward,
				Timestamp: now,
				CreatedAt: now,
			}

			ctx, cancel := context.WithTimeout(agg.ctx, 2*time.Second)
			if _, err := agg.ledgerClient.ProcessAggregatorBatchTransaction(ctx, rewardTx); err != nil {
				log.Printf("LEDGER AGGREGATOR: ERROR: Failed to submit aggregate reward transaction: %v", err)
			}
			cancel()
		}
	}(debitsToProcess)
}

func (agg *LedgerAggregator) SubmitFee(fee model.FeeInfo) bool {
	select {
	case agg.feeChannel <- fee:
		return true
	default:
		return false
	}
}
