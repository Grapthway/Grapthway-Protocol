package penaltybox

import (
	"context"
	"grapthway/pkg/ledger"
	"grapthway/pkg/model"
	"log"
	"sync"
	"time"
)

const (
	PenaltyCheckInterval      = 1 * time.Minute
	MinBalanceToEscapePenalty = 100 * model.GCU_MICRO_UNIT
)

type DeveloperPenaltyBox struct {
	penalizedDevs sync.Map
	ledger        *ledger.Client
	ctx           context.Context
}

func NewDeveloperPenaltyBox(ctx context.Context, ledger *ledger.Client) *DeveloperPenaltyBox {
	return &DeveloperPenaltyBox{
		ledger: ledger,
		ctx:    ctx,
	}
}

func (pb *DeveloperPenaltyBox) Add(developerID string) {
	log.Printf("PENALTY BOX: Adding developer %s due to insufficient funds.", developerID)
	pb.penalizedDevs.Store(developerID, time.Now())
}

func (pb *DeveloperPenaltyBox) IsPenalized(developerID string) bool {
	_, ok := pb.penalizedDevs.Load(developerID)
	return ok
}

func (pb *DeveloperPenaltyBox) StartRecheckRoutine() {
	ticker := time.NewTicker(PenaltyCheckInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				pb.recheckBalances()
			case <-pb.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (pb *DeveloperPenaltyBox) recheckBalances() {
	pb.penalizedDevs.Range(func(key, value interface{}) bool {
		devID := key.(string)
		balance, err := pb.ledger.CheckBalance(devID)
		if err != nil {
			return true
		}
		if uint64(balance*model.GCU_MICRO_UNIT) >= MinBalanceToEscapePenalty {
			log.Printf("PENALTY BOX: Developer %s has sufficient funds. Removing from penalty box.", devID)
			pb.penalizedDevs.Delete(devID)
		}
		return true
	})
}
