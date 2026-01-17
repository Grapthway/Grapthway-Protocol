package dependency

import (
	"grapthway/pkg/crypto"
	"grapthway/pkg/dht"
	ledgeraggregator "grapthway/pkg/economic/worker/ledger-aggregator"
	penaltybox "grapthway/pkg/economic/worker/penalty-box"
	"grapthway/pkg/ledger"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	"grapthway/pkg/p2p"
	"grapthway/pkg/rest/middleware"
	"grapthway/pkg/workflow"
	"net/http"
	"time"
)

// Dependencies holds all application dependencies
type Dependencies struct {
	LedgerClient           *ledger.Client
	HardwareMonitor        *monitoring.Monitor
	NetworkHardwareMonitor *monitoring.NetworkHardwareMonitor
	P2PNode                *p2p.Node
	StorageService         model.Storage
	PenaltyBox             *penaltybox.DeveloperPenaltyBox
	NodeIdentity           *crypto.Identity
	LedgerAggregator       *ledgeraggregator.LedgerAggregator
	Logger                 *logging.Logger
	DhtService             *dht.DHT
	WorkflowEngine         *workflow.Engine
	StartTime              time.Time
	MiddlewareChain        *middleware.MiddlewareChain
}

// NewDependencies creates and initializes all application dependencies
func NewDependencies(
	ledgerClient *ledger.Client,
	hardwareMonitor *monitoring.Monitor,
	networkHardwareMonitor *monitoring.NetworkHardwareMonitor,
	p2pNode *p2p.Node,
	storageService model.Storage,
	penaltyBox *penaltybox.DeveloperPenaltyBox,
	nodeIdentity *crypto.Identity,
	ledgerAggregator *ledgeraggregator.LedgerAggregator,
	logger *logging.Logger,
	dhtService *dht.DHT,
	workflowEngine *workflow.Engine,
	startTime time.Time,
	multipartMiddleware func(http.Handler) http.Handler,
) *Dependencies {
	// Create middleware chain
	MiddlewareChain := middleware.NewChain(&middleware.EconomicDeps{
		LedgerClient:     ledgerClient,
		HardwareMonitor:  hardwareMonitor,
		P2PNode:          p2pNode,
		StorageService:   storageService,
		PenaltyBox:       penaltyBox,
		NodeIdentity:     nodeIdentity,
		LedgerAggregator: ledgerAggregator,
	}, multipartMiddleware)

	return &Dependencies{
		LedgerClient:           ledgerClient,
		HardwareMonitor:        hardwareMonitor,
		NetworkHardwareMonitor: networkHardwareMonitor,
		P2PNode:                p2pNode,
		StorageService:         storageService,
		PenaltyBox:             penaltyBox,
		NodeIdentity:           nodeIdentity,
		LedgerAggregator:       ledgerAggregator,
		Logger:                 logger,
		DhtService:             dhtService,
		WorkflowEngine:         workflowEngine,
		StartTime:              startTime,
		MiddlewareChain:        MiddlewareChain,
	}
}
