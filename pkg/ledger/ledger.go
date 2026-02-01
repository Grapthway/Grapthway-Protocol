package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/json-iterator/go"
	jsoniter "github.com/json-iterator/go"

	"grapthway/pkg/crypto"
	"grapthway/pkg/ledger/state"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	"grapthway/pkg/p2p"
	pb "grapthway/pkg/proto"
	modeltoproto "grapthway/pkg/proto/converter/model-to-proto"
	prototomodel "grapthway/pkg/proto/converter/proto-to-model"
	"grapthway/pkg/proto/converter/utils"

	"github.com/dgraph-io/badger/v4"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	UnbondingPeriod       = 24 * time.Hour
	TransactionExpiration = 60 * time.Second
	MaxBlockTime          = 2500 * time.Millisecond
	MinTxsPerBlock        = 10000
	PreProposalTimeout    = 1500 * time.Millisecond
	accountPrefix         = "acct-"
	transactionPrefix     = "tx-"
	failedTxPrefix        = "failed-tx-"
	rawTxPrefix           = "raw-tx-"
	metaPrefix            = "meta-"
	blockPrefix           = "block-h-"
	lastBlockKey          = "last-block"
	processedTxIDPrefix   = "proc-"
	validatorSetKey       = "validator-set"
	GCUPerCore            = 100.0
)

var (
	ErrUnresolvedNonceGap    = errors.New("block contains unresolved nonce gaps")
	ErrFutureNonce           = errors.New("transaction has a future nonce")
	ErrOldNonce              = errors.New("transaction has an old or duplicate nonce")
	ErrBatchMismatch         = errors.New("leader's batch transaction does not match local validation")
	ErrInvalidBatchInclusion = errors.New("block contains raw batchable transactions that should have been aggregated")
	ErrMissingBatchTxs       = errors.New("validator is missing raw transactions required for batch validation")
	ErrSimulationFailed      = errors.New("simulated block execution failed")
	ErrProposalNotValidated  = errors.New("proposal was not pre-validated or has expired from cache")
)

type ValidatedProposalCacheEntry struct {
	BatchedTransactions []types.Transaction
	ConsumedTxIDs       map[string]struct{}
	RawTransactions     map[string]types.Transaction
	Timestamp           time.Time
}

type batchValidationResult struct {
	recreatedBatch types.Transaction
	consumedIDs    map[string]struct{}
	err            error
}

// TransactionDetail provides a comprehensive, unified view of a transaction
type TransactionDetail struct {
	types.Transaction
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
	// Token-specific fields for unified history
	TokenTicker     string  `json:"tokenTicker,omitempty"`
	TokenName       string  `json:"tokenName,omitempty"`
	TokenDecimals   uint8   `json:"tokenDecimals,omitempty"`
	TokenOperation  string  `json:"tokenOperation,omitempty"`
	AmountFormatted float64 `json:"amountFormatted,omitempty"`
}

type TransactionHistoryPage struct {
	Transactions []TransactionDetail
	TotalRecords int64
	TotalPages   int
	CurrentPage  int
}

type historyEntry struct {
	Timestamp    time.Time
	TxID         string
	IsTokenTx    bool
	TokenHistory *types.TokenHistory
}

// mapTokenOpToTxType converts a TokenOperation to a TransactionType
func mapTokenOpToTxType(op types.TokenOperation) types.TransactionType {
	switch op {
	case types.OpCreate:
		return model.TokenCreateTransaction
	case types.OpTransfer:
		return model.TokenTransferTransaction
	case types.OpMint:
		return model.TokenMintTransaction
	case types.OpBurn:
		return model.TokenBurnTransaction
	case types.OpApprove:
		return model.TokenApproveTransaction
	default:
		// Handle custom operations like CONFIG_BURN, CONFIG_MINT, CONFIG_LOCK
		opStr := string(op)
		switch {
		case strings.Contains(opStr, "CONFIG_BURN"):
			return types.TransactionType("TOKEN_CONFIG_BURN")
		case strings.Contains(opStr, "CONFIG_MINT"):
			return types.TransactionType("TOKEN_CONFIG_MINT")
		case strings.Contains(opStr, "CONFIG_LOCK"):
			return model.TokenLockTransaction
		case strings.Contains(opStr, "REVOKE"):
			return model.TokenRevokeTransaction
		default:
			return types.TransactionType(op)
		}
	}
}

// mapTxTypeToTokenOp creates a user-friendly operation name from a transaction type.
func mapTxTypeToTokenOp(txType types.TransactionType) string {
	switch txType {
	case model.TokenTransferTransaction:
		return string(types.OpTransfer)
	case model.TokenCreateTransaction:
		return string(types.OpCreate)
	case model.TokenMintTransaction:
		return string(types.OpMint)
	case model.TokenBurnTransaction:
		return string(types.OpBurn)
	case model.TokenApproveTransaction:
		return string(types.OpApprove)
	case model.TokenRevokeTransaction:
		return "REVOKE" // Consistent with token.go logic
	case model.TokenLockTransaction:
		return "CONFIG_LOCK"
	default:
		// Check for config change types used in token.go
		opStr := string(txType)
		if strings.HasPrefix(opStr, "TOKEN_CONFIG_") {
			return strings.Replace(opStr, "TOKEN_", "", 1)
		}
		return ""
	}
}

type accountLockShard struct {
	sync.RWMutex
	locks map[string]*sync.Mutex
}

type boundedMempoolShard struct {
	transactions sync.Map
	size         atomic.Int32
	maxSize      int32
}

type accountCacheShard struct {
	sync.RWMutex
	accounts map[string]*types.AccountState
}

type MempoolEntry struct {
	Transaction      types.Transaction
	Validations      map[peer.ID]struct{}
	validationLock   sync.Mutex
	IsLocallyValid   bool
	ValidationResult error
}

type FullSyncState struct {
	Accounts        map[string]types.AccountState `json:"accounts"`
	ValidatorSet    []types.ValidatorInfo         `json:"validatorSet"`
	LastBlockHash   string                        `json:"lastBlockHash"`
	LastBlockHeight uint64                        `json:"lastBlockHeight"`
	LastBlock       types.Block                   `json:"lastBlock"`
}

type HistoryRequest struct {
	Address string `json:"address"`
}

type Client struct {
	db                          *badger.DB
	node                        *p2p.Node
	ctx                         context.Context
	lastBlock                   *types.Block
	lastBlockHash               string
	lastBlockHeight             uint64
	metaLock                    sync.RWMutex
	shredStore                  map[string][]*types.BlockShred
	shredStoreLock              sync.Mutex
	hasSyncedFromNetwork        bool
	pendingFutureNonces         map[string][]types.Transaction
	processedTxDuringBlock      map[string]struct{}
	processedTxDuringBlockMutex sync.Mutex
	accountLockShards           []accountLockShard
	mempoolShards               []boundedMempoolShard
	accountCacheShards          []accountCacheShard
	isCatchingUp                atomic.Bool
	mempoolReadySignal          chan struct{}
	inFlightTxs                 map[string]types.Transaction
	inFlightTxsMutex            sync.RWMutex
	recentTransactions          map[string]types.Transaction
	recentTxMutex               sync.RWMutex
	recentTxCleanup             *time.Timer
	validatorSet                []types.ValidatorInfo
	validatorSetMutex           sync.RWMutex
	blockProcessingQueue        chan types.Block
	preValidationQueue          chan types.Transaction
	liveAccountStateCache       sync.Map
	validatedProposalCache      sync.Map
	networkHardwareMonitor      *monitoring.NetworkHardwareMonitor
	hardwareMonitor             *monitoring.Monitor
	tokenCache                  *state.TokenCache
	txPropagationState          sync.Map // map[txID]map[peerID]time.Time
	txPropagationMutex          sync.RWMutex
	currentTokenBatch           *TokenBatchOperations
	tokenBatchMutex             sync.RWMutex
}

func fnv32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func NewClient(ctx context.Context, db *badger.DB, netHwMonitor *monitoring.NetworkHardwareMonitor, localHwMonitor *monitoring.Monitor) *Client {
	numCPU := runtime.GOMAXPROCS(0)
	if numCPU == 0 {
		numCPU = 1
	}
	numShards := 4096 * (1 + numCPU)
	log.Printf("⚙️ Initializing ledger with %d shards for %d CPUs", numShards, numCPU)

	c := &Client{
		ctx:                    ctx,
		db:                     db,
		shredStore:             make(map[string][]*types.BlockShred),
		pendingFutureNonces:    make(map[string][]types.Transaction),
		processedTxDuringBlock: make(map[string]struct{}),
		mempoolReadySignal:     make(chan struct{}, 1),
		inFlightTxs:            make(map[string]types.Transaction),
		recentTransactions:     make(map[string]types.Transaction),
		validatorSet:           make([]types.ValidatorInfo, 0),
		blockProcessingQueue:   make(chan types.Block, 100000),
		preValidationQueue:     make(chan types.Transaction, 100000),
		liveAccountStateCache:  sync.Map{},
		networkHardwareMonitor: netHwMonitor,
		hardwareMonitor:        localHwMonitor,
		accountLockShards:      make([]accountLockShard, numShards),
		mempoolShards:          make([]boundedMempoolShard, numShards),
		accountCacheShards:     make([]accountCacheShard, numShards),
		tokenCache:             state.NewTokenCache(10 * time.Minute),
	}

	for i := 0; i < numShards; i++ {
		c.accountLockShards[i] = accountLockShard{
			locks: make(map[string]*sync.Mutex),
		}
		c.mempoolShards[i] = boundedMempoolShard{
			maxSize: 100000,
		}
	}

	for i := 0; i < numShards; i++ {
		c.accountCacheShards[i] = accountCacheShard{
			accounts: make(map[string]*types.AccountState),
		}
	}

	c.loadMetadata()
	log.Println("Initializing Grapthway High-Performance Ledger Client...")
	return c
}

func (c *Client) getAccountLock(address string) *sync.Mutex {
	shardIndex := fnv32(address) % uint32(len(c.accountLockShards))
	shard := &c.accountLockShards[shardIndex]
	shard.RLock()
	lock, exists := shard.locks[address]
	shard.RUnlock()
	if exists {
		return lock
	}
	shard.Lock()
	defer shard.Unlock()
	if lock, exists := shard.locks[address]; exists {
		return lock
	}
	newLock := &sync.Mutex{}
	shard.locks[address] = newLock
	return newLock
}

func (c *Client) getMempoolShard(txID string) *boundedMempoolShard {
	shardIndex := fnv32(txID) % uint32(len(c.mempoolShards))
	return &c.mempoolShards[shardIndex]
}

func (c *Client) StartNetworking(node *p2p.Node, txChan, reqChan, blockProposalChan, blockShredChan, preValidatedTxChan, nonceUpdateChan, preProposalChan, validatorUpdateChan <-chan *pubsub.Message) {
	c.node = node
	if c.node == nil {
		log.Println("LEDGER: P2P node is nil, cannot start networking.")
		return
	}
	log.Println("LEDGER: Starting networking services...")
	c.node.Host.SetStreamHandler(p2p.LedgerSyncProtocolID, c.handleLedgerSyncStream)
	c.node.Host.SetStreamHandler(p2p.HistorySyncProtocolID, c.handleHistoryRequestStream)
	c.listenForNetworkUpdates(txChan, reqChan, blockProposalChan, blockShredChan, preValidatedTxChan, nonceUpdateChan, preProposalChan, validatorUpdateChan)
	c.startPreValidationWorkers()
	go c.blockProcessingWorker()

	if c.node.Role != "worker" {
		log.Println("LEDGER: Node is a server, starting consensus routines.")
		go c.leaderBlockCreationRoutine()
		go c.genesisBlockRoutine()
	} else {
		log.Println("LEDGER: Node is a worker, skipping consensus routines.")
	}

	go c.cleanMempoolRoutine(10 * time.Second)
	go c.janitorValidatedProposalCache()
	go c.cleanPropagationStateRoutine()
	if c.node.Role != "worker" {
		go c.readySignalHeartbeat()
	}
}

// cleanPropagationStateRoutine periodically removes stale propagation entries
func (c *Client) cleanPropagationStateRoutine() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			staleThreshold := TransactionExpiration // 60 seconds

			c.txPropagationState.Range(func(key, value interface{}) bool {
				txID := key.(string)
				peerMap := value.(map[string]time.Time)

				// Check if transaction still exists in mempool
				if _, exists := c.GetMempoolTxByID(txID); !exists {
					c.txPropagationState.Delete(txID)
					return true
				}

				// Check if all propagation timestamps are stale
				allStale := true
				for _, timestamp := range peerMap {
					if now.Sub(timestamp) < staleThreshold {
						allStale = false
						break
					}
				}

				if allStale {
					c.txPropagationState.Delete(txID)
				}

				return true
			})
		}
	}
}

func (c *Client) readySignalHeartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.metaLock.RLock()
			currentHeight := c.lastBlockHeight + 1
			lastBlock := c.lastBlock
			c.metaLock.RUnlock()

			leader, err := c.calculateLeader(lastBlock, currentHeight)
			if err != nil || leader == c.node.Host.ID() {
				continue
			}

			// Send ready signal to current leader
			ctx, cancel := context.WithTimeout(c.ctx, 2*time.Second)
			c.node.SendNextBlockReady(ctx, leader, currentHeight)
			cancel()
		}
	}
}

func (c *Client) janitorValidatedProposalCache() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			c.validatedProposalCache.Range(func(k, v interface{}) bool {
				entry := v.(ValidatedProposalCacheEntry)
				if now.Sub(entry.Timestamp) > TransactionExpiration {
					c.validatedProposalCache.Delete(k)
				}
				return true
			})
		}
	}
}

func (c *Client) startPreValidationWorkers() {
	numWorkers := runtime.GOMAXPROCS(0) * 16
	if numWorkers == 0 {
		numWorkers = 8
	}
	log.Printf("Starting %d pre-validation workers...", numWorkers)
	for i := 0; i < numWorkers; i++ {
		go c.preValidationWorker()
	}
}

func (c *Client) preValidationWorker() {
	for tx := range c.preValidationQueue {
		c.runPreValidation(&tx)
	}
}

func (c *Client) blockProcessingWorker() {
	log.Println("Starting block processing worker...")
	for block := range c.blockProcessingQueue {
		if err := c.processBlock(block); err != nil {
			log.Fatalf("CRITICAL: Pipelined block processing failed for block %d: %v. Halting node to prevent state corruption.", block.Height, err)
		}
	}
}

func (c *Client) genesisBlockRoutine() {
	time.Sleep(10 * time.Second)
	c.metaLock.RLock()
	height := c.lastBlockHeight
	synced := c.hasSyncedFromNetwork
	c.metaLock.RUnlock()
	if height > 0 || synced {
		return
	}
	if c.node != nil && len(c.node.GetPeerList()) > 0 {
		return
	}

	log.Println("GENESIS: No existing state and no peers found. Initiating genesis block creation.")

	ownerAddress := c.node.OwnerAddress
	if ownerAddress == "" {
		// Fallback, though owner address should always be set
		ownerAddress = c.node.Host.ID().String()
	}

	// Create the initial funding transaction
	genesisTx := types.Transaction{
		ID:        fmt.Sprintf("tx-genesis-%s", ownerAddress),
		Type:      model.GenesisTransaction,
		From:      "network",
		To:        ownerAddress,
		Amount:    2_803_970_000_000 * model.GCU_MICRO_UNIT,
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	// Manually create the initial account state to bootstrap the validator set
	bootstrapStake := uint64(1000 * model.GCU_MICRO_UNIT)
	initialAccountState := &types.AccountState{
		Address: ownerAddress,
		Balance: genesisTx.Amount, // Start with the full genesis amount
		StakeInfo: &types.StakeInfo{
			TotalStake:  0, // Will be updated by the genesis transaction logic
			Allocations: []types.StakeAllocation{},
			Unbonding:   []types.UnbondingStake{},
		},
		Allowances: make(map[string]uint64),
	}

	// Manually apply the genesis logic to this temporary state
	c.applyGenesisStake(initialAccountState, bootstrapStake)

	// Now that the account state includes the stake, we can form the validator set
	initialValidator := types.ValidatorInfo{
		PeerID:     c.node.Host.ID(),
		TotalStake: bootstrapStake,
	}

	block := types.Block{
		ProposerID:   c.node.Host.ID().String(),
		Transactions: []types.Transaction{genesisTx},
		Timestamp:    time.Now(),
		Height:       1,
		PrevHash:     "0000000000000000000000000000000000000000000000000000000000000000",
		// CRITICAL FIX: Embed the first validator set into the genesis block itself.
		NextValidators: []types.ValidatorInfo{initialValidator},
	}
	block.Hash = c.calculateBlockHash(block)

	log.Printf("GENESIS: Created block 1 with hash %s and validator %s", block.Hash[:10], initialValidator.PeerID)

	// Process the fully-formed genesis block. This will persist the state correctly.
	if err := c.processBlock(block); err == nil {
		log.Println("GENESIS: Successfully processed and broadcasting genesis block.")
		c.broadcastBlock(block)
	} else {
		log.Fatalf("CRITICAL: Failed to process self-created genesis block: %v", err)
	}
}

// applyGenesisStake is a special function called only during the genesis block creation.
// It manually applies the initial stake for the first node to bootstrap the validator set.
func (c *Client) applyGenesisStake(account *types.AccountState, bootstrapStake uint64) {
	if account.StakeInfo == nil {
		account.StakeInfo = &types.StakeInfo{}
	}

	// This logic mirrors StakeDeposit and StakeAssign but is applied directly
	// without a transaction, as it's part of the chain's bootstrapping.

	// Equivalent of StakeDeposit
	account.StakeInfo.TotalStake += bootstrapStake

	// Equivalent of StakeAssign
	account.StakeInfo.Allocations = append(account.StakeInfo.Allocations, types.StakeAllocation{
		NodePeerID: c.node.Host.ID().String(),
		Amount:     bootstrapStake,
		LockedAt:   time.Now(),
	})
}

// storeFailedTransaction saves the details of a failed transaction and crucially,
// writes the necessary history indexes so it can be retrieved.
func (c *Client) storeFailedTransaction(tx types.Transaction, reason string) {
	detail := TransactionDetail{
		Transaction: tx,
		Status:      "Failed",
		Reason:      reason,
	}

	detailBytes, err := json.Marshal(detail)
	if err != nil {
		log.Printf("ERROR: Failed to marshal failed transaction detail %s: %v", tx.ID, err)
		return
	}

	err = c.db.Update(func(txn *badger.Txn) error {
		// Store the failed transaction details under the 'failed-tx-' prefix
		if err := txn.Set([]byte(failedTxPrefix+tx.ID), detailBytes); err != nil {
			return err
		}

		// --- FIX 3: Index failed transactions for history. ---
		// Previously, only successful transactions were indexed, making failed ones
		// invisible to history lookups. This ensures they appear in a user's transaction history.
		timestampStr := tx.Timestamp.Format(time.RFC3339Nano)
		if tx.From != "" && tx.From != "network" {
			fromKey := []byte(fmt.Sprintf("tidx-from-%s-%s-%s", tx.From, timestampStr, tx.ID))
			if err := txn.Set(fromKey, nil); err != nil {
				return err
			}
		}
		if tx.To != "" {
			toKey := []byte(fmt.Sprintf("tidx-to-%s-%s-%s", tx.To, timestampStr, tx.ID))
			if err := txn.Set(toKey, nil); err != nil {
				return err
			}
		}
		// --- END FIX 3 ---

		return nil
	})
	if err != nil {
		log.Printf("ERROR: Failed to store and index failed transaction %s: %v", tx.ID, err)
	}
}

func (c *Client) cleanMempoolRoutine(interval time.Duration) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			var totalSize int32
			var expiredCount int32
			for i := range c.mempoolShards {
				shard := &c.mempoolShards[i]
				var localExpired int32
				shard.transactions.Range(func(key, value interface{}) bool {
					id := key.(string)
					entry := value.(*MempoolEntry)
					if time.Since(entry.Transaction.CreatedAt) > TransactionExpiration {
						if isUserNonceTracked(entry.Transaction.Type) {
							c.adjustPendingSpend(entry.Transaction.From, -int64(entry.Transaction.Amount))
						}
						c.storeFailedTransaction(entry.Transaction, "Transaction expired")
						if _, deleted := shard.transactions.LoadAndDelete(id); deleted {
							shard.size.Add(-1)
							atomic.AddInt32(&localExpired, 1)
						}
					}
					return true
				})
				atomic.AddInt32(&expiredCount, localExpired)
				totalSize += shard.size.Load()
			}
			if expiredCount > 0 {
				log.Printf("DEBUG-MEMPOOL: Expired %d transactions. Total mempool size: %d", expiredCount, totalSize)
			}
		}
	}
}

func (c *Client) aggressiveMempoolCleanup() {}

func (c *Client) getOnlineValidators() []types.ValidatorInfo {
	fullValidatorSet := c.getFullValidatorSet()
	if c.node == nil {
		return []types.ValidatorInfo{}
	}
	connectedPeerSet := make(map[peer.ID]struct{})
	for _, p := range c.node.GetPeerList() {
		connectedPeerSet[p] = struct{}{}
	}
	connectedPeerSet[c.node.Host.ID()] = struct{}{}

	onlineValidators := make([]types.ValidatorInfo, 0, len(fullValidatorSet))

	var peerStats map[string]model.HardwareInfo
	if c.networkHardwareMonitor != nil {
		peerStats = c.networkHardwareMonitor.GetPeerStats()
	} else {
		peerStats = make(map[string]model.HardwareInfo)
		log.Println("WARN: Network hardware monitor is not initialized in ledger client; peer stats are unavailable for validator selection.")
	}

	for _, validator := range fullValidatorSet {
		// Rule 1: Must be connected.
		if _, isConnected := connectedPeerSet[validator.PeerID]; !isConnected {
			continue
		}

		// Rule 2: Must not be a worker.
		// For self, check role directly. For others, check gossiped stats.
		if validator.PeerID == c.node.Host.ID() {
			if c.node.Role == "worker" {
				continue
			}
		} else {
			hwInfo, hasInfo := peerStats[validator.PeerID.String()]
			if hasInfo && hwInfo.Role == "worker" {
				continue // Skip workers.
			}
		}

		// Rule 3: Must meet proportional stake.
		// Check for self
		if validator.PeerID == c.node.Host.ID() {
			localHwInfo := c.hardwareMonitor.GetHardwareInfo()
			if localHwInfo.CgroupLimits.CPUQuotaCores > 0 {
				requiredStake := uint64(localHwInfo.CgroupLimits.CPUQuotaCores * GCUPerCore * model.GCU_MICRO_UNIT)
				if validator.TotalStake >= requiredStake {
					onlineValidators = append(onlineValidators, validator)
				}
			}
			// Check for peers
		} else {
			hwInfo, hasInfo := peerStats[validator.PeerID.String()]
			if hasInfo && hwInfo.CgroupLimits.CPUQuotaCores > 0 {
				requiredStake := uint64(hwInfo.CgroupLimits.CPUQuotaCores * GCUPerCore * model.GCU_MICRO_UNIT)
				if validator.TotalStake >= requiredStake {
					onlineValidators = append(onlineValidators, validator)
				}
			}
		}
	}
	return onlineValidators
}

func (c *Client) getFullValidatorSet() []types.ValidatorInfo {
	c.validatorSetMutex.RLock()
	defer c.validatorSetMutex.RUnlock()
	// Return a copy to prevent race conditions on the slice
	validators := make([]types.ValidatorInfo, len(c.validatorSet))
	copy(validators, c.validatorSet)
	return validators
}

func (c *Client) GetValidatorSet() []types.ValidatorInfo {
	return c.getFullValidatorSet()
}

func (c *Client) calculateLeader(prevBlock *types.Block, height uint64) (peer.ID, error) {
	var validators []types.ValidatorInfo

	if prevBlock != nil && len(prevBlock.NextValidators) > 0 {
		validators = prevBlock.NextValidators
	} else {
		validators = c.getFullValidatorSet()
	}

	if len(validators) == 0 {
		if c.node != nil {
			log.Printf("LEADER-CALC: No on-chain or in-memory validators found for height %d, defaulting to self.", height)
			return c.node.Host.ID(), nil
		}
		return "", errors.New("cannot calculate leader: no validators and p2p node is not initialized")
	}

	sort.Slice(validators, func(i, j int) bool {
		return validators[i].PeerID.String() < validators[j].PeerID.String()
	})

	// Use a deterministic seed based on previous block and height.
	seed := fmt.Sprintf("%s-%d", prevBlock.Hash, height)
	h := sha256.Sum256([]byte(seed))
	val := binary.BigEndian.Uint64(h[:8])
	leaderIndex := int(val % uint64(len(validators)))

	return validators[leaderIndex].PeerID, nil
}

func (c *Client) isCurrentLeader() bool {
	if c.node == nil || c.node.Role == "worker" {
		return false
	}
	c.metaLock.RLock()
	prevBlock := c.lastBlock
	currentHeight := c.lastBlockHeight
	c.metaLock.RUnlock()

	leader, err := c.calculateLeader(prevBlock, currentHeight+1)
	if err != nil {
		log.Printf("Could not determine leader: %v", err)
		return false
	}
	return leader == c.node.Host.ID()
}

func (c *Client) leaderBlockCreationRoutine() {
	timer := time.NewTimer(MaxBlockTime)
	defer timer.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-timer.C:
			if c.isCurrentLeader() {
				if block, ok := c.proposeAndFinalizeBlock(); ok {
					c.metaLock.Lock()
					if block.Height == c.lastBlockHeight+1 {
						c.lastBlock = &block
						c.lastBlockHash = block.Hash
						c.lastBlockHeight = block.Height
						c.blockProcessingQueue <- block
					}
					c.metaLock.Unlock()
				}
			}
			timer.Reset(MaxBlockTime)
		case <-c.mempoolReadySignal:
			if c.isCurrentLeader() {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				if block, ok := c.proposeAndFinalizeBlock(); ok {
					c.metaLock.Lock()
					if block.Height == c.lastBlockHeight+1 {
						c.lastBlock = &block
						c.lastBlockHash = block.Hash
						c.lastBlockHeight = block.Height
						c.blockProcessingQueue <- block
					}
					c.metaLock.Unlock()
				}
				timer.Reset(MaxBlockTime)
			}
		}
	}
}

func (c *Client) proposeAndFinalizeBlock() (types.Block, bool) {
	start := time.Now()
	log.Printf("[PERF-LOG] START: Proposing block for height %d", c.lastBlockHeight+1)

	c.metaLock.RLock()
	currentHeight := c.lastBlockHeight
	prevBlock := c.lastBlock
	c.metaLock.RUnlock()

	numWorkers := runtime.GOMAXPROCS(0) * 16
	if numWorkers == 0 {
		numWorkers = 8
	}

	// --- SOLUTION: Use deterministic validator set for quorum calculation ---
	var activeValidators []types.ValidatorInfo
	if prevBlock != nil && len(prevBlock.NextValidators) > 0 {
		activeValidators = prevBlock.NextValidators
	} else {
		activeValidators = c.getFullValidatorSet()
	}

	if len(activeValidators) > 0 {
		quorumSize := (len(activeValidators) * 2 / 3) + 1

		readyStart := time.Now()
		readyCtx, readyCancel := context.WithTimeout(c.ctx, MaxBlockTime/2)
		defer readyCancel()

		log.Printf("[PERF-LOG] STEP 1: Waiting for 'Ready' signals. Need %d based on deterministic validator set.", quorumSize)
		if err := c.node.CollectNextBlockReady(readyCtx, currentHeight+1, quorumSize); err != nil {
			log.Printf("[PERF-LOG] END: Timed out waiting for 'Ready' signals. Duration: %v", time.Since(start))
			return types.Block{}, false
		}
		log.Printf("[PERF-LOG] STEP 2: 'Ready' signal quorum met. Duration: %v", time.Since(readyStart))
	}

	selectStart := time.Now()
	candidateTxs := c.parallelSelectTransactionsForBlock(numWorkers)
	if len(candidateTxs) == 0 {
		log.Printf("[PERF-LOG] END: No candidate transactions in mempool. Duration: %v", time.Since(start))
		return types.Block{}, false
	}
	log.Printf("[PERF-LOG] STEP 3: Selected %d transactions from mempool. Duration: %v", len(candidateTxs), time.Since(selectStart))

	batchStart := time.Now()
	// --- FIX 1: UNIFY BATCH CREATION LOGIC ---
	batchedTxs, consumedTxIDs := c.createBatchedTransactions(candidateTxs)
	log.Printf("[PERF-LOG] STEP 4: Created %d batched transactions. Duration: %v", len(batchedTxs), time.Since(batchStart))

	finalCandidateTxs := make([]types.Transaction, 0, len(candidateTxs))
	for _, tx := range candidateTxs {
		if _, consumed := consumedTxIDs[tx.ID]; !consumed {
			finalCandidateTxs = append(finalCandidateTxs, tx)
		}
	}
	finalCandidateTxs = append(finalCandidateTxs, batchedTxs...)
	if len(finalCandidateTxs) == 0 {
		log.Printf("[PERF-LOG] END: No transactions remaining after batching. Duration: %v", time.Since(start))
		return types.Block{}, false
	}

	txsWithNonceAssigned, err := c.parallelAssignNonces(finalCandidateTxs, numWorkers)
	if err != nil {
		log.Printf("LEADER: Nonce assignment failed: %v", err)
		return types.Block{}, false
	}
	log.Printf("[PERF-LOG] STEP 5: Assigned nonces to transactions. Duration: %v", time.Since(start))

	// --- CRITICAL: Build raw transaction map for batch validation ---
	var rawTxIDs []string
	rawTxMapForCache := make(map[string]types.Transaction)
	for _, tx := range candidateTxs {
		rawTxIDs = append(rawTxIDs, tx.ID)
		// Only include raw transactions that were consumed by batches
		if _, isConsumed := consumedTxIDs[tx.ID]; isConsumed {
			rawTxMapForCache[tx.ID] = tx
		}
	}

	// --- STEP 6: Transaction Propagation (with optimization) ---
	propStart := time.Now()
	if err := c.ensureTransactionPropagation(c.ctx, rawTxIDs); err != nil {
		log.Printf("WARN: Failed to ensure transaction propagation, block proposal may fail: %v", err)
		return types.Block{}, false
	}
	log.Printf("[PERF-LOG] STEP 6: Ensured transaction propagation to peers. Duration: %v", time.Since(propStart))

	// --- STEP 7: Create Pre-Proposal with Raw Transactions ---
	proposalID := fmt.Sprintf("proposal-%d-%s", time.Now().UnixNano(), c.node.Host.ID())
	preProposal := types.PreBlockProposal{
		ProposalID:      proposalID,
		ProposerID:      c.node.Host.ID().String(),
		Transactions:    txsWithNonceAssigned,
		Height:          currentHeight + 1,
		PrevHash:        prevBlock.Hash,
		RawTransactions: rawTxMapForCache, // <-- CRITICAL: Always include for batch determinism
	}

	// Cache locally
	localCacheEntry := ValidatedProposalCacheEntry{
		BatchedTransactions: batchedTxs,
		ConsumedTxIDs:       consumedTxIDs,
		RawTransactions:     rawTxMapForCache,
		Timestamp:           time.Now(),
	}
	c.validatedProposalCache.Store(proposalID, localCacheEntry)
	log.Printf("LEADER: Pre-proposal %s cached locally.", proposalID)

	// --- STEP 8: Gossip Pre-Proposal with Raw Transactions ---
	gossipStart := time.Now()
	pbTxs := make([]*pb.Transaction, len(preProposal.Transactions))
	for i, tx := range preProposal.Transactions {
		pbTxs[i] = modeltoproto.ModelToProtoTransaction(&tx)
	}

	// Convert raw transaction map to protobuf
	pbRawTxs := make(map[string]*pb.Transaction)
	for txID, tx := range preProposal.RawTransactions {
		pbRawTxs[txID] = modeltoproto.ModelToProtoTransaction(&tx)
	}

	pbPreProposal := &pb.PreBlockProposal{
		ProposalId:      preProposal.ProposalID,
		ProposerId:      preProposal.ProposerID,
		Transactions:    pbTxs,
		Height:          preProposal.Height,
		PrevHash:        preProposal.PrevHash,
		RawTransactions: pbRawTxs, // <-- CRITICAL: Include raw txs
	}
	preProposalBytes, err := proto.Marshal(pbPreProposal)
	if err != nil {
		log.Printf("LEADER: Failed to marshal pre-proposal: %v", err)
		return types.Block{}, false
	}
	c.node.Gossip(p2p.BlockPreProposalTopic, preProposalBytes)
	log.Printf("[PERF-LOG] STEP 7: Gossiped pre-block proposal. Duration: %v", time.Since(gossipStart))

	// --- STEP 9: Collect Pre-Block ACKs ---
	onlineValidators := c.getOnlineValidators()
	if len(onlineValidators) > 1 {
		ackStart := time.Now()
		quorumSize := (len(activeValidators) * 2 / 3) + 1
		ackCtx, cancelAck := context.WithTimeout(c.ctx, PreProposalTimeout)
		defer cancelAck()
		if err := c.collectAndVerifyPreBlockAcks(ackCtx, preProposal.ProposalID, quorumSize-1); err != nil {
			log.Printf("[PERF-LOG] END: Failed to collect enough pre-block ACKs for proposal %s (%d needed): %v. Duration: %v",
				proposalID, quorumSize-1, err, time.Since(start))
			return types.Block{}, false
		}
		log.Printf("[PERF-LOG] STEP 8: Collected %d pre-block ACKs. Duration: %v", quorumSize-1, time.Since(ackStart))
	}

	// --- STEP 10: Filter Valid Transactions ---
	sort.Slice(txsWithNonceAssigned, func(i, j int) bool {
		if txsWithNonceAssigned[i].From != txsWithNonceAssigned[j].From {
			return txsWithNonceAssigned[i].From < txsWithNonceAssigned[j].From
		}
		return txsWithNonceAssigned[i].Nonce < txsWithNonceAssigned[j].Nonce
	})
	validTxs, err := c.filterValidTransactions(txsWithNonceAssigned)
	if err != nil || len(validTxs) == 0 {
		log.Printf("[PERF-LOG] END: Filtering valid transactions failed or resulted in zero transactions. Duration: %v", time.Since(start))
		return types.Block{}, false
	}
	log.Printf("[PERF-LOG] STEP 9: Filtered to %d valid transactions. Duration: %v", len(validTxs), time.Since(start))

	// --- STEP 11: Create Final Block ---
	c.recalculateValidatorSet()
	nextValidatorSet := c.getOnlineValidators()

	block := types.Block{
		ProposalID:     preProposal.ProposalID,
		ProposerID:     c.node.Host.ID().String(),
		Transactions:   validTxs,
		Timestamp:      time.Now(),
		Height:         preProposal.Height,
		PrevHash:       preProposal.PrevHash,
		NextValidators: nextValidatorSet,
	}
	block.Hash = c.calculateBlockHash(block)
	c.broadcastBlock(block)
	log.Printf("[PERF-LOG] END: Successfully proposed and finalized block %d. Total Duration: %v", block.Height, time.Since(start))
	return block, true
}

func (c *Client) collectAndVerifyPreBlockAcks(ctx context.Context, proposalID string, requiredAcks int) error {
	ackChan := c.node.CollectPreBlockAcks(ctx, proposalID)
	receivedAcks := make(map[string]struct{})

	for {
		select {
		case ack, ok := <-ackChan:
			if !ok {
				return fmt.Errorf("ack channel closed prematurely")
			}

			validatorPID, err := peer.Decode(ack.ValidatorID)
			if err != nil {
				continue
			}
			pubKey, err := validatorPID.ExtractPublicKey()
			if err != nil {
				continue
			}
			hash := sha256.Sum256([]byte(proposalID))
			isValid, err := pubKey.Verify(hash[:], ack.ValidatorSig)
			if err != nil || !isValid {
				log.Printf("LEADER: Invalid signature on pre-block ACK from %s", validatorPID)
				continue
			}

			if _, exists := receivedAcks[ack.ValidatorID]; !exists {
				receivedAcks[ack.ValidatorID] = struct{}{}
				log.Printf("LEADER: Received valid pre-block ACK for %s from %s (%d/%d)", proposalID, validatorPID, len(receivedAcks), requiredAcks)
				if len(receivedAcks) >= requiredAcks {
					return nil
				}
			}
		case <-ctx.Done():
			return fmt.Errorf("context canceled while waiting for pre-block ACKs for proposal %s (received %d/%d)", proposalID, len(receivedAcks), requiredAcks)
		}
	}
}

func (c *Client) parallelSelectTransactionsForBlock(numWorkers int) []types.Transaction {
	const maxTxsPerBlock = 100000

	resultsChan := make(chan []types.Transaction, len(c.mempoolShards))
	var wg sync.WaitGroup

	shardChan := make(chan int, len(c.mempoolShards))
	for i := 0; i < len(c.mempoolShards); i++ {
		shardChan <- i
	}
	close(shardChan)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for shardIndex := range shardChan {
				shard := &c.mempoolShards[shardIndex]
				shardTxs := make([]types.Transaction, 0)
				shard.transactions.Range(func(key, value interface{}) bool {
					entry := value.(*MempoolEntry)
					if entry.IsLocallyValid {
						shardTxs = append(shardTxs, entry.Transaction)
					}
					return true
				})
				if len(shardTxs) > 0 {
					resultsChan <- shardTxs
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	allTxs := make([]types.Transaction, 0)
	for shardTxs := range resultsChan {
		allTxs = append(allTxs, shardTxs...)
	}

	sort.Slice(allTxs, func(i, j int) bool {
		return allTxs[i].ID < allTxs[j].ID
	})

	if len(allTxs) > maxTxsPerBlock {
		return allTxs[:maxTxsPerBlock]
	}

	return allTxs
}

type partialBatchResult struct {
	txType          types.TransactionType
	debits          map[string]uint64
	rewards         map[string]uint64
	aggregatedTxIDs []string
}

func (c *Client) parallelCreateBatchedTransactions(candidateTxs []types.Transaction, numWorkers int) ([]types.Transaction, map[string]struct{}) {
	if len(candidateTxs) == 0 || numWorkers <= 0 {
		return nil, make(map[string]struct{})
	}

	txsToProcess := make(chan types.Transaction, len(candidateTxs))
	resultsChan := make(chan partialBatchResult, len(candidateTxs))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localBatches := make(map[types.TransactionType]*partialBatchResult)
			for tx := range txsToProcess {
				if _, ok := localBatches[tx.Type]; !ok {
					localBatches[tx.Type] = &partialBatchResult{
						txType:          tx.Type,
						debits:          make(map[string]uint64),
						rewards:         make(map[string]uint64),
						aggregatedTxIDs: make([]string, 0),
					}
				}
				pbr := localBatches[tx.Type]
				pbr.aggregatedTxIDs = append(pbr.aggregatedTxIDs, tx.ID)
				switch tx.Type {
				case model.TransferTransaction:
					if tx.From != "" && tx.Amount > 0 {
						pbr.debits[tx.From] += tx.Amount
					}
					if tx.To != "" && tx.Amount > 0 {
						pbr.rewards[tx.To] += tx.Amount
					}
				case model.DebitTransaction:
					if tx.From != "" && tx.Amount > 0 {
						pbr.debits[tx.From] += tx.Amount
					}
				case model.RewardTransaction:
					if tx.To != "" && tx.Amount > 0 {
						pbr.rewards[tx.To] += tx.Amount
					}
				}
			}
			for _, result := range localBatches {
				resultsChan <- *result
			}
		}()
	}

	consumedTxIDs := make(map[string]struct{})
	for _, tx := range candidateTxs {
		if isBatchableTransaction(tx) {
			txsToProcess <- tx
			consumedTxIDs[tx.ID] = struct{}{}
		}
	}
	close(txsToProcess)
	wg.Wait()
	close(resultsChan)

	finalAggregates := make(map[types.TransactionType]*types.BatchAggregationPayload)
	for partial := range resultsChan {
		if _, ok := finalAggregates[partial.txType]; !ok {
			finalAggregates[partial.txType] = &types.BatchAggregationPayload{
				Debits:          make(map[string]uint64),
				Rewards:         make(map[string]uint64),
				AggregatedTxIDs: make([]string, 0),
			}
		}
		agg := finalAggregates[partial.txType]
		agg.AggregatedTxIDs = append(agg.AggregatedTxIDs, partial.aggregatedTxIDs...)
		for addr, amount := range partial.debits {
			agg.Debits[addr] += amount
		}
		for addr, amount := range partial.rewards {
			agg.Rewards[addr] += amount
		}
	}

	var finalBatchedTxs []types.Transaction
	txTypes := make([]types.TransactionType, 0, len(finalAggregates))
	for txType := range finalAggregates {
		txTypes = append(txTypes, txType)
	}
	sort.Slice(txTypes, func(i, j int) bool { return txTypes[i] < txTypes[j] })

	for _, txType := range txTypes {
		payload := finalAggregates[txType]
		sort.Strings(payload.AggregatedTxIDs)
		batchID := c.generateBatchTxID(txType, payload)
		batchTx := types.Transaction{
			ID:           batchID,
			Type:         model.BatchAggregationTransaction,
			From:         "system-batch",
			BatchPayload: payload,
		}
		finalBatchedTxs = append(finalBatchedTxs, batchTx)
	}

	return finalBatchedTxs, consumedTxIDs
}

func (c *Client) parallelAssignNonces(transactions []types.Transaction, numWorkers int) ([]types.Transaction, error) {
	txsToAssign := make(map[string][]types.Transaction)
	otherTxs := make([]types.Transaction, 0, len(transactions))
	involvedAddrs := make(map[string]struct{})

	for _, tx := range transactions {
		if isUserNonceTracked(tx.Type) && tx.Nonce == 0 {
			txsToAssign[tx.From] = append(txsToAssign[tx.From], tx)
			involvedAddrs[tx.From] = struct{}{}
		} else {
			otherTxs = append(otherTxs, tx)
		}
	}

	if len(txsToAssign) == 0 {
		return transactions, nil
	}

	addresses := make([]string, 0, len(involvedAddrs))
	for addr := range involvedAddrs {
		addresses = append(addresses, addr)
	}
	liveAccounts, err := c.GetLiveAccountStatesBatch(addresses)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch account states for nonce assignment: %w", err)
	}

	type nonceGroup struct {
		address      string
		transactions []types.Transaction
	}

	groupChan := make(chan nonceGroup, len(txsToAssign))
	resultsChan := make(chan []types.Transaction, len(txsToAssign))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range groupChan {
				acc, ok := liveAccounts[group.address]
				if !ok {
					acc = &types.AccountState{Address: group.address, Nonce: 0, PendingNonce: 0, StakeInfo: &types.StakeInfo{}}
				}
				sort.Slice(group.transactions, func(i, j int) bool {
					return group.transactions[i].CreatedAt.Before(group.transactions[j].CreatedAt)
				})

				nextNonce := acc.PendingNonce + 1
				processedGroup := make([]types.Transaction, len(group.transactions))
				for i := range group.transactions {
					group.transactions[i].Nonce = nextNonce
					processedGroup[i] = group.transactions[i]
					nextNonce++
				}
				acc.PendingNonce = nextNonce - 1
				resultsChan <- processedGroup
			}
		}()
	}

	for addr, txs := range txsToAssign {
		groupChan <- nonceGroup{address: addr, transactions: txs}
	}
	close(groupChan)
	wg.Wait()
	close(resultsChan)

	processedTxs := make([]types.Transaction, 0, len(transactions))
	processedTxs = append(processedTxs, otherTxs...)
	for res := range resultsChan {
		processedTxs = append(processedTxs, res...)
	}

	for addr, state := range liveAccounts {
		shardIndex := fnv32(addr) % uint32(len(c.accountCacheShards))
		shard := &c.accountCacheShards[shardIndex]
		shard.Lock()
		shard.accounts[addr] = state
		shard.Unlock()
	}

	return processedTxs, nil
}

func (c *Client) filterValidTransactions(transactions []types.Transaction) ([]types.Transaction, error) {
	involvedAccounts, err := c.getAccountsForTxs(transactions)
	if err != nil {
		return nil, fmt.Errorf("could not fetch accounts for filtering: %w", err)
	}
	var nonNonceTracked []types.Transaction
	byAccount := make(map[string][]types.Transaction)
	for _, tx := range transactions {
		if isUserNonceTracked(tx.Type) {
			byAccount[tx.From] = append(byAccount[tx.From], tx)
		} else {
			nonNonceTracked = append(nonNonceTracked, tx)
		}
	}
	validTxs := make([]types.Transaction, 0, len(transactions))
	for accountAddr, txs := range byAccount {
		acc, ok := involvedAccounts[accountAddr]
		if !ok {
			continue
		}
		expectedNonce := acc.Nonce + 1
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].Nonce < txs[j].Nonce
		})
		for _, tx := range txs {
			if tx.Nonce < expectedNonce {
				continue
			}
			for tx.Nonce > expectedNonce {
				ctx, cancel := context.WithTimeout(c.ctx, 500*time.Millisecond)
				foundTx, err := c.node.RequestMissingTx(ctx, accountAddr, expectedNonce)
				cancel()
				if err != nil || foundTx == nil {
					goto nextAccount
				}
				validTxs = append(validTxs, *foundTx)
				expectedNonce++
			}
			if tx.Nonce == expectedNonce {
				validTxs = append(validTxs, tx)
				expectedNonce++
			}
		}
	nextAccount:
	}
	validTxs = append(validTxs, nonNonceTracked...)
	sort.Slice(validTxs, func(i, j int) bool {
		if validTxs[i].From != validTxs[j].From {
			return validTxs[i].From < validTxs[j].From
		}
		return validTxs[i].Nonce < validTxs[j].Nonce
	})
	return validTxs, nil
}

func (c *Client) getAccountsForTxs(transactions []types.Transaction) (map[string]*types.AccountState, error) {
	accountAddrs := make(map[string]struct{})
	for _, tx := range transactions {
		if tx.From != "" && tx.From != "network" && tx.From != "system-batch" {
			accountAddrs[tx.From] = struct{}{}
		}
		if tx.To != "" {
			accountAddrs[tx.To] = struct{}{}
		}
		if tx.BatchPayload != nil {
			for addr := range tx.BatchPayload.Debits {
				accountAddrs[addr] = struct{}{}
			}
			for addr := range tx.BatchPayload.Rewards {
				accountAddrs[addr] = struct{}{}
			}
		}
		if tx.Spender != "" {
			accountAddrs[tx.Spender] = struct{}{}
		}
	}

	accounts := make(map[string]*types.AccountState)
	addresses := make([]string, 0, len(accountAddrs))
	for addr := range accountAddrs {
		addresses = append(addresses, addr)
	}

	batchAccounts, err := c.GetLiveAccountStatesBatch(addresses)
	if err != nil {
		return nil, err
	}

	for addr := range accountAddrs {
		acc, ok := batchAccounts[addr]
		if !ok {
			acc = &types.AccountState{Address: addr, StakeInfo: &types.StakeInfo{}, Allowances: make(map[string]uint64)}
		}
		accounts[addr] = acc
	}
	return accounts, nil
}

func (c *Client) broadcastBlock(block types.Block) {
	pbBlock := modeltoproto.ModelToProtoBlock(&block)
	blockBytes, err := proto.Marshal(pbBlock)
	if err != nil {
		log.Printf("ERROR: Failed to marshal block %d for broadcast: %v", block.Height, err)
		return
	}
	if c.node == nil {
		return
	}
	peers := c.node.GetPeerList()
	if len(peers) == 0 {
		return
	}
	shreds, err := c.createBlockShreds(block.Hash, blockBytes)
	if err != nil {
		log.Printf("ERROR: Failed to create shreds for block %d: %v", block.Height, err)
		return
	}
	for _, shred := range shreds {
		shredBytes, err := proto.Marshal(&shred)
		if err != nil {
			continue
		}
		c.node.Gossip(p2p.BlockShredTopic, shredBytes)
	}
}

func (c *Client) createBlockShreds(blockHash string, blockData []byte) ([]pb.BlockShred, error) {
	const shredSize = 4096 * 20
	totalShreds := (len(blockData) + shredSize - 1) / shredSize
	shreds := make([]pb.BlockShred, totalShreds)
	for i := 0; i < totalShreds; i++ {
		start := i * shredSize
		end := start + shredSize
		if end > len(blockData) {
			end = len(blockData)
		}
		shreds[i] = pb.BlockShred{
			BlockHash:   blockHash,
			TotalShreds: int32(totalShreds),
			ShredIndex:  int32(i),
			Data:        blockData[start:end],
		}
	}
	return shreds, nil
}

func (c *Client) handleBlockShredGossip(data []byte) {
	var shred pb.BlockShred
	if err := proto.Unmarshal(data, &shred); err != nil {
		return
	}

	modelShred := types.BlockShred{
		BlockHash:   shred.BlockHash,
		TotalShreds: int(shred.TotalShreds),
		ShredIndex:  int(shred.ShredIndex),
		Data:        shred.Data,
	}

	c.shredStoreLock.Lock()
	defer c.shredStoreLock.Unlock()

	shreds, exists := c.shredStore[modelShred.BlockHash]
	if !exists {
		shreds = make([]*types.BlockShred, modelShred.TotalShreds)
	}
	if shreds[modelShred.ShredIndex] == nil {
		shreds[modelShred.ShredIndex] = &modelShred
	}
	c.shredStore[modelShred.BlockHash] = shreds
	receivedCount := 0
	for _, s := range shreds {
		if s != nil {
			receivedCount++
		}
	}
	if receivedCount == modelShred.TotalShreds {
		var blockData []byte
		for _, s := range shreds {
			blockData = append(blockData, s.Data...)
		}
		delete(c.shredStore, modelShred.BlockHash)
		go c.handleBlockProposal(blockData)
	}
}

func (c *Client) handleBlockProposal(data []byte) {
	if c.isCatchingUp.Load() {
		return
	}
	var pbBlock pb.Block
	if err := proto.Unmarshal(data, &pbBlock); err != nil {
		return
	}
	block := *prototomodel.ProtoToModelBlock(&pbBlock)

	c.metaLock.RLock()
	currentHeight := c.lastBlockHeight
	currentBlock := c.lastBlock
	c.metaLock.RUnlock()

	if block.Height <= currentHeight {
		return
	}
	if block.Height > currentHeight+1 {
		proposerPID, _ := peer.Decode(block.ProposerID)
		go c.initiateBlockSync(proposerPID, currentHeight+1, block.Height)
		return
	}
	if block.PrevHash != currentBlock.Hash {
		proposerPID, _ := peer.Decode(block.ProposerID)
		go c.initiateBlockSync(proposerPID, currentHeight+1, block.Height)
		return
	}
	if c.calculateBlockHash(block) != block.Hash {
		return
	}

	expectedLeader, err := c.calculateLeader(currentBlock, block.Height)
	if err != nil {
		log.Printf("Block validation failed for height %d: could not calculate leader: %v", block.Height, err)
		return
	}

	proposerID, err := peer.Decode(block.ProposerID)
	if err != nil {
		return
	}
	if proposerID != expectedLeader {
		log.Printf("Block validation failed for height %d: proposer %s is not the expected leader %s", block.Height, proposerID, expectedLeader)
		return
	}
	c.metaLock.Lock()
	if block.Height == c.lastBlockHeight+1 {
		c.lastBlock = &block
		c.lastBlockHash = block.Hash
		c.lastBlockHeight = block.Height
		c.blockProcessingQueue <- block
	}
	c.metaLock.Unlock()
}

func (c *Client) HandleTransactionGossip(data []byte) {
	// Try to unmarshal as TransactionList first
	var txList pb.TransactionList
	var txs []*pb.Transaction

	if err := proto.Unmarshal(data, &txList); err == nil {
		txs = txList.Transactions
	} else {
		// Fallback: try single transaction
		var singleTx pb.Transaction
		if err := proto.Unmarshal(data, &singleTx); err == nil {
			txs = append(txs, &singleTx)
		} else {
			return
		}
	}

	for _, pbtx := range txs {
		tx := prototomodel.ProtoToModelTransaction(pbtx)

		// Skip expired transactions
		if time.Since(tx.CreatedAt) > TransactionExpiration {
			continue
		}

		// Skip already processed transactions
		if isProcessed, _ := c.isTransactionProcessed(tx.ID); isProcessed {
			continue
		}

		// --- NEW: Mark transaction as propagated to all online validators ---
		// Since this arrived via gossip, assume all connected validators got it
		if c.node != nil {
			validators := c.getOnlineValidators()
			if len(validators) > 0 {
				peerMap := make(map[string]time.Time)
				for _, v := range validators {
					peerMap[v.PeerID.String()] = time.Now()
				}
				c.txPropagationState.Store(tx.ID, peerMap)
			}
		}
		// --- END NEW ---

		// Add to mempool
		c.addTxToMempool(*tx, false)
	}
}

// recordTxPropagationFromPeer marks that a specific peer has a transaction
func (c *Client) recordTxPropagationFromPeer(txID string, peerID peer.ID) {
	peerIDStr := peerID.String()

	// Load existing propagation map or create new
	var peerMap map[string]time.Time
	if existing, ok := c.txPropagationState.Load(txID); ok {
		peerMap = existing.(map[string]time.Time)
	} else {
		peerMap = make(map[string]time.Time)
	}

	// Record this peer has the transaction
	peerMap[peerIDStr] = time.Now()
	c.txPropagationState.Store(txID, peerMap)
}

func (c *Client) addTxToMempool(tx types.Transaction, prevalidated bool) {
	shard := c.getMempoolShard(tx.ID)
	if _, exists := shard.transactions.Load(tx.ID); exists {
		return
	}
	currentSize := shard.size.Load()
	if currentSize >= shard.maxSize {
		return
	}
	entry := &MempoolEntry{
		Transaction:    tx,
		Validations:    make(map[peer.ID]struct{}),
		IsLocallyValid: prevalidated,
	}
	if _, loaded := shard.transactions.LoadOrStore(tx.ID, entry); !loaded {
		shard.size.Add(1)
		if c.node != nil {
			c.node.BroadcastMempoolAck(tx.ID)
		}
		if isUserNonceTracked(tx.Type) {
			c.adjustPendingSpend(tx.From, int64(tx.Amount))
		}

		if !prevalidated {
			select {
			case c.preValidationQueue <- tx:
			default:
				log.Printf("Pre-validation queue is full, dropping transaction %s", tx.ID)
			}
		} else {
			c.gossipValidationVote(tx.ID)
		}

		var totalTxsInMempool int32
		for i := range c.mempoolShards {
			totalTxsInMempool += c.mempoolShards[i].size.Load()
		}
		if totalTxsInMempool >= MinTxsPerBlock {
			select {
			case c.mempoolReadySignal <- struct{}{}:
			default:
			}
		}
	}
}

func (c *Client) ProcessAggregatorBatchTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	if isProcessed, _ := c.isTransactionProcessed(tx.ID); isProcessed {
		return nil, fmt.Errorf("aggregator transaction %s already processed", tx.ID)
	}

	shard := c.getMempoolShard(tx.ID)
	if _, exists := shard.transactions.Load(tx.ID); exists {
		return &tx, nil
	}
	entry := &MempoolEntry{
		Transaction:    tx,
		IsLocallyValid: true,
		Validations:    make(map[peer.ID]struct{}),
	}
	if _, loaded := shard.transactions.LoadOrStore(tx.ID, entry); !loaded {
		shard.size.Add(1)

		c.broadcastTransaction(tx)

		var totalTxsInMempool int32
		for i := range c.mempoolShards {
			totalTxsInMempool += c.mempoolShards[i].size.Load()
		}
		if totalTxsInMempool >= MinTxsPerBlock {
			select {
			case c.mempoolReadySignal <- struct{}{}:
			default:
			}
		}
	}

	return &tx, nil
}

// --- MODIFIED: GetFullSyncState to include token data ---
// GetFullSyncState creates a consistent snapshot of the current ledger state for a new peer.
// GetFullSyncState creates a consistent snapshot of the current ledger state for a new peer.
func (c *Client) GetFullSyncState() ([]byte, error) {
	c.metaLock.RLock()
	lastBlock := c.lastBlock
	lastHash := c.lastBlockHash
	lastHeight := c.lastBlockHeight
	c.metaLock.RUnlock()

	accountsMap := make(map[string]types.AccountState)

	// Collect all accounts (including their GCU allowances)
	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(accountPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var acc *types.AccountState
			if err := it.Item().Value(func(v []byte) error {
				var unmarshalErr error
				acc, unmarshalErr = utils.UnmarshalAccountState(v)
				return unmarshalErr
			}); err != nil {
				continue
			}
			accountsMap[acc.Address] = *acc
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// ============ COLLECT TOKEN STATE ============
	tokensMap := make(map[string]*pb.TokenState)
	var tokenBalances []*pb.TokenBalance
	var tokenAllowances []*pb.TokenAllowance

	err = c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		// 1. Collect all tokens
		tokenPrefixBytes := []byte(types.TokenPrefix)
		for it.Seek(tokenPrefixBytes); it.ValidForPrefix(tokenPrefixBytes); it.Next() {
			var token types.Token
			if err := it.Item().Value(func(v []byte) error {
				return jsoniter.Unmarshal(v, &token)
			}); err != nil {
				continue
			}
			tokensMap[token.Address] = modeltoproto.ModelToProtoTokenState(&token)
		}

		// 2. Collect all token balances
		balPrefixBytes := []byte(types.TokenBalancePrefix)
		for it.Seek(balPrefixBytes); it.ValidForPrefix(balPrefixBytes); it.Next() {
			key := string(it.Item().Key())
			parts := strings.Split(strings.TrimPrefix(key, types.TokenBalancePrefix), "-")
			if len(parts) != 2 {
				continue
			}
			tokenAddr, owner := parts[0], parts[1]

			var balance uint64
			it.Item().Value(func(v []byte) error {
				if len(v) == 8 {
					balance = binary.BigEndian.Uint64(v)
				}
				return nil
			})

			tokenBalances = append(tokenBalances, &pb.TokenBalance{
				TokenAddress: tokenAddr,
				Owner:        owner,
				Balance:      balance,
			})
		}

		// 3. Collect all token allowances
		allowPrefixBytes := []byte(types.TokenAllowancePrefix)
		for it.Seek(allowPrefixBytes); it.ValidForPrefix(allowPrefixBytes); it.Next() {
			key := string(it.Item().Key())
			parts := strings.Split(strings.TrimPrefix(key, types.TokenAllowancePrefix), "-")
			if len(parts) != 3 {
				continue
			}
			tokenAddr, owner, spender := parts[0], parts[1], parts[2]

			var amount uint64
			it.Item().Value(func(v []byte) error {
				if len(v) == 8 {
					amount = binary.BigEndian.Uint64(v)
				}
				return nil
			})

			tokenAllowances = append(tokenAllowances, &pb.TokenAllowance{
				TokenAddress: tokenAddr,
				Owner:        owner,
				Spender:      spender,
				Amount:       amount,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	// ============ END TOKEN COLLECTION ============

	// Convert accounts to protobuf format (includes GCU allowances)
	pbAccounts := make(map[string]*pb.AccountState)
	for addr, acc := range accountsMap {
		pbAcc := &pb.AccountState{
			Address:      acc.Address,
			Balance:      acc.Balance,
			Nonce:        acc.Nonce,
			PendingNonce: acc.PendingNonce,
			PendingSpend: acc.PendingSpend,
		}

		if acc.StakeInfo != nil {
			pbAcc.StakeInfo = &pb.StakeInfo{
				TotalStake: acc.StakeInfo.TotalStake,
			}
			for _, alloc := range acc.StakeInfo.Allocations {
				pbAcc.StakeInfo.Allocations = append(pbAcc.StakeInfo.Allocations, &pb.StakeAllocation{
					NodePeerId: alloc.NodePeerID,
					Amount:     alloc.Amount,
					LockedAt:   timestamppb.New(alloc.LockedAt),
				})
			}
			for _, unbond := range acc.StakeInfo.Unbonding {
				pbAcc.StakeInfo.Unbonding = append(pbAcc.StakeInfo.Unbonding, &pb.UnbondingStake{
					Amount:     unbond.Amount,
					UnlockTime: timestamppb.New(unbond.UnlockTime),
				})
			}
		}

		// *** CRITICAL: Include GCU allowances in sync ***
		if len(acc.Allowances) > 0 {
			pbAcc.Allowances = acc.Allowances
		}

		pbAccounts[addr] = pbAcc
	}

	pbValidators := make([]*pb.ValidatorInfo, len(c.validatorSet))
	for i, v := range c.validatorSet {
		pbValidators[i] = &pb.ValidatorInfo{
			PeerId:     v.PeerID.String(),
			TotalStake: v.TotalStake,
		}
	}

	fullSync := &pb.FullSyncState{
		Accounts:        pbAccounts,
		ValidatorSet:    pbValidators,
		LastBlockHash:   lastHash,
		LastBlockHeight: lastHeight,
		LastBlock:       modeltoproto.ModelToProtoBlock(lastBlock),
		Tokens:          tokensMap,
		TokenBalances:   tokenBalances,
		TokenAllowances: tokenAllowances,
	}

	return proto.Marshal(fullSync)
}

// --- MODIFIED: clearLedgerData to remove all token data ---
func (c *Client) clearLedgerData() error {
	prefixes := [][]byte{
		[]byte(accountPrefix), []byte(transactionPrefix), []byte(metaPrefix),
		[]byte(blockPrefix), []byte(processedTxIDPrefix),
		// Add token prefixes
		[]byte(types.TokenPrefix), []byte(types.TokenOwnerPrefix), []byte(types.TokenBalancePrefix),
		[]byte(types.TokenAllowancePrefix), []byte(types.TokenMetadataPrefix), []byte(types.TokenHistoryPrefix),
		// Add index prefixes for tokens
		[]byte("tidx-token-from-"),
		[]byte("tidx-token-to-"),
		// Add stake index prefix
		[]byte("sidx-node-"),
	}
	log.Println("SYNC: Clearing all ledger data...")
	for _, prefix := range prefixes {
		if err := c.db.DropPrefix(prefix); err != nil {
			return fmt.Errorf("failed to drop prefix %s: %w", string(prefix), err)
		}
	}
	return nil
}

// ApplyFullSyncState applies a complete ledger snapshot from a peer, ensuring consistency.
func (c *Client) ApplyFullSyncState(data []byte) error {
	var fullSync pb.FullSyncState
	if err := proto.Unmarshal(data, &fullSync); err != nil {
		return fmt.Errorf("failed to unmarshal full sync state: %w", err)
	}

	log.Printf("SYNC: Applying full state sync (Accounts: %d, Validators: %d, Tokens: %d, Balances: %d, Token Allowances: %d)",
		len(fullSync.Accounts),
		len(fullSync.ValidatorSet),
		len(fullSync.Tokens),
		len(fullSync.TokenBalances),
		len(fullSync.TokenAllowances))

	return c.db.Update(func(txn *badger.Txn) error {
		// ============ STEP 1: Apply GCU Account States ============
		for _, pbAcc := range fullSync.Accounts {
			acc := &types.AccountState{
				Address:      pbAcc.Address,
				Balance:      pbAcc.Balance,
				Nonce:        pbAcc.Nonce,
				PendingNonce: pbAcc.PendingNonce,
				PendingSpend: pbAcc.PendingSpend,
			}

			if pbAcc.StakeInfo != nil {
				acc.StakeInfo = &types.StakeInfo{
					TotalStake: pbAcc.StakeInfo.TotalStake,
				}
				for _, pbAlloc := range pbAcc.StakeInfo.Allocations {
					acc.StakeInfo.Allocations = append(acc.StakeInfo.Allocations, types.StakeAllocation{
						NodePeerID: pbAlloc.NodePeerId,
						Amount:     pbAlloc.Amount,
						LockedAt:   pbAlloc.LockedAt.AsTime(),
					})
				}
				for _, pbUnbond := range pbAcc.StakeInfo.Unbonding {
					acc.StakeInfo.Unbonding = append(acc.StakeInfo.Unbonding, types.UnbondingStake{
						Amount:     pbUnbond.Amount,
						UnlockTime: pbUnbond.UnlockTime.AsTime(),
					})
				}
			}

			// *** CRITICAL: Restore GCU allowances ***
			if pbAcc.Allowances != nil {
				acc.Allowances = pbAcc.Allowances
			} else {
				acc.Allowances = make(map[string]uint64)
			}

			accBytes, _ := utils.MarshalAccountState(acc)
			if err := txn.Set([]byte(accountPrefix+acc.Address), accBytes); err != nil {
				return err
			}
		}
		log.Printf("SYNC: ✅ Restored %d GCU account states", len(fullSync.Accounts))

		// ============ STEP 2: Apply Token Definitions ============
		for tokenAddr, pbToken := range fullSync.Tokens {
			token := prototomodel.ProtoToModelTokenState(pbToken)
			if token == nil {
				continue
			}
			tokenBytes, err := jsoniter.Marshal(token)
			if err != nil {
				return fmt.Errorf("failed to marshal synced token %s: %w", tokenAddr, err)
			}
			if err := txn.Set([]byte(types.TokenPrefix+tokenAddr), tokenBytes); err != nil {
				return fmt.Errorf("failed to store synced token %s: %w", tokenAddr, err)
			}
		}
		log.Printf("SYNC: ✅ Restored %d token definitions", len(fullSync.Tokens))

		// ============ STEP 3: Apply Token Balances (with ownership index) ============
		for _, pbBal := range fullSync.TokenBalances {
			// Store token balance with proper prefix
			balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, pbBal.TokenAddress, pbBal.Owner))
			balanceBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(balanceBytes, pbBal.Balance)
			if err := txn.Set(balanceKey, balanceBytes); err != nil {
				return fmt.Errorf("failed to store token balance for %s: %w", pbBal.Owner, err)
			}

			// Store token ownership index (for GetOwnedTokens queries)
			ownerKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, pbBal.Owner, pbBal.TokenAddress))
			if err := txn.Set(ownerKey, []byte{1}); err != nil {
				return fmt.Errorf("failed to store token ownership index: %w", err)
			}
		}
		log.Printf("SYNC: ✅ Restored %d token balances with ownership indexes", len(fullSync.TokenBalances))

		// ============ STEP 4: Apply Token Allowances ============
		for _, pbAllow := range fullSync.TokenAllowances {
			allowKey := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix,
				pbAllow.TokenAddress, pbAllow.Owner, pbAllow.Spender))
			allowBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(allowBytes, pbAllow.Amount)
			if err := txn.Set(allowKey, allowBytes); err != nil {
				return fmt.Errorf("failed to store token allowance: %w", err)
			}
		}
		log.Printf("SYNC: ✅ Restored %d token allowances", len(fullSync.TokenAllowances))

		// ============ STEP 5: Rebuild Stake Indexes (GCU staking system) ============
		log.Printf("SYNC: Rebuilding stake indexes...")
		stakeIndexCount := 0
		for addr, pbAcc := range fullSync.Accounts {
			if pbAcc.StakeInfo != nil && len(pbAcc.StakeInfo.Allocations) > 0 {
				for _, pbAlloc := range pbAcc.StakeInfo.Allocations {
					stakeIndexKey := []byte(fmt.Sprintf("sidx-node-%s-%s", pbAlloc.NodePeerId, addr))
					amountBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(amountBytes, pbAlloc.Amount)
					if err := txn.Set(stakeIndexKey, amountBytes); err != nil {
						return fmt.Errorf("failed to rebuild stake index: %w", err)
					}
					stakeIndexCount++
				}
			}
		}
		log.Printf("SYNC: ✅ Rebuilt %d stake allocation indexes", stakeIndexCount)

		// ============ STEP 6: Mark all synced state as processed ============
		log.Printf("SYNC: Marking synced state as processed...")
		for addr := range fullSync.Accounts {
			processedKey := []byte(fmt.Sprintf("proc-sync-%s", addr))
			if err := txn.Set(processedKey, []byte{1}); err != nil {
				return fmt.Errorf("failed to mark processed: %w", err)
			}
		}
		for tokenAddr := range fullSync.Tokens {
			processedKey := []byte(fmt.Sprintf("proc-sync-token-%s", tokenAddr))
			if err := txn.Set(processedKey, []byte{1}); err != nil {
				return fmt.Errorf("failed to mark token processed: %w", err)
			}
		}
		log.Printf("SYNC: ✅ Marked all synced state as processed")

		// ============ STEP 7: Apply Validator Set ============
		modelValidators := make([]types.ValidatorInfo, len(fullSync.ValidatorSet))
		for i, pbVal := range fullSync.ValidatorSet {
			peerID, _ := peer.Decode(pbVal.PeerId)
			modelValidators[i] = types.ValidatorInfo{
				PeerID:     peerID,
				TotalStake: pbVal.TotalStake,
			}
		}

		c.validatorSetMutex.Lock()
		c.validatorSet = modelValidators
		c.validatorSetMutex.Unlock()
		log.Printf("SYNC: ✅ Restored validator set (%d validators)", len(modelValidators))

		// ============ STEP 8: Apply Last Block ============
		if fullSync.LastBlock != nil {
			lastBlock := prototomodel.ProtoToModelBlock(fullSync.LastBlock)
			if len(lastBlock.NextValidators) == 0 && len(c.validatorSet) > 0 {
				lastBlock.NextValidators = make([]types.ValidatorInfo, len(c.validatorSet))
				copy(lastBlock.NextValidators, c.validatorSet)
			}
			c.metaLock.Lock()
			c.lastBlock = lastBlock
			c.lastBlockHash = fullSync.LastBlockHash
			c.lastBlockHeight = fullSync.LastBlockHeight
			c.metaLock.Unlock()

			pbBlockBytes, _ := proto.Marshal(fullSync.LastBlock)
			if err := txn.Set([]byte(metaPrefix+lastBlockKey), pbBlockBytes); err != nil {
				return fmt.Errorf("failed to store synced last block: %w", err)
			}
			log.Printf("SYNC: ✅ Restored last block (height: %d, hash: %s)", lastBlock.Height, lastBlock.Hash[:10])
		}

		log.Printf("SYNC: ✅✅✅ Full state sync completed successfully!")

		if c.node != nil && c.node.Role != "worker" {
			currentHeight := fullSync.LastBlockHeight + 1
			leader, err := c.calculateLeader(c.lastBlock, currentHeight)
			if err == nil && leader != c.node.Host.ID() {
				ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
				defer cancel()
				log.Printf("SYNC: Sending ready signal for height %d after sync completion", currentHeight)
				c.node.SendNextBlockReady(ctx, leader, currentHeight)
			}
		}
		return nil
	})
}

// GetTotalStakeForNode calculates the total stake allocated to a specific node peer ID
// using an efficient index, avoiding a full scan of all accounts.
func (c *Client) GetTotalStakeForNode(nodePeerID string) (uint64, error) {
	var totalAllocatedStake uint64

	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(fmt.Sprintf("sidx-node-%s-", nodePeerID))

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				if len(val) == 8 {
					totalAllocatedStake += binary.BigEndian.Uint64(val)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return totalAllocatedStake, err
}

func (c *Client) getAccountDirectFromDB(address string) (*types.AccountState, error) {
	var account *types.AccountState
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(accountPrefix + address))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var unmarshalErr error
			account, unmarshalErr = utils.UnmarshalAccountState(val)
			return unmarshalErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return &types.AccountState{
			Address:    address,
			StakeInfo:  &types.StakeInfo{},
			Allowances: make(map[string]uint64),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error reading account %s from db: %w", address, err)
	}
	return account, nil
}

func (c *Client) GetStakeInfo(ownerAddress string) (*types.StakeInfo, error) {
	acc, err := c.getAccountDirectFromDB(ownerAddress)
	if err != nil {
		return nil, err
	}
	if acc.StakeInfo == nil {
		return &types.StakeInfo{}, nil
	}
	var remainingUnbonding []types.UnbondingStake
	now := time.Now()
	for _, unbond := range acc.StakeInfo.Unbonding {
		if !now.After(unbond.UnlockTime) {
			remainingUnbonding = append(remainingUnbonding, unbond)
		}
	}
	acc.StakeInfo.Unbonding = remainingUnbonding
	return acc.StakeInfo, nil
}

// isUserNonceTracked defines which transaction types require a nonce from the user's account.
func isUserNonceTracked(txType types.TransactionType) bool {
	switch txType {
	case model.TransferTransaction, model.StakeDepositTransaction,
		model.StakeWithdrawalTransaction, model.StakeAssignTransaction,
		model.StakeUnassignTransaction, model.SetAllowanceTransaction,
		model.RemoveAllowanceTransaction, model.DelegatedTransferTransaction,
		// Add token transaction types that need nonces
		model.TokenCreateTransaction, model.TokenTransferTransaction,
		model.TokenMintTransaction, model.TokenBurnTransaction,
		model.TokenApproveTransaction, model.TokenRevokeTransaction,
		model.TokenLockTransaction, model.TokenConfigBurnTransaction,
		model.TokenConfigMintTransaction:
		return true
	case model.RollbackDebitTransaction, model.RollbackCreditTransaction:
		return false
	default:
		return false
	}
}

func (c *Client) processUnbondingUnsafe(account *types.AccountState) {
	if account.StakeInfo == nil || len(account.StakeInfo.Unbonding) == 0 {
		return
	}
	var remainingUnbonding []types.UnbondingStake
	now := time.Now()
	for _, unbond := range account.StakeInfo.Unbonding {
		if !now.After(unbond.UnlockTime) {
			remainingUnbonding = append(remainingUnbonding, unbond)
		}
	}
	account.StakeInfo.Unbonding = remainingUnbonding
}

// SubmitDelegatedTransaction is called by the workflow engine to spend on behalf of a user.
func (c *Client) SubmitDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	// This transaction type does not consume a user nonce, so it can be processed directly.
	return c.ProcessTransactionSubmission(ctx, tx)
}

func (c *Client) StakeDeposit(tx types.Transaction) (*types.Transaction, error) {
	return c.ProcessTransactionSubmission(c.ctx, tx)
}

func (c *Client) StakeWithdrawal(tx types.Transaction) (*types.Transaction, error) {
	return c.ProcessTransactionSubmission(c.ctx, tx)
}

func (c *Client) AssignStake(tx types.Transaction) (*types.Transaction, error) {
	return c.ProcessTransactionSubmission(c.ctx, tx)
}

func (c *Client) UnassignStake(tx types.Transaction) (*types.Transaction, error) {
	return c.ProcessTransactionSubmission(c.ctx, tx)
}

func (c *Client) ProcessTransactionSubmission(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	if isProcessed, _ := c.isTransactionProcessed(tx.ID); isProcessed {
		return nil, fmt.Errorf("transaction %s already processed", tx.ID)
	}
	// Perform synchronous pre-validation.
	if err := c.preValidateTransaction(tx); err != nil {
		c.storeFailedTransaction(tx, err.Error())
		return nil, err // Return the validation error immediately
	}

	// If validation passes, add to mempool (as already validated) and broadcast.
	c.addTxToMempool(tx, true)
	c.broadcastTransaction(tx)
	return &tx, nil
}

func (c *Client) BroadcastDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	if isProcessed, _ := c.isTransactionProcessed(tx.ID); isProcessed {
		return nil, fmt.Errorf("transaction %s already processed", tx.ID)
	}

	// Add to mempool as pre-validated (trusting the consensus nodes to do the real validation)
	// and broadcast to the network.
	c.addTxToMempool(tx, true)
	c.broadcastTransaction(tx)

	log.Printf("WORKER_BROADCAST: Delegated transaction %s for %s submitted directly to mempool for network validation.", tx.ID, tx.From)

	return &tx, nil
}

func (c *Client) ProactiveReconcile() {}

func (c *Client) handleHistoryRequestStream(stream network.Stream) { stream.Reset() }

func (c *Client) listenForNetworkUpdates(txChan, reqChan, blockProposalChan, blockShredChan, preValidatedTxChan, nonceUpdateChan, preProposalChan, validatorUpdateChan <-chan *pubsub.Message) {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-txChan:
				c.HandleTransactionGossip(msg.Data)
			case msg := <-blockShredChan:
				if c.node.Role != "worker" {
					c.handleBlockShredGossip(msg.Data)
				}
			case msg := <-preValidatedTxChan:
				if c.node.Role != "worker" {
					c.handlePreValidationVoteGossip(msg.Data)
				}
			case msg := <-nonceUpdateChan:
				// Nonce updates can be relevant for all nodes to keep pending states accurate.
				c.handleNonceUpdateGossip(msg.Data)
			case msg := <-preProposalChan:
				if c.node.Role != "worker" {
					go c.HandlePreBlockProposal(msg.Data)
				}
			case msg := <-validatorUpdateChan:
				if c.node.Role != "worker" {
					c.HandleValidatorUpdate(msg.Data)
				}
			}
		}
	}()
}

func (c *Client) handleLedgerSyncStream(stream network.Stream) { stream.Reset() }

// broadcastTransaction now tracks which validators received the transaction
func (c *Client) broadcastTransaction(tx types.Transaction) {
	if c.node == nil {
		return
	}

	pbTx := modeltoproto.ModelToProtoTransaction(&tx)
	txBytes, err := proto.Marshal(pbTx)
	if err != nil {
		return
	}

	// Gossip to network
	c.node.Gossip(p2p.LedgerTransactionTopic, txBytes)

	// --- NEW: Mark transaction as propagated to all connected validators ---
	validators := c.getOnlineValidators()
	if len(validators) > 0 {
		peerMap := make(map[string]time.Time)
		for _, v := range validators {
			peerMap[v.PeerID.String()] = time.Now()
		}
		c.txPropagationState.Store(tx.ID, peerMap)
		log.Printf("TX-PROPAGATION: Marked tx %s as propagated to %d validators", tx.ID[:8], len(peerMap))
	}
	// --- END NEW ---
}

func (c *Client) loadMetadata() {
	c.metaLock.Lock()
	defer c.metaLock.Unlock()

	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(metaPrefix + lastBlockKey))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var pbBlock pb.Block
			if err := proto.Unmarshal(val, &pbBlock); err != nil {
				return err
			}
			c.lastBlock = prototomodel.ProtoToModelBlock(&pbBlock)
			c.lastBlockHash = c.lastBlock.Hash
			c.lastBlockHeight = c.lastBlock.Height
			return nil
		})
	})

	if err != nil {
		log.Printf("Could not load last block from DB (%v), initializing with genesis state.", err)
		c.lastBlock = &types.Block{
			Hash:           "0000000000000000000000000000000000000000000000000000000000000000",
			Height:         0,
			NextValidators: []types.ValidatorInfo{},
		}
		c.lastBlockHash = c.lastBlock.Hash
		c.lastBlockHeight = 0
	}

	// **SOLUTION**: After loading the last block, explicitly load its validator set
	// into the active in-memory validator set.
	if c.lastBlock != nil && len(c.lastBlock.NextValidators) > 0 {
		c.validatorSetMutex.Lock()
		c.validatorSet = c.lastBlock.NextValidators
		c.validatorSetMutex.Unlock()
		log.Printf("Loaded validator set from last block %d.", c.lastBlock.Height)
	} else {
		// Fallback to recalculating from stake if the block is missing the field
		c.recalculateValidatorSet()
	}
}

func (c *Client) CreateNewWallet() (*crypto.Identity, error) {
	identity, err := crypto.NewIdentity()
	if err != nil {
		return nil, err
	}
	err = c.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get([]byte(accountPrefix + identity.Address)); err == nil {
			return fmt.Errorf("generated a wallet that already exists, please try again")
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		newAccount := &types.AccountState{
			Address:    identity.Address,
			Balance:    0,
			Nonce:      0,
			StakeInfo:  &types.StakeInfo{},
			Allowances: make(map[string]uint64),
		}
		accBytes, err := utils.MarshalAccountState(newAccount)
		if err != nil {
			return err
		}
		return txn.Set([]byte(accountPrefix+identity.Address), accBytes)
	})
	return identity, err
}

func (c *Client) CheckBalance(address string) (float64, error) {
	account, err := c.getAccountDirectFromDB(address)
	if err != nil {
		return 0, err
	}
	if account == nil {
		return 0, nil
	}
	return float64(account.Balance) / model.GCU_MICRO_UNIT, nil
}

func (c *Client) GetNonce(address string) (uint64, error) {
	account, err := c.getAccountDirectFromDB(address)
	if err != nil {
		return 0, err
	}
	if account == nil {
		return 0, nil
	}
	return account.Nonce, nil
}

// GetTransactionHistory retrieves a paginated, unified history for a specific address,
// including both native GCU and all token transactions.
func (c *Client) GetTransactionHistory(address string, page, limit int, startTime, endTime time.Time) (TransactionHistoryPage, error) {
	if limit <= 0 {
		limit = 10
	}
	if page <= 0 {
		page = 1
	}

	start := time.Now()
	allHistoryEntries := make([]historyEntry, 0, limit*2)

	err := c.db.View(func(txn *badger.Txn) error {
		// Scan standard GCU 'from' index
		fromPrefix := []byte(fmt.Sprintf("tidx-from-%s-", address))
		if err := c.scanHistoryIndex(txn, fromPrefix, &allHistoryEntries); err != nil {
			return fmt.Errorf("failed to scan 'from' index: %w", err)
		}

		// Scan standard GCU 'to' index
		toPrefix := []byte(fmt.Sprintf("tidx-to-%s-", address))
		if err := c.scanHistoryIndex(txn, toPrefix, &allHistoryEntries); err != nil {
			return fmt.Errorf("failed to scan 'to' index: %w", err)
		}

		// Scan token 'from' index
		tokenFromPrefix := []byte(fmt.Sprintf("tidx-token-from-%s-", address))
		if err := c.scanHistoryIndex(txn, tokenFromPrefix, &allHistoryEntries); err != nil {
			return fmt.Errorf("failed to scan 'token-from' index: %w", err)
		}

		// Scan token 'to' index
		tokenToPrefix := []byte(fmt.Sprintf("tidx-token-to-%s-", address))
		if err := c.scanHistoryIndex(txn, tokenToPrefix, &allHistoryEntries); err != nil {
			return fmt.Errorf("failed to scan 'token-to' index: %w", err)
		}
		return nil
	})
	if err != nil {
		return TransactionHistoryPage{}, err
	}

	log.Printf("[PERF] History index scan for %s took %v, found %d entries", address, time.Since(start), len(allHistoryEntries))

	// De-duplicate
	seenIDs := make(map[string]struct{})
	uniqueEntries := make([]historyEntry, 0, len(allHistoryEntries))
	for _, entry := range allHistoryEntries {
		if _, seen := seenIDs[entry.TxID]; !seen {
			uniqueEntries = append(uniqueEntries, entry)
			seenIDs[entry.TxID] = struct{}{}
		}
	}

	// Apply time filters
	var filteredEntries []historyEntry
	for _, entry := range uniqueEntries {
		if !startTime.IsZero() && entry.Timestamp.Before(startTime) {
			continue
		}
		if !endTime.IsZero() && entry.Timestamp.After(endTime) {
			continue
		}
		filteredEntries = append(filteredEntries, entry)
	}

	// Sort by timestamp (most recent first)
	sort.Slice(filteredEntries, func(i, j int) bool {
		return filteredEntries[i].Timestamp.After(filteredEntries[j].Timestamp)
	})

	totalRecords := int64(len(filteredEntries))
	if totalRecords == 0 {
		pendingTxs := c.getPendingTransactionsForAddress(address, seenIDs)
		return TransactionHistoryPage{Transactions: pendingTxs, TotalRecords: int64(len(pendingTxs))}, nil
	}

	totalPages := int(math.Ceil(float64(totalRecords) / float64(limit)))
	startIdx := (page - 1) * limit
	if startIdx >= len(filteredEntries) {
		return TransactionHistoryPage{Transactions: []TransactionDetail{}, TotalRecords: totalRecords, TotalPages: totalPages, CurrentPage: page}, nil
	}

	endIdx := startIdx + limit
	if endIdx > len(filteredEntries) {
		endIdx = len(filteredEntries)
	}
	paginatedEntries := filteredEntries[startIdx:endIdx]

	// Batch token lookups for performance
	tokenAddresses := make(map[string]struct{})
	for _, entry := range paginatedEntries {
		if entry.IsTokenTx && entry.TokenHistory != nil {
			tokenAddresses[entry.TokenHistory.TokenAddress] = struct{}{}
		}
	}

	// Fetch all tokens in parallel
	tokenCache := make(map[string]*types.Token)
	if len(tokenAddresses) > 0 {
		tokenFetchStart := time.Now()
		var wg sync.WaitGroup
		var mu sync.Mutex

		for tokenAddr := range tokenAddresses {
			// Check cache first
			if cached, found := c.tokenCache.Get(tokenAddr); found {
				mu.Lock()
				tokenCache[tokenAddr] = cached
				mu.Unlock()
				continue
			}

			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				token, err := c.GetToken(addr)
				if err == nil && token != nil {
					mu.Lock()
					tokenCache[addr] = token
					mu.Unlock()
					c.tokenCache.Set(addr, token)
				}
			}(tokenAddr)
		}
		wg.Wait()
		log.Printf("[PERF] Batch token fetch for %d tokens took %v", len(tokenAddresses), time.Since(tokenFetchStart))
	}

	// Build final details
	finalDetails := make([]TransactionDetail, 0, len(paginatedEntries))
	detailBuildStart := time.Now()

	err = c.db.View(func(txn *badger.Txn) error {
		for _, entry := range paginatedEntries {
			if entry.IsTokenTx && entry.TokenHistory != nil {
				// Build token transaction detail
				token := tokenCache[entry.TokenHistory.TokenAddress]
				var ticker, name string = "N/A", "N/A"
				var decimals uint8
				var amountFormatted float64

				if token != nil {
					ticker = token.Ticker
					name = token.Name
					decimals = token.Decimals
					if decimals > 0 {
						amountFormatted = float64(entry.TokenHistory.Amount) / math.Pow(10, float64(decimals))
					} else {
						amountFormatted = float64(entry.TokenHistory.Amount)
					}
				}

				detail := TransactionDetail{
					Transaction: types.Transaction{
						ID:           entry.TokenHistory.TxHash,
						Type:         mapTokenOpToTxType(entry.TokenHistory.Operation),
						From:         entry.TokenHistory.From,
						To:           entry.TokenHistory.To,
						Amount:       entry.TokenHistory.Amount,
						Timestamp:    entry.TokenHistory.Timestamp,
						TokenAddress: entry.TokenHistory.TokenAddress,
					},
					Status:          "Confirmed",
					TokenTicker:     ticker,
					TokenName:       name,
					TokenDecimals:   decimals,
					TokenOperation:  string(entry.TokenHistory.Operation),
					AmountFormatted: amountFormatted,
				}
				finalDetails = append(finalDetails, detail)
			} else {
				// Standard GCU transaction
				failedKey := []byte(failedTxPrefix + entry.TxID)
				item, err := txn.Get(failedKey)
				if err == nil {
					var detail TransactionDetail
					_ = item.Value(func(val []byte) error {
						return json.Unmarshal(val, &detail)
					})
					finalDetails = append(finalDetails, detail)
					continue
				}

				successKey := []byte(transactionPrefix + entry.TxID)
				item, err = txn.Get(successKey)
				if err == nil {
					var pbtx pb.Transaction
					_ = item.Value(func(val []byte) error {
						return proto.Unmarshal(val, &pbtx)
					})
					tx := prototomodel.ProtoToModelTransaction(&pbtx)
					detail := TransactionDetail{
						Transaction: *tx,
						Status:      "Confirmed",
					}

					// If it's a GCU transaction, format the amount
					if tx.Amount > 0 {
						detail.AmountFormatted = float64(tx.Amount) / model.GCU_MICRO_UNIT
					}

					finalDetails = append(finalDetails, detail)
				}
			}
		}
		return nil
	})

	log.Printf("[PERF] Detail building took %v", time.Since(detailBuildStart))

	// Add pending transactions
	pendingTxs := c.getPendingTransactionsForAddress(address, seenIDs)
	finalDetails = append(finalDetails, pendingTxs...)

	// Re-sort to include pending
	sort.Slice(finalDetails, func(i, j int) bool {
		return finalDetails[i].Timestamp.After(finalDetails[j].Timestamp)
	})

	log.Printf("[PERF] Total GetTransactionHistory for %s took %v", address, time.Since(start))

	return TransactionHistoryPage{
		Transactions: finalDetails,
		TotalRecords: totalRecords + int64(len(pendingTxs)),
		TotalPages:   totalPages,
		CurrentPage:  page,
	}, nil
}

// scanHistoryIndex iterates over a given history index prefix (both GCU and token)
// and collects unified history entries.
func (c *Client) scanHistoryIndex(txn *badger.Txn, prefix []byte, collection *[]historyEntry) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true // We need values for token history
	it := txn.NewIterator(opts)
	defer it.Close()

	prefixStr := string(prefix)
	isTokenIndex := strings.Contains(prefixStr, "-token-")

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()
		keyStr := string(key)

		if isTokenIndex {
			err := item.Value(func(val []byte) error {
				if len(val) == 0 {
					log.Printf("WARN: Empty value for token history index key: %s", keyStr)
					return nil
				}
				var history types.TokenHistory
				if err := json.Unmarshal(val, &history); err == nil {
					*collection = append(*collection, historyEntry{
						Timestamp:    history.Timestamp,
						TxID:         history.TxHash,
						IsTokenTx:    true,
						TokenHistory: &history,
					})
				} else {
					log.Printf("WARN: Failed to unmarshal token history from index key %s: %v", keyStr, err)
				}
				return nil
			})
			if err != nil {
				return err
			}
		} else { // Standard GCU transaction
			suffix := strings.TrimPrefix(keyStr, prefixStr)
			zIndex := strings.Index(suffix, "Z")
			if zIndex == -1 || zIndex+1 >= len(suffix) || suffix[zIndex+1] != '-' {
				log.Printf("WARN: Malformed history index key encountered and skipped: %s", keyStr)
				continue
			}

			timestampStr := suffix[:zIndex+1]
			txID := suffix[zIndex+2:]

			ts, err := time.Parse(time.RFC3339Nano, timestampStr)
			if err != nil {
				log.Printf("WARN: Malformed timestamp in history index key: %s, parsing error: %v", keyStr, err)
				continue
			}
			if txID == "" {
				log.Printf("WARN: Parsed an empty txID from history index key: %s", keyStr)
				continue
			}

			*collection = append(*collection, historyEntry{
				Timestamp: ts,
				TxID:      txID,
				IsTokenTx: false,
			})
		}
	}
	return nil
}

// getPendingTransactionsForAddress checks the mempool for relevant pending transactions.
func (c *Client) getPendingTransactionsForAddress(address string, alreadySeenIDs map[string]struct{}) []TransactionDetail {
	var pending []TransactionDetail
	for i := range c.mempoolShards {
		shard := &c.mempoolShards[i]
		shard.transactions.Range(func(key, value interface{}) bool {
			txID := key.(string)
			if _, seen := alreadySeenIDs[txID]; seen {
				return true
			}
			entry := value.(*MempoolEntry)
			tx := entry.Transaction

			// Check if this transaction involves the address
			if tx.From != address && tx.To != address {
				return true
			}

			detail := TransactionDetail{
				Transaction: tx,
				Status:      "Pending",
			}

			// Enrich with token info if applicable
			if tx.TokenAddress != "" {
				if token, found := c.tokenCache.Get(tx.TokenAddress); found {
					detail.TokenTicker = token.Ticker
					detail.TokenName = token.Name
					detail.TokenDecimals = token.Decimals
					if token.Decimals > 0 {
						detail.AmountFormatted = float64(tx.Amount) / math.Pow(10, float64(token.Decimals))
					} else {
						detail.AmountFormatted = float64(tx.Amount)
					}
				} else if token, err := c.GetToken(tx.TokenAddress); err == nil {
					detail.TokenTicker = token.Ticker
					detail.TokenName = token.Name
					detail.TokenDecimals = token.Decimals
					if token.Decimals > 0 {
						detail.AmountFormatted = float64(tx.Amount) / math.Pow(10, float64(token.Decimals))
					} else {
						detail.AmountFormatted = float64(tx.Amount)
					}
					c.tokenCache.Set(tx.TokenAddress, token)
				}
				detail.TokenOperation = mapTxTypeToTokenOp(tx.Type)
			}

			pending = append(pending, detail)
			return true
		})
	}
	return pending
}

func (c *Client) getAccount(address string) (*types.AccountState, error) {
	shardIndex := fnv32(address) % uint32(len(c.accountCacheShards))
	shard := &c.accountCacheShards[shardIndex]

	shard.RLock()
	if acc, exists := shard.accounts[address]; exists {
		shard.RUnlock()
		return acc, nil
	}
	shard.RUnlock()

	shard.Lock()
	defer shard.Unlock()

	if acc, exists := shard.accounts[address]; exists {
		return acc, nil
	}

	var account *types.AccountState
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(accountPrefix + address))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var unmarshalErr error
			account, unmarshalErr = utils.UnmarshalAccountState(val)
			return unmarshalErr
		})
	})

	if err == badger.ErrKeyNotFound {
		newAccount := &types.AccountState{
			Address:    address,
			StakeInfo:  &types.StakeInfo{},
			Allowances: make(map[string]uint64),
		}
		shard.accounts[address] = newAccount
		return newAccount, nil
	}

	if err != nil {
		return nil, fmt.Errorf("error reading account %s from db: %w", address, err)
	}

	if account.PendingNonce < account.Nonce {
		account.PendingNonce = account.Nonce
	}

	shard.accounts[address] = account
	return account, nil
}

func (c *Client) getAccountFromDB(address string) (*types.AccountState, error) {
	var account *types.AccountState
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(accountPrefix + address))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var unmarshalErr error
			account, unmarshalErr = utils.UnmarshalAccountState(val)
			return unmarshalErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return &types.AccountState{
			Address:    address,
			StakeInfo:  &types.StakeInfo{},
			Allowances: make(map[string]uint64),
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error reading account %s from db: %w", address, err)
	}
	if account.PendingNonce < account.Nonce {
		account.PendingNonce = account.Nonce
	}
	return account, nil
}

func (c *Client) isTransactionProcessed(txID string) (bool, error) {
	err := c.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(processedTxIDPrefix + txID))
		return err
	})
	if err == nil {
		return true, nil
	}
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return false, err
}

func (c *Client) calculateBlockHash(block types.Block) string {
	var txHashes strings.Builder
	for _, tx := range block.Transactions {
		txHashes.WriteString(tx.ID)
	}
	header := fmt.Sprintf("%s%s%d%s", block.ProposerID, block.Timestamp.Format(time.RFC3339Nano), block.Height, block.PrevHash)
	fullData := header + txHashes.String()
	hash := sha256.Sum256([]byte(fullData))
	return fmt.Sprintf("%x", hash)
}

func (c *Client) GetLatestBlockHash() string {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	return c.lastBlockHash
}

func (c *Client) GetLatestBlockHeight() uint64 {
	c.metaLock.RLock()
	defer c.metaLock.RUnlock()
	return c.lastBlockHeight
}

func deepCopyAccountState(original *types.AccountState) *types.AccountState {
	if original == nil {
		return nil
	}
	copy := &types.AccountState{
		Address:      original.Address,
		Balance:      original.Balance,
		Nonce:        original.Nonce,
		PendingNonce: original.PendingNonce,
		PendingSpend: original.PendingSpend,
	}

	if original.StakeInfo != nil {
		copy.StakeInfo = &types.StakeInfo{
			TotalStake: original.StakeInfo.TotalStake,
		}
		if len(original.StakeInfo.Allocations) > 0 {
			copy.StakeInfo.Allocations = make([]types.StakeAllocation, len(original.StakeInfo.Allocations))
			for i, a := range original.StakeInfo.Allocations {
				copy.StakeInfo.Allocations[i] = a
			}
		}
		if len(original.StakeInfo.Unbonding) > 0 {
			copy.StakeInfo.Unbonding = make([]types.UnbondingStake, len(original.StakeInfo.Unbonding))
			for i, u := range original.StakeInfo.Unbonding {
				copy.StakeInfo.Unbonding[i] = u
			}
		}
	}

	if original.Allowances != nil {
		copy.Allowances = make(map[string]uint64, len(original.Allowances))
		for k, v := range original.Allowances {
			copy.Allowances[k] = v
		}
	}

	return copy
}

// Modified processBlock function with token batch support
// Replace your existing processBlock function with this version

func (c *Client) processBlock(block types.Block) error {
	start := time.Now()
	log.Printf("[PERF-LOG] START: Processing block %d (Hash: %s)", block.Height, block.Hash[:10])

	// ============ INITIALIZE TOKEN BATCH ============
	c.tokenBatchMutex.Lock()
	c.currentTokenBatch = NewTokenBatchOperations()
	c.tokenBatchMutex.Unlock()
	// ============ END TOKEN BATCH INIT ============

	involvedAccounts, err := c.lockAccountsForBlock(block)
	if err != nil {
		return fmt.Errorf("failed to lock accounts for block: %w", err)
	}
	defer func() {
		if unlockErr := c.unlockAccounts(involvedAccounts); unlockErr != nil {
			log.Printf("CRITICAL: Failed to unlock accounts during block processing: %v", unlockErr)
		}
	}()

	c.processedTxDuringBlockMutex.Lock()
	c.processedTxDuringBlock = make(map[string]struct{})
	c.pendingFutureNonces = make(map[string][]types.Transaction)
	c.processedTxDuringBlockMutex.Unlock()

	if block.Height > 1 {
		for _, tx := range block.Transactions {
			if isBatchableTransaction(tx) {
				return ErrInvalidBatchInclusion
			}
		}
	}

	c.inFlightTxsMutex.Lock()
	c.recentTxMutex.Lock()
	c.inFlightTxs = make(map[string]types.Transaction)
	for _, tx := range block.Transactions {
		c.inFlightTxs[tx.ID] = tx
		c.recentTransactions[tx.ID] = tx
	}
	c.recentTxMutex.Unlock()
	c.inFlightTxsMutex.Unlock()
	c.scheduleRecentTxCleanup()

	var batchedTxs []types.Transaction
	var consumedTxIDs map[string]struct{}
	var rawTxMap map[string]types.Transaction

	// Poll for the pre-proposal to arrive, making the cache hit reliable.
	for i := 0; i < 5; i++ { // Poll for up to 50ms
		if cached, ok := c.validatedProposalCache.Load(block.ProposalID); ok {
			entry := cached.(ValidatedProposalCacheEntry)
			batchedTxs = entry.BatchedTransactions
			consumedTxIDs = entry.ConsumedTxIDs
			rawTxMap = entry.RawTransactions
			log.Printf("[PERF-LOG] STEP 1: Block %d proposal found in cache. Duration: %v", block.Height, time.Since(start))
			goto foundInCache // Skip the fallback logic
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("[PERF-LOG] STEP 1 (WARN): Block %d proposal not in cache after delay, using fallback. Duration: %v", block.Height, time.Since(start))
	if true { // Keep indentation, was previously under else
		proposerID, err := peer.Decode(block.ProposerID)
		if err != nil {
			return fmt.Errorf("invalid proposer ID in block: %w", err)
		}
		batchedTxs, consumedTxIDs, rawTxMap, err = c.validateAndRecreateBatches(block.Transactions, proposerID)
		if err != nil {
			return err
		}
		log.Printf("[PERF-LOG] STEP 1.1: Fallback validation successful. Duration: %v", time.Since(start))
	}

foundInCache:
	if rawTxMap == nil || len(rawTxMap) == 0 {
		rawTxMap = make(map[string]types.Transaction)
		for _, tx := range block.Transactions {
			if isBatchableTransaction(tx) {
				rawTxMap[tx.ID] = tx
			}
		}
	}

	filteredTxs := make([]types.Transaction, 0, len(block.Transactions))
	for _, tx := range block.Transactions {
		if _, consumed := consumedTxIDs[tx.ID]; !consumed {
			filteredTxs = append(filteredTxs, tx)
		}
	}
	finalTxs := append(filteredTxs, batchedTxs...)
	log.Printf("[PERF-LOG] STEP 2: Reconstructed final transaction list (%d txs). Duration: %v", len(finalTxs), time.Since(start))

	txGroups := make(map[string][]types.Transaction)
	nonUserTxs := []types.Transaction{}
	for _, tx := range finalTxs {
		if isUserNonceTracked(tx.Type) {
			txGroups[tx.From] = append(txGroups[tx.From], tx)
		} else {
			nonUserTxs = append(nonUserTxs, tx)
		}
	}

	numWorkers := runtime.GOMAXPROCS(0) * 16
	if numWorkers == 0 {
		numWorkers = 8
	}
	var wg sync.WaitGroup
	groupChan := make(chan []types.Transaction, len(txGroups))
	resultsChan := make(chan error, len(finalTxs))
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go c.transactionGroupWorker(&wg, groupChan, resultsChan, involvedAccounts)
	}
	for _, group := range txGroups {
		sort.Slice(group, func(i, j int) bool { return group[i].Nonce < group[j].Nonce })
		groupChan <- group
	}
	close(groupChan)
	for _, tx := range nonUserTxs {
		if err := c.applyTransactionState(tx, involvedAccounts); err != nil {
			resultsChan <- err
		}
	}
	wg.Wait()
	close(resultsChan)
	for err := range resultsChan {
		if err != nil && !errors.Is(err, ErrOldNonce) {
			return fmt.Errorf("unrecoverable error during parallel execution: %w", err)
		}
	}
	log.Printf("[PERF-LOG] STEP 3: Parallel transaction execution complete. Duration: %v", time.Since(start))

	if len(c.pendingFutureNonces) > 0 {
		return ErrUnresolvedNonceGap
	}

	for _, tx := range finalTxs {
		shard := c.getMempoolShard(tx.ID)
		shard.transactions.Delete(tx.ID)
	}
	for txID := range consumedTxIDs {
		shard := c.getMempoolShard(txID)
		shard.transactions.Delete(txID)
	}
	// --- NEW: Clean up propagation state for processed transactions ---
	for _, tx := range finalTxs {
		c.txPropagationState.Delete(tx.ID)
	}
	for txID := range consumedTxIDs {
		c.txPropagationState.Delete(txID)
	}
	log.Printf("PERF-LOG: Cleaned up propagation state for %d transactions", len(finalTxs)+len(consumedTxIDs))
	// --- END NEW ---
	log.Printf("[PERF-LOG] STEP 4: Mempool cleanup complete. Duration: %v", time.Since(start))

	for _, tx := range finalTxs {
		if isUserNonceTracked(tx.Type) {
			if acc, ok := involvedAccounts[tx.From]; ok {
				c.adjustPendingSpendUnsafe(acc, -int64(tx.Amount))
			}
		}
	}

	accountsToWrite := make(map[string]*types.AccountState, len(involvedAccounts))
	for addr, acc := range involvedAccounts {
		accountsToWrite[addr] = deepCopyAccountState(acc)
	}

	c.inFlightTxsMutex.Lock()
	c.inFlightTxs = make(map[string]types.Transaction)
	c.inFlightTxsMutex.Unlock()

	wb := c.db.NewWriteBatch()
	defer wb.Cancel()

	// Helper for writing transaction history indexes
	writeTxHistoryIndexes := func(wb *badger.WriteBatch, tx types.Transaction) error {
		timestampStr := tx.Timestamp.Format(time.RFC3339Nano)
		if tx.From != "" && tx.From != "network" && tx.From != "system-batch" {
			fromKey := []byte(fmt.Sprintf("tidx-from-%s-%s-%s", tx.From, timestampStr, tx.ID))
			if err := wb.Set(fromKey, nil); err != nil {
				return err
			}
		}
		if tx.To != "" {
			toKey := []byte(fmt.Sprintf("tidx-to-%s-%s-%s", tx.To, timestampStr, tx.ID))
			if err := wb.Set(toKey, nil); err != nil {
				return err
			}
		}
		return nil
	}

	// Persist raw (unbatched) transactions that were part of a batch
	for txID, rawTx := range rawTxMap {
		if _, isConsumed := consumedTxIDs[txID]; isConsumed {
			pbTx := modeltoproto.ModelToProtoTransaction(&rawTx)
			txBytes, err := proto.Marshal(pbTx)
			if err != nil {
				return fmt.Errorf("failed to marshal raw tx %s for storage: %w", txID, err)
			}
			if err := wb.Set([]byte(rawTxPrefix+txID), txBytes); err != nil {
				return err
			}
			// Also write to the main transaction prefix so it can be looked up by history queries
			if err := wb.Set([]byte(transactionPrefix+txID), txBytes); err != nil {
				return err
			}
			if err := wb.Set([]byte(processedTxIDPrefix+txID), []byte{1}); err != nil {
				return err
			}
		}
	}

	// Write batched transactions and standard, non-batched transactions
	for _, tx := range finalTxs {
		pbTx := modeltoproto.ModelToProtoTransaction(&tx)
		txBytes, _ := proto.Marshal(pbTx)
		if err := wb.Set([]byte(transactionPrefix+tx.ID), txBytes); err != nil {
			return err
		}
		if err := wb.Set([]byte(processedTxIDPrefix+tx.ID), []byte{1}); err != nil {
			return err
		}
	}

	// Create history indexes for ALL user-facing transactions
	for _, tx := range filteredTxs {
		if err := writeTxHistoryIndexes(wb, tx); err != nil {
			return err
		}
	}

	for txID, rawTx := range rawTxMap {
		if _, isConsumed := consumedTxIDs[txID]; isConsumed {
			if err := writeTxHistoryIndexes(wb, rawTx); err != nil {
				return err
			}
		}
	}

	// --- NEW: Add Stake Index Management ---
	// Iterate through all final transactions to update stake indexes atomically.
	allTxsForStakeIndex := append(append(filteredTxs, batchedTxs...), values(rawTxMap)...)
	for _, tx := range allTxsForStakeIndex {
		switch tx.Type {
		case model.StakeAssignTransaction:
			// Create an index: sidx-node-[NodeID]-[OwnerAddress] -> Amount
			stakeIndexKey := []byte(fmt.Sprintf("sidx-node-%s-%s", tx.TargetNodeID, tx.From))
			amountBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(amountBytes, tx.Amount)
			if err := wb.Set(stakeIndexKey, amountBytes); err != nil {
				return err
			}

		case model.StakeUnassignTransaction:
			// Delete the index for this allocation.
			stakeIndexKey := []byte(fmt.Sprintf("sidx-node-%s-%s", tx.TargetNodeID, tx.From))
			if err := wb.Delete(stakeIndexKey); err != nil {
				// Log error but don't fail the block; a stale index is better than a chain halt.
				log.Printf("WARN: Failed to delete stake index key '%s': %v", stakeIndexKey, err)
			}
		}
	}
	// --- END NEW SECTION ---

	// ============ APPLY TOKEN BATCH ============
	// Apply all token operations to the write batch
	c.tokenBatchMutex.RLock()
	currentBatch := c.currentTokenBatch
	c.tokenBatchMutex.RUnlock()

	if err := c.ApplyTokenBatchToWriteBatch(currentBatch, wb); err != nil {
		return fmt.Errorf("failed to apply token batch: %w", err)
	}
	log.Printf("[PERF-LOG] STEP 5.5: Applied token batch operations. Duration: %v", time.Since(start))
	// ============ END TOKEN BATCH APPLICATION ============

	// Write the final state of all modified accounts
	for _, acc := range accountsToWrite {
		accBytes, err := utils.MarshalAccountState(acc)
		if err != nil {
			return fmt.Errorf("failed to marshal account %s for batch write: %w", acc.Address, err)
		}
		if err := wb.Set([]byte(accountPrefix+acc.Address), accBytes); err != nil {
			return err
		}
	}

	// Write the block metadata itself
	pbBlock := modeltoproto.ModelToProtoBlock(&block)
	blockBytes, err := proto.Marshal(pbBlock)
	if err != nil {
		return fmt.Errorf("failed to marshal block for storage: %w", err)
	}

	if err := wb.Set([]byte(metaPrefix+lastBlockKey), blockBytes); err != nil {
		return fmt.Errorf("failed to store full block %d: %w", block.Height, err)
	}

	log.Printf("[PERF-LOG] STEP 5: Prepared BadgerDB write batch. Duration: %v", time.Since(start))

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("failed to commit block to DB: %w", err)
	}
	log.Printf("[PERF-LOG] STEP 6: Flushed write batch to disk. Duration: %v", time.Since(start))

	for addr, acc := range accountsToWrite {
		shardIndex := fnv32(addr) % uint32(len(c.accountCacheShards))
		shard := &c.accountCacheShards[shardIndex]
		shard.Lock()
		delete(shard.accounts, addr)
		shard.Unlock()
		c.liveAccountStateCache.Store(addr, acc)
	}
	log.Printf("[PERF-LOG] STEP 7: Updated caches. Duration: %v", time.Since(start))

	if c.node != nil {
		c.node.CleanUpReadyChannel(block.Height)
		c.node.PrepareForNextBlock(block.Height + 1)
	}

	var totalIndividualTxs int
	for _, tx := range finalTxs {
		if tx.Type == model.BatchAggregationTransaction && tx.BatchPayload != nil {
			totalIndividualTxs += len(tx.BatchPayload.AggregatedTxIDs)
		} else {
			totalIndividualTxs++
		}
	}

	log.Printf("[PERF-LOG] END: Finished processing block %s (%d txs, %d ops). Total Duration: %v",
		block.Hash[:10], len(finalTxs), totalIndividualTxs, time.Since(start))

	go func(processedBlock types.Block) {
		if len(processedBlock.NextValidators) > 0 {
			c.validatorSetMutex.Lock()
			c.validatorSet = processedBlock.NextValidators
			c.validatorSetMutex.Unlock()
			log.Printf("INFO: Active validator set updated from block %d.", processedBlock.Height)
		}

		nextLeader, err := c.calculateLeader(&processedBlock, processedBlock.Height+1)
		if err != nil {
			log.Printf("Could not determine next leader after processing block %d: %v", processedBlock.Height, err)
			return
		}
		if c.node != nil && nextLeader == c.node.Host.ID() {
			return
		}
		ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
		defer cancel()
		if c.node != nil {
			log.Printf("Sending NextBlockReady for height %d to determined leader %s", processedBlock.Height+1, nextLeader)
			c.node.SendNextBlockReady(ctx, nextLeader, processedBlock.Height+1)
		}
	}(block)

	return nil
}

// values is a helper to get values from a map.
func values(m map[string]types.Transaction) []types.Transaction {
	vals := make([]types.Transaction, 0, len(m))
	for _, v := range m {
		vals = append(vals, v)
	}
	return vals
}

func (c *Client) validateAndRecreateBatches(blockTxs []types.Transaction, proposerID peer.ID) ([]types.Transaction, map[string]struct{}, map[string]types.Transaction, error) {
	start := time.Now()
	log.Printf("[PERF-LOG] START: Validating pre-proposal in parallel")

	var leaderBatches []types.Transaction
	var allRawTxIDsToFetch []string
	for _, tx := range blockTxs {
		if tx.Type == model.BatchAggregationTransaction {
			leaderBatches = append(leaderBatches, tx)
			if tx.BatchPayload != nil {
				allRawTxIDsToFetch = append(allRawTxIDsToFetch, tx.BatchPayload.AggregatedTxIDs...)
			}
		}
	}

	if len(leaderBatches) == 0 {
		log.Printf("[PERF-LOG] END: No batches in proposal to validate. Duration: %v", time.Since(start))
		return nil, make(map[string]struct{}), nil, nil
	}

	rawTxMap, err := c.fetchTransactionsWithRetry(allRawTxIDsToFetch, proposerID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: %v", ErrMissingBatchTxs, err)
	}

	numWorkers := runtime.GOMAXPROCS(0) * 16
	if numWorkers == 0 {
		numWorkers = 8
	}

	jobChan := make(chan types.Transaction, len(leaderBatches))
	resultChan := make(chan batchValidationResult, len(leaderBatches))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for leaderBatch := range jobChan {
				if leaderBatch.BatchPayload == nil {
					resultChan <- batchValidationResult{err: fmt.Errorf("%w: leader proposed a batch with no payload", ErrBatchMismatch)}
					continue
				}

				rawTxsForThisBatch := make([]types.Transaction, 0, len(leaderBatch.BatchPayload.AggregatedTxIDs))
				sort.Strings(leaderBatch.BatchPayload.AggregatedTxIDs)
				for _, rawID := range leaderBatch.BatchPayload.AggregatedTxIDs {
					if tx, ok := rawTxMap[rawID]; ok {
						rawTxsForThisBatch = append(rawTxsForThisBatch, tx)
					} else {
						resultChan <- batchValidationResult{err: fmt.Errorf("%w: could not find raw tx %s for leader batch %s after fetch", ErrMissingBatchTxs, rawID, leaderBatch.ID)}
						return
					}
				}

				locallyCreatedBatches, consumed := c.createBatchedTransactions(rawTxsForThisBatch)

				if len(locallyCreatedBatches) != 1 {
					resultChan <- batchValidationResult{err: fmt.Errorf("%w: validation logic error - expected 1 batch from raw txs, got %d", ErrBatchMismatch, len(locallyCreatedBatches))}
					continue
				}
				localBatch := locallyCreatedBatches[0]

				if localBatch.ID != leaderBatch.ID {
					resultChan <- batchValidationResult{err: fmt.Errorf("%w: local batch ID %s does not match leader's batch ID %s", ErrBatchMismatch, localBatch.ID, leaderBatch.ID)}
					continue
				}

				resultChan <- batchValidationResult{recreatedBatch: localBatch, consumedIDs: consumed}
			}
		}()
	}

	for _, batch := range leaderBatches {
		jobChan <- batch
	}
	close(jobChan)

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	recreatedBatches := make([]types.Transaction, 0, len(leaderBatches))
	allConsumedTxIDs := make(map[string]struct{})

	for res := range resultChan {
		if res.err != nil {
			return nil, nil, nil, res.err
		}
		recreatedBatches = append(recreatedBatches, res.recreatedBatch)
		for id := range res.consumedIDs {
			allConsumedTxIDs[id] = struct{}{}
		}
	}

	if len(leaderBatches) != len(recreatedBatches) {
		return nil, nil, nil, fmt.Errorf("%w: leader proposed %d batches, but local validation successfully recreated %d", ErrBatchMismatch, len(leaderBatches), len(recreatedBatches))
	}

	log.Printf("[PERF-LOG] Reconstructed and validated batches in parallel (%d). Duration: %v", len(recreatedBatches), time.Since(start))
	return recreatedBatches, allConsumedTxIDs, rawTxMap, nil
}

func (c *Client) fetchTransactionsWithRetry(txIDs []string, proposerID peer.ID) (map[string]types.Transaction, error) {
	foundMap := make(map[string]types.Transaction)
	missingIDs := make([]string, len(txIDs))
	copy(missingIDs, txIDs)

	for i := 0; i < 3; i++ {
		if len(missingIDs) == 0 {
			break
		}

		foundTxs, _ := c.GetMempoolTxsByIDs(missingIDs)
		for _, tx := range foundTxs {
			foundMap[tx.ID] = tx
		}

		if len(foundMap) == len(txIDs) {
			break
		}

		stillMissing := make([]string, 0)
		for _, id := range txIDs {
			if _, ok := foundMap[id]; !ok {
				stillMissing = append(stillMissing, id)
			}
		}
		missingIDs = stillMissing

		if len(missingIDs) > 0 && i < 2 {
			time.Sleep(time.Duration(50*(i+1)) * time.Millisecond)
		}
	}

	if len(missingIDs) > 0 && c.node != nil && proposerID != c.node.Host.ID() {
		ctx, cancel := context.WithTimeout(c.ctx, 500*time.Millisecond)
		defer cancel()

		fetchedTxs, err := c.node.RequestMissingTxs(ctx, proposerID, missingIDs)
		if err != nil {
			log.Printf("VALIDATOR: Failed to request %d missing txs from proposer %s: %v", len(missingIDs), proposerID, err)
		} else {
			for _, tx := range fetchedTxs {
				foundMap[tx.ID] = tx
			}
		}
	}

	if len(foundMap) != len(txIDs) {
		return nil, fmt.Errorf("failed to find all %d transactions after retries and network fetch", len(txIDs))
	}

	return foundMap, nil
}

func (c *Client) getTransactionFromDB(txID string) (*types.Transaction, bool) {
	var pbtx pb.Transaction
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(transactionPrefix + txID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return proto.Unmarshal(val, &pbtx)
		})
	})
	if err == nil {
		return prototomodel.ProtoToModelTransaction(&pbtx), true
	}
	return nil, false
}

func (c *Client) GetMempoolTxsByIDs(txIDs []string) ([]types.Transaction, error) {
	var foundTxs []types.Transaction
	foundIDs := make(map[string]struct{})
	c.recentTxMutex.RLock()
	for _, id := range txIDs {
		if tx, exists := c.recentTransactions[id]; exists {
			foundTxs = append(foundTxs, tx)
			foundIDs[id] = struct{}{}
		}
	}
	c.recentTxMutex.RUnlock()
	c.inFlightTxsMutex.RLock()
	for _, id := range txIDs {
		if _, found := foundIDs[id]; !found {
			if tx, ok := c.inFlightTxs[id]; ok {
				foundTxs = append(foundTxs, tx)
				foundIDs[id] = struct{}{}
			}
		}
	}
	c.inFlightTxsMutex.RUnlock()
	for _, id := range txIDs {
		if _, found := foundIDs[id]; !found {
			if tx, ok := c.GetMempoolTxByID(id); ok {
				foundTxs = append(foundTxs, *tx)
			}
		}
	}
	return foundTxs, nil
}

func (c *Client) lockAccountsForBlock(block types.Block) (map[string]*types.AccountState, error) {
	accountAddrs := make(map[string]struct{})

	for _, tx := range block.Transactions {
		if tx.From != "" && tx.From != "network" && tx.From != "system-batch" {
			accountAddrs[tx.From] = struct{}{}
		}
		if tx.To != "" {
			accountAddrs[tx.To] = struct{}{}
		}
		if tx.BatchPayload != nil {
			for addr := range tx.BatchPayload.Debits {
				accountAddrs[addr] = struct{}{}
			}
			for addr := range tx.BatchPayload.Rewards {
				accountAddrs[addr] = struct{}{}
			}
		}
	}

	sortedAddrs := make([]string, 0, len(accountAddrs))
	for addr := range accountAddrs {
		sortedAddrs = append(sortedAddrs, addr)
	}
	sort.Strings(sortedAddrs)

	lockedAccounts := make(map[string]*types.AccountState)
	for _, addr := range sortedAddrs {
		acc, err := c.GetLiveAccountState(addr)
		if err != nil {
			c.unlockAccounts(lockedAccounts)
			return nil, err
		}
		if acc == nil {
			acc = &types.AccountState{Address: addr, StakeInfo: &types.StakeInfo{}, Allowances: make(map[string]uint64)}
		}
		acc.Lock.Lock()
		lockedAccounts[addr] = acc
	}
	return lockedAccounts, nil
}

func (c *Client) unlockAccounts(accounts map[string]*types.AccountState) error {
	sortedAddrs := make([]string, 0, len(accounts))
	for addr := range accounts {
		sortedAddrs = append(sortedAddrs, addr)
	}
	sort.Strings(sortedAddrs)
	for i := len(sortedAddrs) - 1; i >= 0; i-- {
		if acc, ok := accounts[sortedAddrs[i]]; ok {
			acc.Lock.Unlock()
		}
	}
	return nil
}

func (c *Client) transactionGroupWorker(wg *sync.WaitGroup, groups <-chan []types.Transaction, results chan<- error, accounts map[string]*types.AccountState) {
	defer wg.Done()
	for group := range groups {
		for _, tx := range group {
			results <- c.applyTransactionState(tx, accounts)
		}
	}
}

func (c *Client) applyTransactionState(tx types.Transaction, accounts map[string]*types.AccountState) error {
	effectiveType := tx.Type
	if strings.HasPrefix(tx.ID, "tx-rollback-credit-") || strings.HasPrefix(tx.ID, "tx-rollback-fee-credit-") {
		effectiveType = model.RollbackCreditTransaction
	} else if strings.HasPrefix(tx.ID, "tx-rollback-debit-") {
		effectiveType = model.RollbackDebitTransaction
	}

	if isUserNonceTracked(effectiveType) {
		fromAcc, ok := accounts[tx.From]
		if !ok {
			return fmt.Errorf("account %s not found in locked map for tx %s", tx.From, tx.ID)
		}

		if tx.Nonce > fromAcc.Nonce+1 {
			c.processedTxDuringBlockMutex.Lock()
			c.pendingFutureNonces[tx.From] = append(c.pendingFutureNonces[tx.From], tx)
			c.processedTxDuringBlockMutex.Unlock()
			return ErrFutureNonce
		}
		if tx.Nonce <= fromAcc.Nonce {
			return ErrOldNonce
		}

		fromAcc.Nonce = tx.Nonce
		if fromAcc.PendingNonce < fromAcc.Nonce {
			fromAcc.PendingNonce = fromAcc.Nonce
		}
	}

	return c.applyTransactionStateLogic(tx, accounts, effectiveType)
}

func (c *Client) calculatePendingDeductions(address string) uint64 {
	var pendingDeductions uint64
	for i := range c.mempoolShards {
		shard := &c.mempoolShards[i]
		shard.transactions.Range(func(key, value interface{}) bool {
			entry := value.(*MempoolEntry)
			if entry.Transaction.From == address && entry.IsLocallyValid {
				switch entry.Transaction.Type {
				case model.TransferTransaction, model.StakeDepositTransaction:
					pendingDeductions += entry.Transaction.Amount
				}
			}
			return true
		})
	}
	return pendingDeductions
}

func logAccountState(prefix string, txID string, acc *types.AccountState) {
	if acc == nil {
		return
	}
	stakeTotal := uint64(0)
	if acc.StakeInfo != nil {
		stakeTotal = acc.StakeInfo.TotalStake
	}
	log.Printf("%s [%s] Addr: %s, Balance: %d, Nonce: %d, Stake: %d", prefix, txID, acc.Address, acc.Balance, acc.Nonce, stakeTotal)
}

// Modified applyTransactionStateLogic function
// Replace the token transaction cases (around lines 3794-3860) with this version

// Modified applyTransactionStateLogic function
// Replace the token transaction cases (around lines 3794-3860) with this version

func (c *Client) applyTransactionStateLogic(tx types.Transaction, accounts map[string]*types.AccountState, effectiveType types.TransactionType) error {
	c.processedTxDuringBlockMutex.Lock()
	if _, exists := c.processedTxDuringBlock[tx.ID]; exists {
		c.processedTxDuringBlockMutex.Unlock()
		return nil
	}
	c.processedTxDuringBlock[tx.ID] = struct{}{}
	c.processedTxDuringBlockMutex.Unlock()

	switch effectiveType {
	case model.GenesisTransaction:
		toAcc := accounts[tx.To]
		toAcc.Balance += tx.Amount
		if toAcc.StakeInfo == nil {
			toAcc.StakeInfo = &types.StakeInfo{}
		}
		if c.node != nil && c.node.Host != nil {
			bootstrapStake := uint64(1000 * model.GCU_MICRO_UNIT)
			toAcc.StakeInfo.TotalStake += bootstrapStake
			toAcc.StakeInfo.Allocations = append(toAcc.StakeInfo.Allocations, types.StakeAllocation{
				NodePeerID: c.node.Host.ID().String(),
				Amount:     bootstrapStake,
				LockedAt:   time.Now(),
			})
		}

	case model.TransferTransaction:
		fromAcc, toAcc := accounts[tx.From], accounts[tx.To]
		if fromAcc.Balance < tx.Amount {
			return fmt.Errorf("insufficient funds for transfer %s", tx.ID)
		}
		fromAcc.Balance -= tx.Amount
		toAcc.Balance += tx.Amount

	case model.BatchAggregationTransaction:
		for devAddress, totalDebit := range tx.BatchPayload.Debits {
			devAcc, ok := accounts[devAddress]
			if !ok {
				devAcc = &types.AccountState{Address: devAddress, StakeInfo: &types.StakeInfo{}, Allowances: make(map[string]uint64)}
				accounts[devAddress] = devAcc
			}
			devAcc.Balance -= totalDebit
		}
		for opAddress, totalReward := range tx.BatchPayload.Rewards {
			opAcc, ok := accounts[opAddress]
			if !ok {
				opAcc = &types.AccountState{Address: opAddress, StakeInfo: &types.StakeInfo{}, Allowances: make(map[string]uint64)}
				accounts[opAddress] = opAcc
			}
			opAcc.Balance += totalReward
		}

	case model.StakeDepositTransaction:
		acc := accounts[tx.From]
		if acc.StakeInfo == nil {
			acc.StakeInfo = &types.StakeInfo{}
		}
		if acc.Balance < tx.Amount {
			return fmt.Errorf("insufficient spendable balance to deposit to stake")
		}
		acc.Balance -= tx.Amount
		acc.StakeInfo.TotalStake += tx.Amount

	case model.StakeWithdrawalTransaction:
		acc := accounts[tx.From]
		if acc.StakeInfo == nil {
			return fmt.Errorf("account has no stake info to withdraw from")
		}
		c.processUnbondingUnsafe(acc)
		var allocatedAmount, unbondingAmount uint64
		for _, alloc := range acc.StakeInfo.Allocations {
			allocatedAmount += alloc.Amount
		}
		for _, unbond := range acc.StakeInfo.Unbonding {
			unbondingAmount += unbond.Amount
		}
		if acc.StakeInfo.TotalStake-allocatedAmount-unbondingAmount < tx.Amount {
			return fmt.Errorf("insufficient available stake to withdraw")
		}
		acc.StakeInfo.TotalStake -= tx.Amount
		acc.Balance += tx.Amount

	case model.StakeAssignTransaction:
		acc := accounts[tx.From]
		if acc.StakeInfo == nil {
			return fmt.Errorf("account has no stake info to assign from")
		}
		var allocatedAmount, unbondingAmount uint64
		for _, alloc := range acc.StakeInfo.Allocations {
			allocatedAmount += alloc.Amount
		}
		for _, unbond := range acc.StakeInfo.Unbonding {
			unbondingAmount += unbond.Amount
		}
		if acc.StakeInfo.TotalStake-allocatedAmount-unbondingAmount < tx.Amount {
			return fmt.Errorf("insufficient available stake to assign")
		}
		acc.StakeInfo.Allocations = append(acc.StakeInfo.Allocations, types.StakeAllocation{
			NodePeerID: tx.TargetNodeID, Amount: tx.Amount, LockedAt: time.Now(),
		})

	case model.StakeUnassignTransaction:
		acc := accounts[tx.From]
		if acc.StakeInfo == nil {
			return fmt.Errorf("account has no stake info to unassign from")
		}
		var newAllocations []types.StakeAllocation
		found, unassignedAmount := false, uint64(0)
		for _, alloc := range acc.StakeInfo.Allocations {
			if alloc.NodePeerID == tx.TargetNodeID {
				found, unassignedAmount = true, alloc.Amount
			} else {
				newAllocations = append(newAllocations, alloc)
			}
		}
		if !found {
			return fmt.Errorf("no stake allocation found for node %s", tx.TargetNodeID)
		}
		acc.StakeInfo.Allocations = newAllocations
		acc.StakeInfo.Unbonding = append(acc.StakeInfo.Unbonding, types.UnbondingStake{
			Amount: unassignedAmount, UnlockTime: time.Now().Add(UnbondingPeriod),
		})

	case model.SetAllowanceTransaction:
		fromAcc := accounts[tx.From]
		if fromAcc.Allowances == nil {
			fromAcc.Allowances = make(map[string]uint64)
		}
		fromAcc.Allowances[tx.To] = tx.AllowanceLimit
		log.Printf("LEDGER: Set GCU allowance for spender %s on behalf of %s to %d", tx.To, tx.From, tx.AllowanceLimit)

	case model.RemoveAllowanceTransaction:
		fromAcc := accounts[tx.From]
		if fromAcc.Allowances != nil {
			delete(fromAcc.Allowances, tx.To)
			log.Printf("LEDGER: Removed GCU allowance for spender %s on behalf of %s", tx.To, tx.From)
		}

	case model.DelegatedTransferTransaction:
		fromAcc, toAcc := accounts[tx.From], accounts[tx.To]
		spender := tx.Spender

		// *** CRITICAL: Ensure allowances map exists ***
		if fromAcc.Allowances == nil {
			fromAcc.Allowances = make(map[string]uint64)
		}

		allowance, ok := fromAcc.Allowances[spender]
		if !ok {
			log.Printf("LEDGER ERROR: Delegated transfer failed - spender %s has no allowance on account %s", spender, tx.From)
			log.Printf("LEDGER ERROR: Current allowances: %+v", fromAcc.Allowances)
			return fmt.Errorf("delegated transfer failed: spender %s is not approved by %s or no allowance is set", spender, tx.From)
		}

		if allowance < tx.Amount {
			log.Printf("LEDGER ERROR: Delegated transfer failed - insufficient allowance. Spender %s has %d, needs %d", spender, allowance, tx.Amount)
			return fmt.Errorf("delegated transfer failed: amount %d exceeds allowance %d for spender %s", tx.Amount, allowance, spender)
		}

		if fromAcc.Balance < tx.Amount {
			return fmt.Errorf("delegated transfer failed: insufficient funds for account %s", tx.From)
		}

		// Deduct allowance and perform transfer
		fromAcc.Allowances[spender] -= tx.Amount
		fromAcc.Balance -= tx.Amount
		toAcc.Balance += tx.Amount
		log.Printf("LEDGER: Executed delegated transfer of %d from %s to %s by spender %s (remaining allowance: %d)",
			tx.Amount, tx.From, tx.To, spender, fromAcc.Allowances[spender])

	case model.RollbackDebitTransaction:
		if tx.From == "" {
			log.Printf("ERROR: RollbackDebitTransaction %s has no source address.", tx.ID)
			return nil
		}
		fromAcc, ok := accounts[tx.From]
		if !ok {
			return fmt.Errorf("rollback debit failed: account %s not found in locked map", tx.From)
		}
		if fromAcc == nil {
			return fmt.Errorf("rollback debit failed: unexpected nil account for address %s", tx.From)
		}
		if fromAcc.Balance < tx.Amount {
			return fmt.Errorf("rollback debit failed: insufficient balance in account %s", tx.From)
		}
		fromAcc.Balance -= tx.Amount
		log.Printf("LEDGER: Executed rollback debit of %d from %s", tx.Amount, tx.From)

	case model.RollbackCreditTransaction:
		if tx.To == "" {
			log.Printf("ERROR: RollbackCreditTransaction %s has no recipient address.", tx.ID)
			return nil
		}
		toAcc, ok := accounts[tx.To]
		if !ok {
			return fmt.Errorf("rollback credit failed: account %s not found in locked map", tx.To)
		}
		if toAcc == nil {
			return fmt.Errorf("rollback credit failed: unexpected nil account for address %s", tx.To)
		}
		toAcc.Balance += tx.Amount
		log.Printf("LEDGER: Executed rollback credit of %d to %s", tx.Amount, tx.To)

	// ============ TOKEN OPERATIONS (USING BATCH) ============
	case model.TokenCreateTransaction:
		if tx.TokenMetadata == nil {
			return fmt.Errorf("token metadata missing for token creation")
		}

		// Get current token batch
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()

		// Use batch operation instead of direct DB write
		_, err := c.CreateTokenInBatch(
			currentBatch,
			tx.ID,
			tx.From,
			tx.TokenMetadata.Name,
			tx.TokenMetadata.Ticker,
			tx.TokenMetadata.Decimals,
			tx.TokenMetadata.InitialSupply,
			types.TokenType(tx.TokenMetadata.TokenType),
			types.BurnConfig(tx.TokenMetadata.BurnConfig),
			types.MintConfig(tx.TokenMetadata.MintConfig),
			tx.TokenMetadata.Metadata,
		)
		return err

	case model.TokenTransferTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token transfer")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.TransferTokenInBatch(currentBatch, tx.TokenAddress, tx.From, tx.To, tx.Amount, tx.ID)

	case model.TokenMintTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token mint")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.ManualMintTokenInBatch(currentBatch, tx.TokenAddress, tx.From, tx.To, tx.Amount, tx.ID)

	case model.TokenBurnTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token burn")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.ManualBurnTokenInBatch(currentBatch, tx.TokenAddress, tx.From, tx.Amount, tx.ID)

	case model.TokenApproveTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token approve")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.SetTokenAllowanceInBatch(currentBatch, tx.TokenAddress, tx.From, tx.To, tx.AllowanceLimit, tx.ID)

	case model.TokenRevokeTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token revoke")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.DeleteTokenAllowanceInBatch(currentBatch, tx.TokenAddress, tx.From, tx.To, tx.ID)

	case model.TokenLockTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token lock")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.LockTokenConfigInBatch(currentBatch, tx.TokenAddress, tx.From, tx.ID)

	case model.TokenConfigBurnTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token config burn")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.SetBurnableConfigInBatch(currentBatch, tx.TokenAddress, tx.From, types.BurnConfig(tx.TokenMetadata.BurnConfig), tx.ID)

	case model.TokenConfigMintTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address missing for token config mint")
		}
		c.tokenBatchMutex.RLock()
		currentBatch := c.currentTokenBatch
		c.tokenBatchMutex.RUnlock()
		return c.SetMintableConfigInBatch(currentBatch, tx.TokenAddress, tx.From, types.MintConfig(tx.TokenMetadata.MintConfig), tx.ID)
	// ============ END TOKEN OPERATIONS ============

	default:
		return fmt.Errorf("unknown transaction type: %s", tx.Type)
	}

	return nil
}

func (c *Client) GetLiveAccountState(address string) (*types.AccountState, error) {
	if state, ok := c.liveAccountStateCache.Load(address); ok {
		if accState, ok := state.(*types.AccountState); ok {
			return accState, nil
		}
	}

	acc, err := c.getAccount(address)
	if err != nil {
		return nil, err
	}
	if acc != nil {
		c.liveAccountStateCache.Store(address, acc)
	}
	return acc, nil
}

func (c *Client) GetLiveAccountStatesBatch(addresses []string) (map[string]*types.AccountState, error) {
	accounts := make(map[string]*types.AccountState)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, len(addresses))

	for _, addr := range addresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			acc, err := c.GetLiveAccountState(address)
			if err != nil {
				errChan <- err
				return
			}
			mu.Lock()
			accounts[address] = acc
			mu.Unlock()
		}(addr)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return accounts, nil
}

func (c *Client) GetMempoolTxIDs() []string {
	var ids []string
	for i := range c.mempoolShards {
		shard := &c.mempoolShards[i]
		shard.transactions.Range(func(key, value interface{}) bool {
			ids = append(ids, key.(string))
			return true
		})
	}
	return ids
}

func (c *Client) GetMempoolTxByID(txID string) (*types.Transaction, bool) {
	shard := c.getMempoolShard(txID)
	if v, ok := shard.transactions.Load(txID); ok {
		entry := v.(*MempoolEntry)
		return &entry.Transaction, true
	}
	return nil, false
}

func (c *Client) FindMempoolTxByNonce(address string, nonce uint64) (*types.Transaction, bool) {
	var foundTx *types.Transaction
	for i := range c.mempoolShards {
		shard := &c.mempoolShards[i]
		shard.transactions.Range(func(key, value interface{}) bool {
			entry := value.(*MempoolEntry)
			if entry.Transaction.From == address && entry.Transaction.Nonce == nonce {
				txCopy := entry.Transaction
				foundTx = &txCopy
				return false
			}
			return true
		})
		if foundTx != nil {
			return foundTx, true
		}
	}
	return nil, false
}

func (c *Client) runPreValidation(tx *types.Transaction) {
	err := c.preValidateTransaction(*tx)
	shard := c.getMempoolShard(tx.ID)
	if v, ok := shard.transactions.Load(tx.ID); ok {
		currentEntry := v.(*MempoolEntry)
		currentEntry.IsLocallyValid = (err == nil)
		currentEntry.ValidationResult = err
		if err != nil {
			c.storeFailedTransaction(*tx, err.Error())
			if _, deleted := shard.transactions.LoadAndDelete(tx.ID); deleted {
				shard.size.Add(-1)
				if isUserNonceTracked(tx.Type) {
					c.adjustPendingSpend(tx.From, -int64(tx.Amount))
				}
			}
		} else {
			currentEntry.validationLock.Lock()
			currentEntry.Validations[c.node.Host.ID()] = struct{}{}
			currentEntry.validationLock.Unlock()
			c.gossipValidationVote(tx.ID)
		}
	}
}

func (c *Client) preValidateTransaction(tx types.Transaction) error {
	if isProcessed, _ := c.isTransactionProcessed(tx.ID); isProcessed {
		return fmt.Errorf("transaction %s already processed", tx.ID)
	}
	if time.Since(tx.CreatedAt) > TransactionExpiration {
		return fmt.Errorf("transaction expired")
	}

	effectiveType := tx.Type
	if strings.HasPrefix(tx.ID, "tx-rollback-credit-") || strings.HasPrefix(tx.ID, "tx-rollback-fee-credit-") {
		effectiveType = model.RollbackCreditTransaction
	} else if strings.HasPrefix(tx.ID, "tx-rollback-debit-") {
		effectiveType = model.RollbackDebitTransaction
	}

	if effectiveType == model.RollbackCreditTransaction || effectiveType == model.RollbackDebitTransaction {
		return nil
	}

	if tx.From == "" || tx.From == "network" || tx.From == "system-batch" {
		return nil
	}

	fromAccount, err := c.GetLiveAccountState(tx.From)
	if err != nil {
		return fmt.Errorf("could not get account %s: %w", tx.From, err)
	}

	if isUserNonceTracked(tx.Type) && tx.Nonce != 0 {
		if fromAccount != nil && tx.Nonce <= fromAccount.Nonce {
			return fmt.Errorf("invalid nonce for %s", tx.From)
		}
	}

	// Token-specific validations
	switch tx.Type {
	case model.TokenCreateTransaction:
		if tx.TokenMetadata == nil {
			return fmt.Errorf("token metadata required for token creation")
		}
		if tx.TokenMetadata.Name == "" || tx.TokenMetadata.Ticker == "" {
			return fmt.Errorf("token name and ticker are required")
		}
		if tx.TokenMetadata.Decimals > types.MaxDecimals {
			return fmt.Errorf("decimals cannot exceed %d", types.MaxDecimals)
		}
		return nil

	case model.TokenTransferTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		balance, err := c.GetTokenBalance(tx.TokenAddress, tx.From)
		if err != nil {
			return fmt.Errorf("failed to get token balance: %w", err)
		}
		if balance < tx.Amount {
			return fmt.Errorf("insufficient token balance")
		}
		return nil

	case model.TokenMintTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		token, err := c.GetToken(tx.TokenAddress)
		if err != nil {
			return fmt.Errorf("token not found")
		}
		if token.Creator != tx.From {
			return fmt.Errorf("only creator can mint")
		}
		if !token.MintConfig.Enabled || !token.MintConfig.ManualMint {
			return fmt.Errorf("manual mint not enabled")
		}
		return nil

	case model.TokenBurnTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		token, err := c.GetToken(tx.TokenAddress)
		if err != nil {
			return fmt.Errorf("token not found")
		}
		if !token.BurnConfig.Enabled || !token.BurnConfig.ManualBurn {
			return fmt.Errorf("manual burn not enabled")
		}
		balance, err := c.GetTokenBalance(tx.TokenAddress, tx.From)
		if err != nil {
			return fmt.Errorf("failed to get token balance: %w", err)
		}
		if balance < tx.Amount {
			return fmt.Errorf("insufficient token balance to burn")
		}
		return nil

	case model.TokenApproveTransaction, model.TokenRevokeTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		if _, err := c.GetToken(tx.TokenAddress); err != nil {
			return fmt.Errorf("token not found")
		}
		return nil

	case model.TokenLockTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		token, err := c.GetToken(tx.TokenAddress)
		if err != nil {
			return fmt.Errorf("token not found")
		}
		if token.Creator != tx.From {
			return fmt.Errorf("only creator can lock config")
		}
		if token.ConfigLocked {
			return fmt.Errorf("config already locked")
		}
		return nil

	case model.TokenConfigBurnTransaction, model.TokenConfigMintTransaction:
		if tx.TokenAddress == "" {
			return fmt.Errorf("token address required")
		}
		token, err := c.GetToken(tx.TokenAddress)
		if err != nil {
			return fmt.Errorf("token not found")
		}
		if token.Creator != tx.From {
			return fmt.Errorf("only creator can modify config")
		}
		if token.ConfigLocked {
			return fmt.Errorf("config already locked")
		}
		return nil
	}

	// GCU balance and allowance validations
	if fromAccount != nil {
		availableBalance := fromAccount.Balance
		if availableBalance > fromAccount.PendingSpend {
			availableBalance -= fromAccount.PendingSpend
		} else {
			availableBalance = 0
		}

		switch tx.Type {
		case model.DelegatedTransferTransaction:
			// *** CRITICAL FIX: Validate GCU allowances properly ***
			log.Printf("PREVALIDATION: DelegatedTransfer - From: %s, To: %s, Spender: %s, Amount: %d",
				tx.From, tx.To, tx.Spender, tx.Amount)

			if fromAccount.Allowances == nil {
				log.Printf("PREVALIDATION: REJECT - Account %s has no allowances map", tx.From)
				return fmt.Errorf("no allowances set for account %s", tx.From)
			}

			allowance, ok := fromAccount.Allowances[tx.Spender]
			if !ok {
				log.Printf("PREVALIDATION: REJECT - No allowance found for spender %s on account %s",
					tx.Spender, tx.From)
				log.Printf("PREVALIDATION: Available allowances: %+v", fromAccount.Allowances)
				return fmt.Errorf("no allowance for spender %s", tx.Spender)
			}

			log.Printf("PREVALIDATION: Allowance check - Spender %s has %d allowance, needs %d",
				tx.Spender, allowance, tx.Amount)

			if allowance < tx.Amount {
				return fmt.Errorf("insufficient allowance: has %d, needs %d", allowance, tx.Amount)
			}

			log.Printf("PREVALIDATION: Balance check - Account %s has %d available balance, needs %d",
				tx.From, availableBalance, tx.Amount)

			if availableBalance < tx.Amount {
				return fmt.Errorf("insufficient balance: has %d, needs %d", availableBalance, tx.Amount)
			}

			log.Printf("PREVALIDATION: SUCCESS - DelegatedTransfer validated")

		case model.TransferTransaction, model.DebitTransaction:
			if availableBalance < tx.Amount {
				return fmt.Errorf("insufficient balance")
			}
		case model.StakeDepositTransaction:
			if availableBalance < tx.Amount {
				return fmt.Errorf("insufficient balance")
			}
		case model.StakeWithdrawalTransaction:
			var allocated, unbonding uint64
			if fromAccount.StakeInfo != nil {
				for _, a := range fromAccount.StakeInfo.Allocations {
					allocated += a.Amount
				}
				for _, u := range fromAccount.StakeInfo.Unbonding {
					unbonding += u.Amount
				}
				if fromAccount.StakeInfo.TotalStake-allocated-unbonding < tx.Amount {
					return fmt.Errorf("insufficient available stake")
				}
			} else {
				return fmt.Errorf("no stake info")
			}
		case model.StakeAssignTransaction:
			var allocatedAmount, unbondingAmount uint64
			if fromAccount.StakeInfo != nil {
				for _, alloc := range fromAccount.StakeInfo.Allocations {
					allocatedAmount += alloc.Amount
				}
				for _, unbond := range fromAccount.StakeInfo.Unbonding {
					unbondingAmount += unbond.Amount
				}
				if fromAccount.StakeInfo.TotalStake-allocatedAmount-unbondingAmount < tx.Amount {
					return fmt.Errorf("insufficient stake balance")
				}
			} else {
				return fmt.Errorf("insufficient stake balance")
			}
		case model.StakeUnassignTransaction:
			if fromAccount.StakeInfo == nil {
				return fmt.Errorf("no stake info")
			}
			found := false
			for _, alloc := range fromAccount.StakeInfo.Allocations {
				if alloc.NodePeerID == tx.TargetNodeID {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("allocation not found")
			}
		}
	}
	return nil
}

func (c *Client) gossipValidationVote(txID string) {
	if c.node == nil {
		return
	}
	privKey := c.node.Host.Peerstore().PrivKey(c.node.Host.ID())
	if privKey == nil {
		return
	}
	validatorIDStr := c.node.Host.ID().String()
	dataToSign := []byte(fmt.Sprintf("%s:%s", txID, validatorIDStr))
	hash := sha256.Sum256(dataToSign)
	sig, err := privKey.Sign(hash[:])
	if err != nil {
		return
	}
	vote := &pb.PreValidationVote{TxId: txID, ValidatorId: validatorIDStr, ValidatorSig: sig}
	voteBytes, _ := proto.Marshal(vote)
	c.node.Gossip(p2p.PreValidatedTxTopic, voteBytes)
}

func (c *Client) handlePreValidationVoteGossip(data []byte) {
	var vote pb.PreValidationVote
	if err := proto.Unmarshal(data, &vote); err != nil {
		return
	}
	validatorPID, err := peer.Decode(vote.ValidatorId)
	if err != nil {
		return
	}
	pubKey, err := validatorPID.ExtractPublicKey()
	if err != nil {
		return
	}
	dataToVerify := []byte(fmt.Sprintf("%s:%s", vote.TxId, vote.ValidatorId))
	hash := sha256.Sum256(dataToVerify)
	if isValid, err := pubKey.Verify(hash[:], vote.ValidatorSig); err != nil || !isValid {
		return
	}
	shard := c.getMempoolShard(vote.TxId)
	if v, ok := shard.transactions.Load(vote.TxId); ok {
		entry := v.(*MempoolEntry)
		entry.validationLock.Lock()
		defer entry.validationLock.Unlock()
		entry.Validations[validatorPID] = struct{}{}
	}
}

func (c *Client) handleNonceUpdateGossip(data []byte) {
	var update pb.NonceUpdate
	proto.Unmarshal(data, &update)
}

func (c *Client) initiateBlockSync(sourcePeer peer.ID, startHeight, endHeight uint64) {
	if !c.isCatchingUp.CompareAndSwap(false, true) {
		return
	}
	defer c.isCatchingUp.Store(false)
	req := types.BlockRequest{StartHeight: startHeight, Limit: 100}
	blocks, err := c.node.RequestMissingBlocks(c.ctx, sourcePeer, req)
	if err != nil || len(blocks) == 0 {
		return
	}
	c.ProcessCatchupBlocks(blocks)
}

func (c *Client) GetBlocksByRange(startHeight, limit uint64) ([]types.Block, error) {
	var blocks []types.Block
	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(blockPrefix)
		startKey := []byte(fmt.Sprintf("%s%020d", blockPrefix, startHeight))
		for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
			var pbBlock pb.Block
			if err := it.Item().Value(func(val []byte) error { return proto.Unmarshal(val, &pbBlock) }); err != nil {
				continue
			}
			blocks = append(blocks, *prototomodel.ProtoToModelBlock(&pbBlock))
			if uint64(len(blocks)) >= limit {
				break
			}
		}
		return nil
	})
	return blocks, err
}

func (c *Client) ProcessCatchupBlocks(blocks []types.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})
	for _, block := range blocks {
		c.metaLock.RLock()
		currentHeight := c.lastBlockHeight
		currentHash := c.lastBlockHash
		c.metaLock.RUnlock()
		if block.Height != currentHeight+1 || block.PrevHash != currentHash {
			return fmt.Errorf("block sequence error at height %d", block.Height)
		}
		if err := c.processBlock(block); err != nil {
			return fmt.Errorf("failed to process block %d during catch-up: %w", block.Height, err)
		}
	}

	if len(blocks) > 0 && c.node != nil && c.node.Role != "worker" {
		lastProcessed := blocks[len(blocks)-1]
		nextHeight := lastProcessed.Height + 1

		leader, err := c.calculateLeader(&lastProcessed, nextHeight)
		if err == nil && leader != c.node.Host.ID() {
			ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
			defer cancel()
			log.Printf("CATCHUP: Sending ready signal for height %d after processing %d blocks",
				nextHeight, len(blocks))
			c.node.SendNextBlockReady(ctx, leader, nextHeight)
		}
	}

	return nil
}

func (c *Client) syncMempoolFromPeers() {
	if c.node == nil {
		return
	}
	localTxIDs := c.GetMempoolTxIDs()
	knownTxIDs := make(map[string]struct{}, len(localTxIDs))
	for _, id := range localTxIDs {
		knownTxIDs[id] = struct{}{}
	}
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	newTxs, err := c.node.SynchronizeMempool(ctx, knownTxIDs)
	if err != nil {
		return
	}
	for _, tx := range newTxs {
		c.addTxToMempool(tx, false)
	}
}

func (c *Client) HandlePreBlockProposal(data []byte) {
	start := time.Now()
	log.Printf("[PERF-LOG] START: Validating pre-proposal in parallel")

	var pbProposal pb.PreBlockProposal
	if err := proto.Unmarshal(data, &pbProposal); err != nil {
		log.Printf("VALIDATOR: Failed to unmarshal pre-proposal: %v", err)
		return
	}

	// Convert protobuf transactions to model
	modelTxs := make([]types.Transaction, len(pbProposal.Transactions))
	for i, pbtx := range pbProposal.Transactions {
		modelTxs[i] = *prototomodel.ProtoToModelTransaction(pbtx)
	}

	// --- CRITICAL: Convert raw transaction map from protobuf ---
	rawTxMap := make(map[string]types.Transaction)
	for txID, pbTx := range pbProposal.RawTransactions {
		rawTxMap[txID] = *prototomodel.ProtoToModelTransaction(pbTx)
	}

	proposal := types.PreBlockProposal{
		ProposalID:      pbProposal.ProposalId,
		ProposerID:      pbProposal.ProposerId,
		Transactions:    modelTxs,
		Height:          pbProposal.Height,
		PrevHash:        pbProposal.PrevHash,
		RawTransactions: rawTxMap, // <-- CRITICAL: Include raw txs for batch validation
	}

	proposerID, err := peer.Decode(proposal.ProposerID)
	if err != nil {
		log.Printf("VALIDATOR: Failed to decode proposer ID: %v", err)
		return
	}

	// Validate height and prev hash
	c.metaLock.RLock()
	currentHeight := c.lastBlockHeight
	currentBlock := c.lastBlock
	c.metaLock.RUnlock()

	if proposal.Height != currentHeight+1 {
		log.Printf("VALIDATOR: Pre-proposal %s has wrong height (expected %d, got %d)",
			proposal.ProposalID, currentHeight+1, proposal.Height)
		return
	}
	if proposal.PrevHash != currentBlock.Hash {
		log.Printf("VALIDATOR: Pre-proposal %s has wrong prev hash", proposal.ProposalID)
		return
	}

	// --- CRITICAL: Validate and recreate batches using raw transactions ---
	batchedTxs, consumedTxIDs, err := c.validateAndRecreateBatchesWithRawTxs(
		proposal.Transactions,
		proposal.RawTransactions, // <-- Pass raw txs from leader
		proposerID,
	)
	if err != nil {
		log.Printf("VALIDATOR: Pre-proposal %s failed validation: %v", proposal.ProposalID, err)
		return
	}

	// Cache validated proposal
	cacheEntry := ValidatedProposalCacheEntry{
		BatchedTransactions: batchedTxs,
		ConsumedTxIDs:       consumedTxIDs,
		RawTransactions:     proposal.RawTransactions,
		Timestamp:           time.Now(),
	}
	c.validatedProposalCache.Store(proposal.ProposalID, cacheEntry)
	log.Printf("VALIDATOR: Pre-proposal %s validated and cached successfully. Duration: %v",
		proposal.ProposalID, time.Since(start))

	// Sign and send ACK
	privKey := c.node.Host.Peerstore().PrivKey(c.node.Host.ID())
	if privKey == nil {
		log.Printf("VALIDATOR: Failed to get private key for signing ACK")
		return
	}

	hash := sha256.Sum256([]byte(proposal.ProposalID))
	sig, err := privKey.Sign(hash[:])
	if err != nil {
		log.Printf("VALIDATOR: Failed to sign pre-block proposal ACK for %s: %v", proposal.ProposalID, err)
		return
	}

	ack := model.PreBlockAck{
		ProposalID:   proposal.ProposalID,
		ValidatorID:  c.node.Host.ID().String(),
		ValidatorSig: sig,
	}

	ackCtx, ackCancel := context.WithTimeout(c.ctx, 500*time.Millisecond)
	defer ackCancel()
	if err := c.node.SendPreBlockAck(ackCtx, proposerID, ack); err != nil {
		log.Printf("VALIDATOR: Failed to send pre-block ACK for proposal %s to leader %s: %v",
			proposal.ProposalID, proposerID, err)
	}
}

// validateAndRecreateBatchesWithRawTxs validates a pre-proposal by recreating batches
// using the raw transactions provided by the leader
func (c *Client) validateAndRecreateBatchesWithRawTxs(
	proposedTxs []types.Transaction,
	rawTxs map[string]types.Transaction,
	proposerID peer.ID,
) ([]types.Transaction, map[string]struct{}, error) {

	// Separate batch transactions from non-batch transactions
	var batchTxs []types.Transaction
	var nonBatchTxs []types.Transaction
	for _, tx := range proposedTxs {
		if tx.Type == model.BatchAggregationTransaction {
			batchTxs = append(batchTxs, tx)
		} else {
			nonBatchTxs = append(nonBatchTxs, tx)
		}
	}

	if len(batchTxs) == 0 {
		// No batches to validate
		return nil, make(map[string]struct{}), nil
	}

	// Extract raw transactions that are marked as consumed
	var rawTxList []types.Transaction
	for _, rawTx := range rawTxs {
		rawTxList = append(rawTxList, rawTx)
	}

	// Recreate batches using the SAME logic as the leader
	recreatedBatches, consumedIDs := c.createBatchedTransactions(rawTxList)

	// Validate: Number of batches must match
	if len(recreatedBatches) != len(batchTxs) {
		return nil, nil, fmt.Errorf(
			"batch count mismatch: leader has %d batches, validator recreated %d batches",
			len(batchTxs), len(recreatedBatches),
		)
	}

	// Validate: Each batch ID must match
	leaderBatchIDs := make(map[string]struct{})
	for _, batch := range batchTxs {
		leaderBatchIDs[batch.ID] = struct{}{}
	}

	for _, recreatedBatch := range recreatedBatches {
		if _, exists := leaderBatchIDs[recreatedBatch.ID]; !exists {
			// Find the leader's batch ID for better error message
			var leaderBatchID string
			if len(batchTxs) > 0 {
				leaderBatchID = batchTxs[0].ID
			}
			return nil, nil, fmt.Errorf(
				"leader's batch transaction does not match local validation: "+
					"local batch ID %s does not match leader's batch ID %s",
				recreatedBatch.ID, leaderBatchID,
			)
		}
	}

	log.Printf("VALIDATOR: Successfully validated %d batch transactions", len(recreatedBatches))
	return recreatedBatches, consumedIDs, nil
}

func (c *Client) scheduleRecentTxCleanup() {
	go c.boundRecentTransactions()
	c.recentTxMutex.Lock()
	if c.recentTxCleanup != nil {
		c.recentTxCleanup.Stop()
	}
	c.recentTxCleanup = time.AfterFunc(10*time.Minute, func() {
		c.recentTxMutex.Lock()
		c.recentTransactions = make(map[string]types.Transaction)
		c.recentTxMutex.Unlock()
	})
	c.recentTxMutex.Unlock()
}

func (c *Client) boundRecentTransactions() {
	c.recentTxMutex.Lock()
	defer c.recentTxMutex.Unlock()
	const maxRecentTxs = 2000
	if len(c.recentTransactions) > maxRecentTxs {
		type txEntry struct {
			id        string
			createdAt time.Time
		}
		entries := make([]txEntry, 0, len(c.recentTransactions))
		for id, tx := range c.recentTransactions {
			entries = append(entries, txEntry{id: id, createdAt: tx.CreatedAt})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].createdAt.Before(entries[j].createdAt) })
		for i := 0; i < len(entries)-maxRecentTxs; i++ {
			delete(c.recentTransactions, entries[i].id)
		}
	}
}

// ensureTransactionPropagation now checks if 2/3+ validators already have the transactions
func (c *Client) ensureTransactionPropagation(ctx context.Context, txIDs []string) error {
	start := time.Now()

	if len(txIDs) == 0 {
		return nil
	}

	// --- NEW: Check propagation state first ---
	validators := c.getOnlineValidators()
	if len(validators) == 0 {
		log.Printf("TX-PROPAGATION: No validators available for propagation check")
		return fmt.Errorf("no validators available")
	}

	quorumSize := (len(validators) * 2 / 3) + 1

	// Count how many validators already have each transaction
	needsPropagation := make(map[string]bool)
	for _, txID := range txIDs {
		propagatedCount := 0
		if peerMapRaw, ok := c.txPropagationState.Load(txID); ok {
			peerMap := peerMapRaw.(map[string]time.Time)

			// Count how many online validators have this transaction
			for _, v := range validators {
				if _, hasTx := peerMap[v.PeerID.String()]; hasTx {
					propagatedCount++
				}
			}
		}

		if propagatedCount >= quorumSize {
			log.Printf("TX-PROPAGATION: TX %s already at %d/%d validators (quorum met)",
				txID[:8], propagatedCount, quorumSize)
		} else {
			needsPropagation[txID] = true
			log.Printf("TX-PROPAGATION: TX %s needs propagation: %d/%d validators",
				txID[:8], propagatedCount, quorumSize)
		}
	}

	// If 2/3+ validators already have ALL transactions, skip propagation entirely
	if len(needsPropagation) == 0 {
		log.Printf("PERF-LOG: Skipped ensureTransactionPropagation (all TXs already at quorum). Duration: %v", time.Since(start))
		return nil
	}

	// Only propagate transactions that haven't reached quorum
	txIDsToPropagate := make([]string, 0, len(needsPropagation))
	for txID := range needsPropagation {
		txIDsToPropagate = append(txIDsToPropagate, txID)
	}
	// --- END NEW ---

	propCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	peers := c.node.GetPeerList()
	if len(peers) == 0 {
		return nil
	}

	// Query each validator to see which transactions they're missing
	allMissingTxs := make(map[string]struct{})
	var missingMutex sync.Mutex

	concurrentQueries := 5
	semaphore := make(chan struct{}, concurrentQueries)

	var wg sync.WaitGroup
	for _, peerID := range peers {
		if propCtx.Err() != nil {
			break
		}

		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-propCtx.Done():
				return
			}

			// --- MODIFIED: Only query for transactions that need propagation ---
			knownTxs, err := c.node.RequestPeerKnownTxs(propCtx, p, txIDsToPropagate)
			// --- END MODIFIED ---
			if err != nil {
				return
			}

			var missingForPeer []types.Transaction
			for _, id := range txIDsToPropagate {
				if _, known := knownTxs[id]; !known {
					if tx, ok := c.GetMempoolTxByID(id); ok {
						missingForPeer = append(missingForPeer, *tx)

						missingMutex.Lock()
						allMissingTxs[id] = struct{}{}
						missingMutex.Unlock()
					}
				} else {
					// --- NEW: Mark that this peer has the transaction ---
					c.recordTxPropagationFromPeer(id, p)
					// --- END NEW ---
				}
			}

			if len(missingForPeer) > 0 {
				c.pushTransactionsToPeer(propCtx, p, missingForPeer)

				// --- NEW: Mark these transactions as sent to this peer ---
				for _, tx := range missingForPeer {
					c.recordTxPropagationFromPeer(tx.ID, p)
				}
				// --- END NEW ---
			}
		}(peerID)
	}

	wg.Wait()

	if propCtx.Err() != nil {
		return fmt.Errorf("propagation timed out: %w", propCtx.Err())
	}

	log.Printf("PERF-LOG: Ensured propagation for %d TXs (%d needed sync). Duration: %v",
		len(txIDs), len(needsPropagation), time.Since(start))
	return nil
}

func (c *Client) getTxsForPropagation(txIDs []string) []types.Transaction {
	var txs []types.Transaction
	found := make(map[string]struct{})
	for _, id := range txIDs {
		if tx, ok := c.GetMempoolTxByID(id); ok {
			txs = append(txs, *tx)
			found[id] = struct{}{}
		}
	}
	c.recentTxMutex.RLock()
	for _, id := range txIDs {
		if _, ok := found[id]; !ok {
			if tx, exists := c.recentTransactions[id]; exists {
				txs = append(txs, tx)
			}
		}
	}
	c.recentTxMutex.RUnlock()
	return txs
}

func (c *Client) pushTransactionsToPeer(ctx context.Context, peerID peer.ID, transactions []types.Transaction) {
	if len(transactions) == 0 {
		return
	}
	pbTxs := make([]*pb.Transaction, len(transactions))
	for i, tx := range transactions {
		pbTxs[i] = modeltoproto.ModelToProtoTransaction(&tx)
	}
	txList := &pb.TransactionList{Transactions: pbTxs}
	batchBytes, err := proto.Marshal(txList)
	if err != nil {
		return
	}
	c.node.Gossip(p2p.LedgerTransactionTopic, batchBytes)
}

func (c *Client) adjustPendingSpend(address string, delta int64) {
	if address == "" || address == "network" || address == "system-batch" {
		return
	}
	account, err := c.GetLiveAccountState(address)
	if err != nil || account == nil {
		return
	}
	accountLock := c.getAccountLock(address)
	accountLock.Lock()
	defer accountLock.Unlock()
	if delta > 0 {
		account.PendingSpend += uint64(delta)
	} else {
		absDelta := uint64(-delta)
		if account.PendingSpend >= absDelta {
			account.PendingSpend -= absDelta
		} else {
			account.PendingSpend = 0
		}
	}
	shardIndex := fnv32(address) % uint32(len(c.accountCacheShards))
	shard := &c.accountCacheShards[shardIndex]
	shard.Lock()
	if cachedAcc, exists := shard.accounts[address]; exists {
		cachedAcc.PendingSpend = account.PendingSpend
	}
	shard.Unlock()
}

func (c *Client) adjustPendingSpendUnsafe(account *types.AccountState, delta int64) {
	if account == nil {
		return
	}
	if delta > 0 {
		account.PendingSpend += uint64(delta)
	} else {
		absDelta := uint64(-delta)
		if account.PendingSpend >= absDelta {
			account.PendingSpend -= absDelta
		} else {
			account.PendingSpend = 0
		}
	}
}

func (c *Client) connectToValidators() {
	time.Sleep(5 * time.Second)
	c.validatorSetMutex.RLock()
	validators := make([]types.ValidatorInfo, len(c.validatorSet))
	copy(validators, c.validatorSet)
	c.validatorSetMutex.RUnlock()
	if c.node == nil {
		return
	}
	for _, v := range validators {
		if v.PeerID == c.node.Host.ID() {
			continue
		}
		go func(validatorID peer.ID) {
			for i := 0; i < 3; i++ {
				if c.node.IsPeerConnected(validatorID) {
					return
				}
				if err := c.node.ConnectToPeerByID(validatorID); err != nil {
					time.Sleep(5 * time.Second)
				} else {
					return
				}
			}
		}(v.PeerID)
	}
}

func (c *Client) RevalidateValidators() {
	c.recalculateValidatorSet()
}

func (c *Client) recalculateValidatorSet() {
	c.validatorSetMutex.Lock()
	defer c.validatorSetMutex.Unlock()
	c.recalculateValidatorSetUnlocked()
}

func (c *Client) recalculateValidatorSetUnlocked() {
	stakedTotals := make(map[string]uint64)
	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(accountPrefix)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var acc *types.AccountState
			if err := it.Item().Value(func(v []byte) error {
				var unmarshalErr error
				acc, unmarshalErr = utils.UnmarshalAccountState(v)
				return unmarshalErr
			}); err != nil {
				continue
			}
			if acc.StakeInfo != nil {
				for _, alloc := range acc.StakeInfo.Allocations {
					stakedTotals[alloc.NodePeerID] += alloc.Amount
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("ERROR: Failed to recalculate validator set: %v", err)
		return
	}
	newValidatorSet := make([]types.ValidatorInfo, 0)
	for pStr, totalStake := range stakedTotals {
		pID, err := peer.Decode(pStr)
		if err != nil {
			log.Printf("WARN: Invalid peer ID '%s' in stake total: %v", pStr, err)
			continue
		}
		if totalStake > 0 {
			newValidatorSet = append(newValidatorSet, types.ValidatorInfo{PeerID: pID, TotalStake: totalStake})
		}
	}

	oldSetCopy := make([]types.ValidatorInfo, len(c.validatorSet))
	copy(oldSetCopy, c.validatorSet)
	sort.Slice(oldSetCopy, func(i, j int) bool {
		return oldSetCopy[i].PeerID.String() < oldSetCopy[j].PeerID.String()
	})

	sort.Slice(newValidatorSet, func(i, j int) bool {
		return newValidatorSet[i].PeerID.String() < newValidatorSet[j].PeerID.String()
	})

	isDifferent := len(oldSetCopy) != len(newValidatorSet)
	if !isDifferent {
		for i := range oldSetCopy {
			if i >= len(newValidatorSet) || oldSetCopy[i].PeerID != newValidatorSet[i].PeerID ||
				oldSetCopy[i].TotalStake != newValidatorSet[i].TotalStake {
				isDifferent = true
				break
			}
		}
	}

	if isDifferent {
		c.validatorSet = newValidatorSet
		log.Printf("INFO: Validator set updated: %d validators", len(c.validatorSet))
		c.persistAndBroadcastValidatorSet()
	}
}

func (c *Client) persistAndBroadcastValidatorSet() {

	pbValidators := make(map[string]uint64)
	for _, v := range c.validatorSet {
		pbValidators[v.PeerID.String()] = v.TotalStake
	}

	update := &pb.ValidatorSetUpdate{
		Validators: pbValidators,
	}

	if c.node != nil && c.node.Host != nil {
		update.Source = c.node.Host.ID().String()
	}
	update.Timestamp = timestamppb.Now()

	updateBytes, err := proto.Marshal(update)
	if err != nil {
		log.Printf("WARN: Failed to marshal validator set update: %v", err)
		return
	}

	if err := c.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaPrefix+validatorSetKey), updateBytes)
	}); err != nil {
		log.Printf("WARN: Failed to persist validator set to DB: %v", err)
	}

	if c.node != nil {
		if err := c.node.Gossip(p2p.ValidatorUpdateTopic, updateBytes); err != nil {
			log.Printf("WARN: Failed to gossip validator set update: %v", err)
		}
	}
}

func (c *Client) HandleValidatorUpdate(data []byte) {
	var update pb.ValidatorSetUpdate
	if err := proto.Unmarshal(data, &update); err != nil {
		log.Printf("WARN: Failed to unmarshal validator update: %v", err)
		return
	}

	c.validatorSetMutex.Lock()
	defer c.validatorSetMutex.Unlock()

	if len(update.Validators) == len(c.validatorSet) {
		isSame := true
		for peerIDStr, stake := range update.Validators {
			found := false
			for _, currentValidator := range c.validatorSet {
				if currentValidator.PeerID.String() == peerIDStr && currentValidator.TotalStake == stake {
					found = true
					break
				}
			}
			if !found {
				isSame = false
				break
			}
		}
		if isSame {
			return
		}
	}

	c.recalculateValidatorSetUnlocked()
}

func (c *Client) HandlePeerConnectionEvent(peerID peer.ID) {
	go func() {
		time.Sleep(500 * time.Millisecond) // Allow network state to settle
		c.metaLock.RLock()
		currentBlock := c.lastBlock
		c.metaLock.RUnlock()
		nextLeader, err := c.calculateLeader(currentBlock, currentBlock.Height+1)
		if err != nil {
			return
		}
		if c.node == nil {
			return
		}
		if nextLeader != c.node.Host.ID() {
			ctx, cancel := context.WithTimeout(c.ctx, 3*time.Second)
			defer cancel()
			c.node.SendNextBlockReady(ctx, nextLeader, currentBlock.Height+1)
		}
	}()
}

func (c *Client) createBatchedTransactions(candidateTxs []types.Transaction) ([]types.Transaction, map[string]struct{}) {
	consumedTxIDs := make(map[string]struct{})
	txsToBatchByType := make(map[types.TransactionType][]types.Transaction)

	// Group batchable transactions by type
	for _, tx := range candidateTxs {
		if isBatchableTransaction(tx) {
			txsToBatchByType[tx.Type] = append(txsToBatchByType[tx.Type], tx)
		}
	}

	var finalBatchedTxs []types.Transaction

	// Sort transaction types for deterministic processing
	txTypes := make([]types.TransactionType, 0, len(txsToBatchByType))
	for txType := range txsToBatchByType {
		txTypes = append(txTypes, txType)
	}
	sort.Slice(txTypes, func(i, j int) bool {
		return txTypes[i] < txTypes[j]
	})

	// Create batch transactions
	for _, txType := range txTypes {
		txs := txsToBatchByType[txType]
		if len(txs) == 0 {
			continue
		}

		batchPayload := &types.BatchAggregationPayload{
			Debits:          make(map[string]uint64),
			Rewards:         make(map[string]uint64),
			AggregatedTxIDs: make([]string, 0, len(txs)),
		}

		// Sort transactions by ID for deterministic batching
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].ID < txs[j].ID
		})

		// Aggregate transaction effects
		for _, tx := range txs {
			batchPayload.AggregatedTxIDs = append(batchPayload.AggregatedTxIDs, tx.ID)
			consumedTxIDs[tx.ID] = struct{}{}

			switch tx.Type {
			case model.TransferTransaction:
				if tx.From != "" && tx.Amount > 0 {
					batchPayload.Debits[tx.From] += tx.Amount
				}
				if tx.To != "" && tx.Amount > 0 {
					batchPayload.Rewards[tx.To] += tx.Amount
				}
			case model.DebitTransaction:
				if tx.From != "" && tx.Amount > 0 {
					batchPayload.Debits[tx.From] += tx.Amount
				}
			case model.RewardTransaction:
				if tx.To != "" && tx.Amount > 0 {
					batchPayload.Rewards[tx.To] += tx.Amount
				}
			}
		}

		// Generate deterministic batch ID
		sort.Strings(batchPayload.AggregatedTxIDs)
		batchID := c.generateBatchTxID(txType, batchPayload)

		batchTx := types.Transaction{
			ID:           batchID,
			Type:         model.BatchAggregationTransaction,
			From:         "system-batch",
			BatchPayload: batchPayload,
		}

		finalBatchedTxs = append(finalBatchedTxs, batchTx)
	}

	return finalBatchedTxs, consumedTxIDs
}

func (c *Client) generateBatchTxID(txType types.TransactionType, payload *types.BatchAggregationPayload) string {
	var content strings.Builder
	content.WriteString(string(txType))
	content.WriteString(strings.Join(payload.AggregatedTxIDs, ""))

	debitKeys := make([]string, 0, len(payload.Debits))
	for k := range payload.Debits {
		debitKeys = append(debitKeys, k)
	}
	sort.Strings(debitKeys)
	for _, k := range debitKeys {
		content.WriteString(fmt.Sprintf("d%s%d", k, payload.Debits[k]))
	}

	rewardKeys := make([]string, 0, len(payload.Rewards))
	for k := range payload.Rewards {
		rewardKeys = append(rewardKeys, k)
	}
	sort.Strings(rewardKeys)
	for _, k := range rewardKeys {
		content.WriteString(fmt.Sprintf("r%s%d", k, payload.Rewards[k]))
	}

	hash := sha256.Sum256([]byte(content.String()))
	return fmt.Sprintf("batch-%s-%x", txType, hash[:12])
}

// Add token transaction support to isBatchableTransaction
func isBatchableTransaction(tx types.Transaction) bool {
	// Token transactions should NOT be batched
	switch tx.Type {
	case model.TokenCreateTransaction, model.TokenTransferTransaction,
		model.TokenMintTransaction, model.TokenBurnTransaction,
		model.TokenApproveTransaction, model.TokenRevokeTransaction,
		model.TokenLockTransaction, model.TokenConfigBurnTransaction,
		model.TokenConfigMintTransaction:
		return false
	case model.TransferTransaction, model.DebitTransaction, model.RewardTransaction:
		return true
	case model.RollbackDebitTransaction, model.RollbackCreditTransaction:
		return false
	default:
		return false
	}
}

func (c *Client) assignNoncesInPlace(transactions []types.Transaction) ([]types.Transaction, error) {
	txsToAssign := make(map[string][]types.Transaction)
	otherTxs := make([]types.Transaction, 0)
	involvedAddrs := make(map[string]struct{})

	for _, tx := range transactions {
		if isUserNonceTracked(tx.Type) && tx.Nonce == 0 {
			txsToAssign[tx.From] = append(txsToAssign[tx.From], tx)
			involvedAddrs[tx.From] = struct{}{}
		} else {
			otherTxs = append(otherTxs, tx)
		}
	}

	if len(txsToAssign) == 0 {
		return transactions, nil
	}

	addresses := make([]string, 0, len(involvedAddrs))
	for addr := range involvedAddrs {
		addresses = append(addresses, addr)
	}

	liveAccounts, err := c.GetLiveAccountStatesBatch(addresses)
	if err != nil {
		return nil, fmt.Errorf("failed to get batch account states for nonce assignment: %w", err)
	}

	processedTxs := make([]types.Transaction, 0, len(transactions))
	processedTxs = append(processedTxs, otherTxs...)

	for addr, txs := range txsToAssign {
		acc, ok := liveAccounts[addr]
		if !ok {
			acc = &types.AccountState{Address: addr, Nonce: 0, PendingNonce: 0, StakeInfo: &types.StakeInfo{}}
		}

		sort.Slice(txs, func(i, j int) bool {
			return txs[i].CreatedAt.Before(txs[j].CreatedAt)
		})

		nextNonce := acc.PendingNonce + 1
		for i := range txs {
			txs[i].Nonce = nextNonce
			processedTxs = append(processedTxs, txs[i])
			nextNonce++
		}
		acc.PendingNonce = nextNonce - 1
	}

	for addr, state := range liveAccounts {
		shardIndex := fnv32(addr) % uint32(len(c.accountCacheShards))
		shard := &c.accountCacheShards[shardIndex]
		shard.Lock()
		shard.accounts[addr] = state
		shard.Unlock()
	}

	return processedTxs, nil
}
