package types

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type TransactionType string

type StakeAllocation struct {
	NodePeerID string    `json:"nodePeerId"`
	Amount     uint64    `json:"amount"`
	LockedAt   time.Time `json:"lockedAt"`
}

type UnbondingStake struct {
	Amount     uint64    `json:"amount"`
	UnlockTime time.Time `json:"unlockTime"`
}

type StakeInfo struct {
	TotalStake  uint64            `json:"totalStake"`
	Allocations []StakeAllocation `json:"allocations"`
	Unbonding   []UnbondingStake  `json:"unbonding"`
}

type AccountState struct {
	Address      string            `json:"address"`
	Balance      uint64            `json:"balance"`
	Nonce        uint64            `json:"nonce"`
	PendingNonce uint64            `json:"pendingNonce"`
	PendingSpend uint64            `json:"pendingSpend"`
	StakeInfo    *StakeInfo        `json:"stakeInfo,omitempty"`
	Allowances   map[string]uint64 `json:"allowances,omitempty"`
	Lock         sync.RWMutex      `json:"-"`
}

type BatchAggregationPayload struct {
	Debits          map[string]uint64
	Rewards         map[string]uint64
	AggregatedTxIDs []string
}

type Transaction struct {
	ID             string                   `json:"id"`
	Type           TransactionType          `json:"type"`
	From           string                   `json:"from"`
	To             string                   `json:"to,omitempty"`
	Amount         uint64                   `json:"amount,omitempty"`
	Nonce          uint64                   `json:"nonce"`
	Timestamp      time.Time                `json:"timestamp"`
	CreatedAt      time.Time                `json:"createdAt"`
	TargetNodeID   string                   `json:"targetNodeId,omitempty"`
	BatchPayload   *BatchAggregationPayload `json:"batchPayload,omitempty"`
	Spender        string                   `json:"spender,omitempty"`
	AllowanceLimit uint64                   `json:"allowanceLimit,omitempty"`
	// Add token-specific fields
	TokenAddress  string         `json:"tokenAddress,omitempty"`
	TokenMetadata *TokenMetadata `json:"tokenMetadata,omitempty"`
}

type ValidatorInfo struct {
	PeerID     peer.ID
	TotalStake uint64
}

type Block struct {
	ProposalID     string
	ProposerID     string
	Transactions   []Transaction
	Timestamp      time.Time
	Hash           string
	Height         uint64
	PrevHash       string
	NextValidators []ValidatorInfo
}

type PreBlockProposal struct {
	ProposalID      string
	ProposerID      string
	Transactions    []Transaction
	Height          uint64
	PrevHash        string
	RawTransactions map[string]Transaction
}

type BlockRequest struct {
	StartHeight uint64
	Limit       uint64
}

type BlockShred struct {
	BlockHash   string
	TotalShreds int
	ShredIndex  int
	Data        []byte
}
