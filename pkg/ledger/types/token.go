package types

import (
	"sync"
	"time"
)

const (
	TokenPrefix          = "token-"
	TokenOwnerPrefix     = "token-owner-"
	TokenBalancePrefix   = "token-bal-"
	TokenAllowancePrefix = "token-allow-"
	TokenMetadataPrefix  = "token-meta-"
	TokenHistoryPrefix   = "token-hist-"

	MaxMetadataSize = 1024 // 1KB max metadata
	MaxDecimals     = 6
)

type TokenType string

const (
	FungibleToken    TokenType = "FUNGIBLE"
	NonFungibleToken TokenType = "NFT"
	RealWorldAsset   TokenType = "RWA"
)

type BurnConfig struct {
	Enabled       bool    `json:"enabled"`
	BurnRatePerTx float64 `json:"burnRatePerTx"` // Percentage burned per transaction (0-100)
	ManualBurn    bool    `json:"manualBurn"`    // Allow manual burn operations
}

type MintConfig struct {
	Enabled       bool    `json:"enabled"`
	MintRatePerTx float64 `json:"mintRatePerTx"` // Percentage minted per transaction (0-100)
	ManualMint    bool    `json:"manualMint"`    // Allow manual mint operations
}

type TokenMetadata struct {
	Name          string     `json:"name"`
	Ticker        string     `json:"ticker"`
	Decimals      uint8      `json:"decimals"`
	InitialSupply uint64     `json:"initialSupply"`
	TokenType     string     `json:"tokenType"`
	BurnConfig    BurnConfig `json:"burnConfig"`
	MintConfig    MintConfig `json:"mintConfig"`
	Metadata      string     `json:"metadata"`
}

type Token struct {
	Address       string       `json:"address"` // Unique token address
	Name          string       `json:"name"`
	Ticker        string       `json:"ticker"`
	Decimals      uint8        `json:"decimals"`
	InitialSupply uint64       `json:"initialSupply"`
	TotalSupply   uint64       `json:"totalSupply"`
	Creator       string       `json:"creator"`
	CreatedAt     time.Time    `json:"createdAt"`
	TokenType     TokenType    `json:"tokenType"`
	BurnConfig    BurnConfig   `json:"burnConfig"`
	MintConfig    MintConfig   `json:"mintConfig"`
	ConfigLocked  bool         `json:"configLocked"` // Lock burn/mint config
	Metadata      string       `json:"metadata"`     // JSON/text/link (max 1KB)
	Lock          sync.RWMutex `json:"-"`
}

type TokenBalance struct {
	TokenAddress string     `json:"tokenAddress"`
	Owner        string     `json:"owner"`
	Balance      uint64     `json:"balance"`
	Lock         sync.Mutex `json:"-"`
}

type TokenAllowance struct {
	TokenAddress string `json:"tokenAddress"`
	Owner        string `json:"owner"`
	Spender      string `json:"spender"`
	Amount       uint64 `json:"amount"`
}

type TokenTransfer struct {
	TokenAddress string    `json:"tokenAddress"`
	From         string    `json:"from"`
	To           string    `json:"to"`
	Amount       uint64    `json:"amount"`
	Timestamp    time.Time `json:"timestamp"`
	TxHash       string    `json:"txHash"`
}

// Token operation types for history
type TokenOperation string

const (
	OpCreate     TokenOperation = "CREATE"
	OpTransfer   TokenOperation = "TRANSFER"
	OpMint       TokenOperation = "MINT"
	OpBurn       TokenOperation = "BURN"
	OpApprove    TokenOperation = "APPROVE"
	OpConfigMint TokenOperation = "CONFIG_MINT"
	OpConfigBurn TokenOperation = "CONFIG_BURN"
	OpConfigLock TokenOperation = "CONFIG_LOCK"
)

type TokenHistory struct {
	TokenAddress string         `json:"tokenAddress"`
	Operation    TokenOperation `json:"operation"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	Amount       uint64         `json:"amount"`
	Timestamp    time.Time      `json:"timestamp"`
	TxHash       string         `json:"txHash"`
}
