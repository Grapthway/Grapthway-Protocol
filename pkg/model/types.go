package model

import (
	"context"
	"time"

	"grapthway/pkg/crypto"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/router"

	localdht "grapthway/pkg/dht"
)

const (
	GCU_MICRO_UNIT = 1_000_000
)

const (
	TransferTransaction          types.TransactionType = "TRANSFER"
	DebitTransaction             types.TransactionType = "DEBIT"
	RewardTransaction            types.TransactionType = "REWARD"
	GenesisTransaction           types.TransactionType = "GENESIS"
	StakeDepositTransaction      types.TransactionType = "STAKE_DEPOSIT"
	StakeWithdrawalTransaction   types.TransactionType = "STAKE_WITHDRAWAL"
	StakeAssignTransaction       types.TransactionType = "STAKE_ASSIGN"
	StakeUnassignTransaction     types.TransactionType = "STAKE_UNASSIGN"
	BatchAggregationTransaction  types.TransactionType = "BATCH_AGGREGATION"
	SetAllowanceTransaction      types.TransactionType = "SET_ALLOWANCE"
	RemoveAllowanceTransaction   types.TransactionType = "REMOVE_ALLOWANCE"
	DelegatedTransferTransaction types.TransactionType = "DELEGATED_TRANSFER"
	RollbackDebitTransaction     types.TransactionType = "ROLLBACK_DEBIT"
	RollbackCreditTransaction    types.TransactionType = "ROLLBACK_CREDIT"
	// Add token transaction types
	TokenCreateTransaction     types.TransactionType = "TOKEN_CREATE"
	TokenTransferTransaction   types.TransactionType = "TOKEN_TRANSFER"
	TokenMintTransaction       types.TransactionType = "TOKEN_MINT"
	TokenBurnTransaction       types.TransactionType = "TOKEN_BURN"
	TokenApproveTransaction    types.TransactionType = "TOKEN_APPROVE"
	TokenRevokeTransaction     types.TransactionType = "TOKEN_REVOKE"
	TokenLockTransaction       types.TransactionType = "TOKEN_LOCK"
	TokenConfigBurnTransaction types.TransactionType = "TOKEN_CONFIG_BURN"
	TokenConfigMintTransaction types.TransactionType = "TOKEN_CONFIG_MINT"
)

type PreBlockAck struct {
	ProposalID   string
	ValidatorID  string
	ValidatorSig []byte
}

type NextBlockReady struct {
	BlockHeight uint64
	ValidatorID string
}

type MissingTxRequest struct {
	Address string
	Nonce   uint64
}

type ValidatorSetUpdate struct {
	Validators map[string]uint64
	Source     string
	Timestamp  time.Time
}

type ContextKey string

const FeeInfoKey ContextKey = "feeInfo"

type FeeInfo struct {
	DeveloperID    string
	NodeOperatorID string
	Cost           uint64
}

type HardwareInfoProvider interface {
	GetHardwareInfo() HardwareInfo
}

type CgroupStats struct {
	MemoryLimitBytes uint64  `json:"memory_limit_bytes"`
	CPUQuotaCores    float64 `json:"cpu_quota_cores"`
}

type SystemStats struct {
	CPUUser, CPUNice, CPUSystem, CPUIdle, CPUIowait, CPUirq, CPUSoftirq, CPUSteal uint64
	MemTotal, MemFree, MemAvailable, Buffers, Cached                              uint64
	NetBytesSent, NetBytesRecv                                                    uint64
	DiskReadBytes, DiskWriteBytes                                                 uint64
}

type UsageStats struct {
	CPUTotalUsagePercent float64 `json:"cpu_total_usage_percent"`
	CPUUsedCores         float64 `json:"cpu_used_cores"`
	RAMUsedBytes         uint64  `json:"ram_used_bytes"`
	RAMTotalBytes        uint64  `json:"ram_total_bytes"`
	RAMUsagePercent      float64 `json:"ram_usage_percent"`
	NetSentBps           float64 `json:"net_sent_bps"`
	NetRecvBps           float64 `json:"net_recv_bps"`
	DiskReadBps          float64 `json:"disk_read_bps"`
	DiskWriteBps         float64 `json:"disk_write_bps"`
}

type HardwareInfo struct {
	NodeID       string      `json:"node_id"`
	Role         string      `json:"role"`
	CgroupLimits CgroupStats `json:"cgroup_limits"`
	Usage        UsageStats  `json:"usage"`
}

type HardwareStatsGossip struct {
	SenderStats HardwareInfo            `json:"sender_stats"`
	KnownPeers  map[string]HardwareInfo `json:"known_peers"`
}

type GlobalHardwareStats struct {
	TotalNodes          int     `json:"total_nodes"`
	TotalServers        int     `json:"total_servers"`
	TotalWorkers        int     `json:"total_workers"`
	TotalRAMBytes       uint64  `json:"total_ram_bytes"`
	UsedRAMBytes        uint64  `json:"used_ram_bytes"`
	AverageRAMUsage     float64 `json:"average_ram_usage_percent"`
	TotalAvailableCores float64 `json:"total_available_cores"`
	TotalUsedCores      float64 `json:"total_used_cores"`
	AverageCPUUsage     float64 `json:"average_cpu_usage_percent"`
}

type SchemaChangeResult struct {
	IsChanged            bool
	BlueGreenSwapSuccess bool
	SchemaBefore         string
	VersionBefore        int
	VersionAfter         int
	NewCID               localdht.CID
}

type LedgerExecuteAction struct {
	From         interface{} `json:"from"` // Can be address (GCU) or tokenAddress (token)
	To           interface{} `json:"to"`
	Amount       interface{} `json:"amount"`
	TokenAddress string      `json:"tokenAddress,omitempty"` // If set, this is a token operation
}

type Storage interface {
	UpdateServiceRegistration(developerAddress, serviceName, url, subgraph, configCID, serviceType, servicePath string) (SchemaChangeResult, error)
	UpdateServiceInstances(developerAddress, service string, instances []router.ServiceInstance) error
	GetService(developerAddress, service, subgraph string) ([]router.ServiceInstance, error)
	GetAllServices() (map[string][]router.ServiceInstance, error)
	RemoveService(developerAddress, service string) error
	GetSchema(developerAddress, service string) (string, error)
	GetSchemaVersion(developerAddress, service string) (int, error)
	GetAllSchemas() (map[string]string, error)
	RemoveSchema(developerAddress, service string) error
	GetMiddlewarePipeline(developerAddress, workflowName string) (*PipelineConfig, error)
	GetAllMiddlewarePipelines() (map[string]map[string]PipelineConfig, error)
	GetAllPipelineKeys() ([]string, error)
	GetRestPipelineMap(developerAddress, service string) (RestPipelineMap, error)
	SetStitchingConfig(developerAddress, service string, config StitchingConfig) error
	GetAllStitchingConfigs() (map[string]StitchingConfig, error)
	BroadcastCurrentState()
	RequestNetworkState()
	GetLatestCID(developerAddress, serviceName string) (localdht.CID, bool)
	FetchVerifyAndCacheConfig(compositeKey string, expectedCID localdht.CID) (SchemaChangeResult, error)
	SubmitDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error)
	BroadcastDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error)
	GetNodeIdentity() *crypto.Identity
}

type RestPipelineMap map[string]*PipelineConfig

type RetryPolicy struct {
	Attempts     int `json:"attempts"`
	DelaySeconds int `json:"delaySeconds"`
}

type OnErrorConfig struct {
	Stop     *bool          `json:"stop,omitempty"`
	Message  string         `json:"message,omitempty"`
	Rollback []PipelineStep `json:"rollback,omitempty"`
}

type PipelineStep struct {
	Service       string               `json:"service,omitempty"`
	Field         string               `json:"field,omitempty"`
	Operation     string               `json:"operation,omitempty"`
	PassHeaders   []string             `json:"passHeaders,omitempty"`
	Selection     []string             `json:"selection,omitempty"`
	Assign        map[string]string    `json:"assign,omitempty"`
	OnError       *OnErrorConfig       `json:"onError,omitempty"`
	Concurrent    bool                 `json:"concurrent,omitempty"`
	ArgsMapping   map[string]string    `json:"argsMapping,omitempty"`
	Method        string               `json:"method,omitempty"`
	Path          string               `json:"path,omitempty"`
	BodyMapping   map[string]string    `json:"bodyMapping,omitempty"`
	PassContext   []string             `json:"passContext,omitempty"`
	CallWorkflow  string               `json:"callWorkflow,omitempty"`
	Conditional   string               `json:"conditional,omitempty"`
	RetryPolicy   *RetryPolicy         `json:"retryPolicy,omitempty"`
	LedgerExecute *LedgerExecuteAction `json:"ledgerExecute,omitempty"`
	TTL           string               `json:"ttl,omitempty"`
}

type PipelineConfig struct {
	IsWorkflow bool           `json:"isWorkflow,omitempty"`
	IsInternal bool           `json:"isInternal,omitempty"`
	IsDurable  bool           `json:"isDurable,omitempty"`
	Pre        []PipelineStep `json:"pre,omitempty"`
	Post       []PipelineStep `json:"post,omitempty"`
}

type ExecutionResult struct {
	Context      map[string]interface{}
	PostSteps    []PipelineStep
	Transactions []types.Transaction `json:"transactions,omitempty"`
}

type FieldStitch struct {
	Service       string            `json:"service"`
	ResolverField string            `json:"resolverField"`
	ArgsMapping   map[string]string `json:"argsMapping"`
}

type StitchingConfig map[string]map[string]FieldStitch

type GraphQLResponse struct {
	Data   map[string]interface{} `json:"data"`
	Errors []GraphQLError         `json:"errors"`
}
type GraphQLError struct {
	Message string `json:"message"`
}
