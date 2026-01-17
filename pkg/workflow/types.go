package workflow

import (
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging" // NEW: Import model
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Task represents a task announcement on the gossip network.
type Task struct {
	InstanceID    string  `json:"instanceId"`
	WorkflowName  string  `json:"workflowName"`
	CoordinatorID peer.ID `json:"coordinatorId"`
}

// State represents the current status of a workflow instance.
type State string

const (
	StateRunning   State = "RUNNING"
	StateFailed    State = "FAILED"
	StateCompleted State = "COMPLETED"
)

// Instance represents the complete state of a single, durable workflow execution.
// MODIFIED: Add a slice to track successful transactions for rollback.
type Instance struct {
	ID                 string                    `json:"id"`
	WorkflowName       string                    `json:"workflowName"`
	Status             State                     `json:"status"`
	Context            map[string]interface{}    `json:"context"`
	CurrentStep        int                       `json:"currentStep"`
	CurrentRetry       int                       `json:"currentRetry"`
	CreatedAt          time.Time                 `json:"createdAt"`
	UpdatedAt          time.Time                 `json:"updatedAt"`
	FailureReason      string                    `json:"failureReason,omitempty"`
	StepLogs           []logging.PipelineStepLog `json:"stepLogs,omitempty"`
	ParentTraceID      string                    `json:"parentTraceId,omitempty"`
	IsInternal         bool                      `json:"isInternal"`
	AssociatedService  string                    `json:"associatedService,omitempty"`
	OriginalHeaders    http.Header               `json:"originalHeaders,omitempty"`
	CoordinatorID      peer.ID                   `json:"coordinatorId"`
	CoordinatorAddress string                    `json:"coordinatorAddress,omitempty"`
	Version            int64                     `json:"version"` // Used for state recovery and preventing stale updates
	DeveloperID        string                    `json:"developerId,omitempty"`
	Transactions       []types.Transaction       `json:"transactions,omitempty"` // NEW: Tracks successful ledger transactions for this instance.
}

// --- P2P Worker-Coordinator Protocol Message Types ---

type ClaimRequest struct {
	InstanceID string  `json:"instanceId"`
	WorkerID   peer.ID `json:"workerId"`
}

type ClaimResponse struct {
	InstanceID  string    `json:"instanceId"`
	Locked      bool      `json:"locked"`
	Instance    *Instance `json:"instance,omitempty"`
	ExpectedTTL int       `json:"expectedTtl,omitempty"`
}

// MODIFIED: Add a field to report the details of an executed transaction and all step logs.
type UpdateReport struct {
	InstanceID          string                    `json:"instanceId"`
	Success             bool                      `json:"success"`
	Error               string                    `json:"error,omitempty"`
	ResultData          map[string]interface{}    `json:"resultData"`
	NextContext         map[string]interface{}    `json:"nextContext"`
	ExecutedTransaction *types.Transaction        `json:"executedTransaction,omitempty"` // NEW: Reports the successful transaction to the coordinator.
	StepLogs            []logging.PipelineStepLog `json:"stepLogs,omitempty"`            // NEW: Reports the full history of step logs.
}

// --- P2P Coordinator Recovery Protocol Message Types ---

// RecoveryProposal is broadcast by a node that detects an orphaned workflow.
type RecoveryProposal struct {
	InstanceData     []byte  `json:"instanceData"` // The full JSON of the last known Instance state
	NewCoordinatorID peer.ID `json:"newCoordinatorId"`
}
