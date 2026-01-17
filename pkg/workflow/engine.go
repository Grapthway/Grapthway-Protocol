package workflow

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"grapthway/pkg/dht"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/p2p"

	json "github.com/json-iterator/go"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	uuid "github.com/satori/go.uuid"
)

type Engine struct {
	storage          model.Storage
	p2pNode          *p2p.Node
	dht              *dht.DHT
	ctx              context.Context
	activeInstances  map[string]*Instance
	instanceLocks    map[string]bool
	mu               sync.Mutex
	unclaimedTasks   map[string]time.Time // Track unclaimed tasks
	unclaimedTasksMu sync.Mutex           // Mutex for the new map
}

func NewEngine(storage model.Storage, p2pNode *p2p.Node, dht *dht.DHT, recoveryChan <-chan *pubsub.Message) *Engine {
	engine := &Engine{
		storage:         storage,
		p2pNode:         p2pNode,
		dht:             dht,
		ctx:             context.Background(),
		activeInstances: make(map[string]*Instance),
		instanceLocks:   make(map[string]bool),
		unclaimedTasks:  make(map[string]time.Time), // Initialize map
	}
	engine.setupStreamHandler()
	engine.listenForRecoveryProposals(recoveryChan)
	go engine.reAnnounceLoop() // Start the re-announcer goroutine
	log.Println("WORKFLOW_ENGINE: Coordinator engine initialized.")
	return engine
}

// reAnnounceLoop periodically re-announces unclaimed tasks.
func (e *Engine) reAnnounceLoop() {
	// --- FIX START ---
	// Changed the ticker to a more frequent but controlled interval.
	ticker := time.NewTicker(2 * time.Second)
	// --- FIX END ---
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// --- DEADLOCK FIX START ---
			// The lock order is now mu -> unclaimedTasksMu, which is consistent
			// with the handleClaim function, preventing deadlocks.
			e.mu.Lock()
			e.unclaimedTasksMu.Lock()
			// --- DEADLOCK FIX END ---

			for instanceID, announcedAt := range e.unclaimedTasks {
				// --- FIX START ---
				// Only re-announce if it has been more than 3 seconds since the last announcement.
				// This gives workers a chance to claim the task without the coordinator spamming the network.
				if time.Since(announcedAt) < 3*time.Second {
					continue
				}
				// --- FIX END ---

				if instance, exists := e.activeInstances[instanceID]; exists {
					log.Printf("COORDINATOR-REANNOUNCE: Re-announcing task for unclaimed instance %s", instanceID)
					e.announceTask(instance)
					// --- FIX START ---
					// Update the announcement time to now to reset the cooldown.
					e.unclaimedTasks[instanceID] = time.Now()
					// --- FIX END ---
				} else {
					// The instance is no longer active, so it must have been completed/failed/claimed.
					// This is a failsafe cleanup.
					delete(e.unclaimedTasks, instanceID)
				}
			}

			// --- DEADLOCK FIX START ---
			e.unclaimedTasksMu.Unlock()
			e.mu.Unlock()
			// --- DEADLOCK FIX END ---
		}
	}
}

func (e *Engine) GetActiveInstances() []*Instance {
	e.mu.Lock()
	defer e.mu.Unlock()
	instances := make([]*Instance, 0, len(e.activeInstances))
	for _, instance := range e.activeInstances {
		instances = append(instances, instance)
	}
	return instances
}

func (e *Engine) setupStreamHandler() {
	e.p2pNode.Host.SetStreamHandler(p2p.WorkflowProtocolID, e.handleWorkflowStream)
}

func (e *Engine) listenForRecoveryProposals(recoveryChan <-chan *pubsub.Message) {
	log.Println("WORKFLOW_ENGINE: Listening to pre-subscribed recovery topic.")
	go func() {
		for msg := range recoveryChan {
			var proposal RecoveryProposal
			if err := json.Unmarshal(msg.Data, &proposal); err != nil {
				continue
			}
			go e.handleRecoveryProposal(proposal)
		}
	}()
}

func (e *Engine) StartWorkflow(workflowName string, initialContext map[string]interface{}, parentTraceID string, isInternal bool, originalHeaders http.Header, developerID string) (string, error) {
	instanceID := uuid.NewV4().String()
	log.Printf("COORDINATOR: Starting durable workflow '%s' for developer '%s' with instance ID: %s", workflowName, developerID, instanceID)
	if initialContext == nil {
		initialContext = make(map[string]interface{})
	}
	instance := &Instance{
		ID:                 instanceID,
		WorkflowName:       workflowName,
		Status:             StateRunning,
		Context:            initialContext,
		CurrentStep:        0,
		CurrentRetry:       0,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
		StepLogs:           make([]logging.PipelineStepLog, 0),
		ParentTraceID:      parentTraceID,
		IsInternal:         isInternal,
		OriginalHeaders:    originalHeaders,
		CoordinatorID:      e.p2pNode.Host.ID(),
		CoordinatorAddress: e.p2pNode.OwnerAddress,
		Version:            1,
		DeveloperID:        developerID,
	}
	e.mu.Lock()
	e.activeInstances[instanceID] = instance
	e.mu.Unlock()
	if err := e.checkpointState(instance); err != nil {
		log.Printf("COORDINATOR: CRITICAL: Failed initial checkpoint for instance %s: %v", instanceID, err)
	}

	// Add the task to the list of unclaimed tasks to be periodically announced.
	e.unclaimedTasksMu.Lock()
	e.unclaimedTasks[instanceID] = time.Now()
	e.unclaimedTasksMu.Unlock()

	// --- FIX START ---
	// Announce the task once immediately upon creation. The reAnnounceLoop will handle retries.
	e.announceTask(instance)
	// --- FIX END ---
	return instanceID, nil
}

func (e *Engine) handleWorkflowStream(stream network.Stream) {
	reader := bufio.NewReader(stream)
	jsonBytes, err := reader.ReadBytes('\n')
	if err != nil {
		stream.Reset()
		return
	}

	var claimReq ClaimRequest
	if err := json.Unmarshal(jsonBytes, &claimReq); err != nil {
		stream.Reset()
		return
	}
	e.handleClaim(stream, claimReq)
}

func (e *Engine) handleClaim(stream network.Stream, req ClaimRequest) {
	e.mu.Lock()
	instance, exists := e.activeInstances[req.InstanceID]
	if !exists {
		e.mu.Unlock()
		resp := ClaimResponse{InstanceID: req.InstanceID, Locked: false}
		json.NewEncoder(stream).Encode(resp)
		stream.Close()
		log.Printf("COORDINATOR: Instance %s not found in active instances", req.InstanceID)
		return
	}

	if instance.CoordinatorID != e.p2pNode.Host.ID() {
		e.mu.Unlock()
		resp := ClaimResponse{InstanceID: req.InstanceID, Locked: false}
		json.NewEncoder(stream).Encode(resp)
		stream.Close()
		log.Printf("COORDINATOR: Not the coordinator for instance %s (expected %s, got %s)",
			req.InstanceID, instance.CoordinatorID, e.p2pNode.Host.ID())
		return
	}

	if locked, _ := e.instanceLocks[req.InstanceID]; locked {
		e.mu.Unlock()
		resp := ClaimResponse{InstanceID: req.InstanceID, Locked: false}
		json.NewEncoder(stream).Encode(resp)
		stream.Close()
		return
	}

	e.instanceLocks[req.InstanceID] = true

	// Once a worker claims the task, remove it from the unclaimed list.
	e.unclaimedTasksMu.Lock()
	delete(e.unclaimedTasks, req.InstanceID)
	e.unclaimedTasksMu.Unlock()

	// ✅ NEW: Calculate expected TTL from current step
	expectedTTL := e.calculateStepTTL(instance)

	e.mu.Unlock()

	resp := ClaimResponse{
		InstanceID:  req.InstanceID,
		Locked:      true,
		Instance:    instance,
		ExpectedTTL: expectedTTL, // ✅ Pass TTL to worker
	}

	if err := json.NewEncoder(stream).Encode(resp); err != nil {
		e.releaseLock(req.InstanceID)
		stream.Reset()
		return
	}

	// ✅ Pass expectedTTL to waitForUpdate
	e.waitForUpdate(stream, req.WorkerID, expectedTTL)
}

func (e *Engine) calculateStepTTL(instance *Instance) int {
	// Default timeout: 2 minutes
	defaultTTL := 120

	// Get workflow configuration
	workflowConfig, err := e.storage.GetMiddlewarePipeline(instance.DeveloperID, instance.WorkflowName)
	if err != nil || workflowConfig == nil {
		return defaultTTL
	}

	// Get all steps (pre + post)
	allSteps := append(workflowConfig.Pre, workflowConfig.Post...)

	// Check if current step index is valid
	if instance.CurrentStep >= len(allSteps) {
		return defaultTTL
	}

	// Get the current step
	step := allSteps[instance.CurrentStep]

	// Parse TTL if specified
	if step.TTL != "" {
		if duration, err := time.ParseDuration(step.TTL); err == nil {
			ttlSeconds := int(duration.Seconds())
			if ttlSeconds > 0 {
				log.Printf("COORDINATOR: Step %d has TTL: %s (%d seconds)",
					instance.CurrentStep, step.TTL, ttlSeconds)
				return ttlSeconds
			}
		} else {
			log.Printf("COORDINATOR: Failed to parse TTL '%s' for step %d: %v. Using default.",
				step.TTL, instance.CurrentStep, err)
		}
	}

	return defaultTTL
}

func (e *Engine) waitForUpdate(stream network.Stream, workerID peer.ID, expectedTTL int) {
	var report UpdateReport

	// ✅ Calculate timeout: step TTL + 60s buffer (minimum 2 minutes)
	timeoutSeconds := expectedTTL + 60 // Add 1 minute buffer for network overhead
	if timeoutSeconds < 120 {
		timeoutSeconds = 120 // Minimum 2 minutes
	}
	timeout := time.Duration(timeoutSeconds) * time.Second

	log.Printf("COORDINATOR: Waiting for worker report with timeout: %v (step TTL: %ds + 60s buffer)",
		timeout, expectedTTL)

	// Set dynamic deadline based on step's TTL
	stream.SetReadDeadline(time.Now().Add(timeout))

	if err := json.NewDecoder(stream).Decode(&report); err != nil {
		log.Printf("COORDINATOR: Worker %s failed to send update for %s: %v. Re-announcing task.",
			workerID, report.InstanceID, err)
		e.releaseLock(report.InstanceID)

		// If the worker disconnects or fails to report, put the task back in the unclaimed queue.
		e.unclaimedTasksMu.Lock()
		e.unclaimedTasks[report.InstanceID] = time.Time{} // Set to zero time to trigger immediate re-announce
		e.unclaimedTasksMu.Unlock()

		stream.Reset()
		return
	}

	defer stream.Close()
	e.processUpdate(report)
}

func (e *Engine) processUpdate(report UpdateReport) {
	defer e.releaseLock(report.InstanceID)

	e.mu.Lock()
	instance, exists := e.activeInstances[report.InstanceID]
	if !exists {
		e.mu.Unlock()
		return
	}

	workflowConfig, err := e.storage.GetMiddlewarePipeline(instance.DeveloperID, instance.WorkflowName)
	if err != nil || workflowConfig == nil {
		instance.Status = StateFailed
		instance.FailureReason = fmt.Sprintf("Workflow definition '%s' not found for developer %s.", instance.WorkflowName, instance.DeveloperID)
		delete(e.activeInstances, instance.ID)
		e.mu.Unlock() // Unlock before checkpoint
		e.checkpointState(instance)
		return
	}
	allSteps := append(workflowConfig.Pre, workflowConfig.Post...)
	step := allSteps[instance.CurrentStep]
	instance.Context = report.NextContext
	// --- FIX START: Update the instance's step logs with the complete history from the worker. ---
	if report.StepLogs != nil {
		instance.StepLogs = report.StepLogs
	}
	// --- FIX END ---

	if report.Success && report.ExecutedTransaction != nil {
		instance.Transactions = append(instance.Transactions, *report.ExecutedTransaction)
	}

	if !report.Success {
		maxRetries := 0
		if step.RetryPolicy != nil {
			maxRetries = step.RetryPolicy.Attempts
		}
		if instance.CurrentRetry < maxRetries {
			instance.CurrentRetry++
			e.unclaimedTasksMu.Lock()
			e.unclaimedTasks[instance.ID] = time.Time{} // Zero time for immediate re-announce
			e.unclaimedTasksMu.Unlock()
		} else {
			instance.Status = StateFailed
			instance.FailureReason = fmt.Sprintf("Step %d ('%s') failed after %d retries: %s", instance.CurrentStep, step.Field, maxRetries, report.Error)
			go e.initiateWorkflowRollback(instance)
			delete(e.activeInstances, instance.ID)
			e.unclaimedTasksMu.Lock()
			delete(e.unclaimedTasks, instance.ID)
			e.unclaimedTasksMu.Unlock()
		}
	} else {
		instance.CurrentStep++
		instance.CurrentRetry = 0
		if instance.CurrentStep >= len(allSteps) {
			instance.Status = StateCompleted
			delete(e.activeInstances, instance.ID)
			e.unclaimedTasksMu.Lock()
			delete(e.unclaimedTasks, instance.ID)
			e.unclaimedTasksMu.Unlock()
		} else {
			e.unclaimedTasksMu.Lock()
			e.unclaimedTasks[instance.ID] = time.Time{} // Zero time for immediate announce
			e.unclaimedTasksMu.Unlock()
		}
	}

	instance.Version++
	instance.UpdatedAt = time.Now().UTC()
	instanceToCheckpoint := *instance
	e.mu.Unlock()
	e.checkpointState(&instanceToCheckpoint)
}

// --- FIX START: Implemented full compensating transaction logic for durable workflows ---
func (e *Engine) initiateWorkflowRollback(instance *Instance) {
	if len(instance.Transactions) == 0 {
		return
	}

	log.Printf("COORDINATOR: Initiating rollback of %d transactions for failed instance %s.", len(instance.Transactions), instance.ID)

	for _, tx := range instance.Transactions {
		if tx.Type != model.DelegatedTransferTransaction {
			continue
		}

		// Create a debit transaction to take the money back from the original recipient.
		// This is a system-level transaction that bypasses allowance checks.
		rollbackDebitTx := types.Transaction{
			ID:        fmt.Sprintf("tx-rollback-debit-%s", tx.ID),
			Type:      model.RollbackDebitTransaction,
			From:      tx.To, // Debit the original recipient
			Amount:    tx.Amount,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		// Create a credit transaction to give the money back to the original sender.
		// This is also a system-level transaction.
		rollbackCreditTx := types.Transaction{
			ID:        fmt.Sprintf("tx-rollback-credit-%s", tx.ID),
			Type:      model.RollbackCreditTransaction,
			To:        tx.From, // Credit the original sender
			Amount:    tx.Amount,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		// Submit both system transactions to the ledger.
		if _, err := e.storage.BroadcastDelegatedTransaction(context.Background(), rollbackDebitTx); err != nil {
			log.Printf("COORDINATOR: CRITICAL_ROLLBACK_FAILURE for instance %s: Failed to submit rollback debit for tx %s: %v", instance.ID, tx.ID, err)
		} else {
			log.Printf("COORDINATOR: Submitted rollback debit for tx %s successfully.", tx.ID)
		}

		if _, err := e.storage.BroadcastDelegatedTransaction(context.Background(), rollbackCreditTx); err != nil {
			log.Printf("COORDINATOR: CRITICAL_ROLLBACK_FAILURE for instance %s: Failed to submit rollback credit for tx %s: %v", instance.ID, tx.ID, err)
		} else {
			log.Printf("COORDINATOR: Submitted rollback credit for tx %s successfully.", tx.ID)
		}
	}
}

// --- FIX END ---

func (e *Engine) handleRecoveryProposal(proposal RecoveryProposal) {
	var proposedInstance Instance
	if err := json.Unmarshal(proposal.InstanceData, &proposedInstance); err != nil {
		return
	}

	e.mu.Lock()
	if _, exists := e.activeInstances[proposedInstance.ID]; exists {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	isOffline := true
	connectedPeers := e.p2pNode.GetPeerList()
	for _, p := range connectedPeers {
		if p == proposedInstance.CoordinatorID {
			isOffline = false
			break
		}
	}

	if !isOffline {
		return
	}

	e.mu.Lock()
	if _, exists := e.activeInstances[proposedInstance.ID]; exists {
		e.mu.Unlock()
		return
	}

	proposedInstance.CoordinatorID = e.p2pNode.Host.ID()
	proposedInstance.CoordinatorAddress = e.p2pNode.OwnerAddress
	proposedInstance.Version++
	proposedInstance.UpdatedAt = time.Now().UTC()
	e.activeInstances[proposedInstance.ID] = &proposedInstance
	e.mu.Unlock()

	e.checkpointState(&proposedInstance)
	e.announceTask(&proposedInstance)
	log.Printf("RECOVERY: Successfully took over coordination for instance %s.", proposedInstance.ID)
}

func (e *Engine) checkpointState(instance *Instance) error {
	instanceBytes, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("failed to marshal instance for checkpoint: %w", err)
	}
	key := "workflow:instance:" + instance.ID
	_, err = e.dht.Put(key, instanceBytes)
	if err != nil {
		return fmt.Errorf("failed to put instance checkpoint into DHT: %w", err)
	}
	return nil
}

func (e *Engine) announceTask(instance *Instance) {
	task := Task{
		InstanceID:    instance.ID,
		WorkflowName:  instance.WorkflowName,
		CoordinatorID: instance.CoordinatorID,
	}
	taskPayload, _ := json.Marshal(task)
	if err := e.p2pNode.Gossip(DurableTaskTopic, taskPayload); err != nil {
		log.Printf("COORDINATOR: CRITICAL: Failed to gossip task for instance %s: %v", instance.ID, err)
	}
}

func (e *Engine) releaseLock(instanceID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.instanceLocks, instanceID)
}
