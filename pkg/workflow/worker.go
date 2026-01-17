package workflow

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"grapthway/pkg/common"
	"grapthway/pkg/crypto"
	"grapthway/pkg/dht"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/p2p"
	"grapthway/pkg/router"
	"grapthway/pkg/util"

	"github.com/Knetic/govaluate"
	json "github.com/json-iterator/go"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	DurableTaskTopic             = "grapthway-durable-tasks"
	internalGrapthwayServiceName = "grapthway-internal-api"
	systemDeveloperAddress       = "grapthway-system" // Placeholder for internal services
	ConfigUpdateTopic            = "grapthway-config-updates"
)

type Worker struct {
	storage      model.Storage
	logger       *logging.Logger
	node         *p2p.Node
	dht          *dht.DHT
	nodeIdentity *crypto.Identity
	stopChan     chan struct{}
	ctx          context.Context
	taskChan     <-chan *pubsub.Message
}

func NewWorker(ctx context.Context, storage model.Storage, logger *logging.Logger, node *p2p.Node, dht *dht.DHT, identity *crypto.Identity, taskChan <-chan *pubsub.Message) *Worker {
	return &Worker{
		storage:      storage,
		logger:       logger,
		node:         node,
		dht:          dht,
		nodeIdentity: identity,
		stopChan:     make(chan struct{}),
		ctx:          ctx,
		taskChan:     taskChan,
	}
}

func (w *Worker) Start() {
	log.Println("WORKER: Starting durable workflow worker...")

	go func() {
		time.Sleep(5 * time.Second)
		log.Println("WORKER: Requesting initial network state...")
		w.storage.RequestNetworkState()
	}()

	for {
		select {
		case <-w.stopChan:
			log.Println("WORKER: Shutting down.")
			return
		case msg := <-w.taskChan:
			var task Task
			if err := json.Unmarshal(msg.Data, &task); err != nil {
				log.Printf("WORKER: Error unmarshalling task announcement: %v", err)
				continue
			}
			log.Printf("WORKER: Heard announcement for task on instance ID: %s. Coordinator: %s", task.InstanceID, task.CoordinatorID)
			go w.claimAndProcessTask(task)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stopChan)
}

func (w *Worker) claimAndProcessTask(task Task) {
	log.Printf("WORKER: Attempting to claim task for instance %s from coordinator %s", task.InstanceID, task.CoordinatorID)

	stream, err := w.node.Host.NewStream(w.ctx, task.CoordinatorID, p2p.WorkflowProtocolID)
	if err != nil {
		log.Printf("WORKER: Could not open stream to coordinator %s: %v. INITIATING RECOVERY.", task.CoordinatorID, err)
		w.initiateRecovery(task.InstanceID)
		return
	}
	defer stream.Close()

	claimReq := ClaimRequest{InstanceID: task.InstanceID, WorkerID: w.node.Host.ID()}
	writer := bufio.NewWriter(stream)
	reqBytes, _ := json.Marshal(claimReq)
	if _, err := writer.Write(append(reqBytes, '\n')); err != nil {
		log.Printf("WORKER: Failed to send claim request for %s: %v", task.InstanceID, err)
		return
	}
	writer.Flush()

	var claimResp ClaimResponse
	if err := json.NewDecoder(stream).Decode(&claimResp); err != nil {
		log.Printf("WORKER: Failed to read claim response for %s: %v", task.InstanceID, err)
		return
	}

	if !claimResp.Locked || claimResp.Instance == nil {
		log.Printf("WORKER: Lost race for instance %s. Another worker won.", task.InstanceID)
		return
	}

	log.Printf("WORKER: Acquired lock for instance %s. Processing step.", task.InstanceID)
	stepResult, executedTx, stepErr := w.ProcessDurableStep(claimResp.Instance)

	report := UpdateReport{
		InstanceID:          task.InstanceID,
		Success:             stepErr == nil,
		ResultData:          stepResult,
		NextContext:         claimResp.Instance.Context,
		ExecutedTransaction: executedTx,
		StepLogs:            claimResp.Instance.StepLogs,
	}
	if stepErr != nil {
		report.Error = stepErr.Error()
	}

	if err := json.NewEncoder(stream).Encode(report); err != nil {
		log.Printf("WORKER: CRITICAL: Failed to send update report for %s to coordinator: %v", task.InstanceID, err)
	} else {
		log.Printf("WORKER: Successfully reported result for instance %s to coordinator.", task.InstanceID)
	}
}

func (w *Worker) initiateRecovery(instanceID string) {
	key := "workflow:instance:" + instanceID
	instanceBytes, err := w.dht.Get(key)
	if err != nil {
		log.Printf("RECOVERY: Coordinator for %s is offline, but no checkpoint found in DHT. Workflow may be lost.", instanceID)
		return
	}

	log.Printf("RECOVERY: Found checkpoint for orphaned instance %s. Proposing recovery.", instanceID)

	proposal := RecoveryProposal{
		InstanceData:     instanceBytes,
		NewCoordinatorID: w.node.Host.ID(),
	}

	proposalBytes, err := json.Marshal(proposal)
	if err != nil {
		return
	}

	if err := w.node.Gossip(p2p.RecoveryProposalTopic, proposalBytes); err != nil {
		log.Printf("RECOVERY: Failed to gossip recovery proposal for instance %s: %v", instanceID, err)
	}
}

func (w *Worker) ProcessDurableStep(instance *Instance) (map[string]interface{}, *types.Transaction, error) {
	logEntry := &logging.LogEntry{
		Timestamp:          time.Now(),
		LogType:            logging.GatewayLog,
		TraceID:            instance.ParentTraceID,
		RequestAddress:     "durable-worker",
		Subgraph:           instance.AssociatedService,
		WorkflowInstanceID: instance.ID,
	}
	defer func() {
		if r := recover(); r != nil {
			instance.Status = StateFailed
			instance.FailureReason = fmt.Sprintf("Worker panic recovered: %v", r)
			logEntry.FailureMessage = instance.FailureReason
		}
		logEntry.ClientResponse.StatusCode = 200
		w.logger.Log(*logEntry)
	}()

	workflowConfig, err := w.storage.GetMiddlewarePipeline(instance.DeveloperID, instance.WorkflowName)
	if err != nil || workflowConfig == nil {
		return nil, nil, fmt.Errorf("workflow definition '%s' not found for developer %s", instance.WorkflowName, instance.DeveloperID)
	}

	allSteps := append(workflowConfig.Pre, workflowConfig.Post...)
	if instance.CurrentStep >= len(allSteps) {
		return nil, nil, fmt.Errorf("workflow instance %s is already completed", instance.ID)
	}
	step := allSteps[instance.CurrentStep]

	shouldExecute, err := w.evaluateConditional(step, instance.Context)
	if err != nil {
		stepLog := toPipelineStepLog(step)
		stepLog.Error = fmt.Sprintf("Conditional evaluation failed: %v", err)
		instance.StepLogs = append(instance.StepLogs, stepLog)
		logEntry.PreFailureSteps = append(logEntry.PreFailureSteps, stepLog)
		return nil, nil, fmt.Errorf("step %d: Failed to evaluate conditional '%s': %v", instance.CurrentStep, step.Conditional, err)
	}
	if !shouldExecute {
		log.Printf("WORKER: Instance %s, Step %d: Conditional false, skipping.", instance.ID, instance.CurrentStep)
		stepLog := toPipelineStepLog(step)
		instance.StepLogs = append(instance.StepLogs, stepLog)
		logEntry.PreSuccessSteps = append(logEntry.PreSuccessSteps, stepLog) // Skipped is a form of success
		return nil, nil, nil
	}

	stepLog := toPipelineStepLog(step)
	var stepResult map[string]interface{}
	var stepErr error
	var executedTx *types.Transaction

	if step.LedgerExecute != nil {
		stepResult, executedTx, stepErr = w.executeLedgerStep(instance, step, &stepLog)
	} else if step.CallWorkflow != "" {
		stepResult, stepErr = w.executeNestedWorkflowStep(instance, step, &stepLog)
	} else if step.Service != "" && step.Path != "" {
		stepResult, stepErr = w.executeRESTStep(instance, step, &stepLog)
	} else if step.Service != "" && step.Field != "" {
		stepResult, stepErr = w.executeGraphQLStep(instance, step, &stepLog)
	} else {
		stepErr = fmt.Errorf("invalid pipeline step configuration: missing service/field or service/path")
	}

	if stepErr != nil {
		stepLog.Error = stepErr.Error()
		logEntry.PreFailureSteps = append(logEntry.PreFailureSteps, stepLog)
	} else {
		logEntry.PreSuccessSteps = append(logEntry.PreSuccessSteps, stepLog)
	}
	instance.StepLogs = append(instance.StepLogs, stepLog)

	if stepErr == nil && stepResult != nil {
		if step.CallWorkflow != "" {
			for k, v := range stepResult {
				instance.Context[k] = v
			}
		} else if step.Assign != nil {
			dataToAssign := stepResult
			if step.Field != "" {
				if fieldData, ok := stepResult[step.Field]; ok {
					dataToAssign = map[string]interface{}{step.Field: fieldData}
				}
			}

			// ✅ FIX: Handle both root-level and nested responses
			for ctxKey, resKey := range step.Assign {
				if resKey == "*" {
					// Assign the entire response or field
					if step.Field != "" {
						instance.Context[ctxKey] = dataToAssign[step.Field]
					} else {
						instance.Context[ctxKey] = dataToAssign
					}
				} else {
					// ✅ NEW: Try to get from root level first
					if val, ok := stepResult[resKey]; ok {
						instance.Context[ctxKey] = val
					} else if step.Field != "" {
						// Fallback: try nested access for GraphQL responses
						if result, ok := dataToAssign[step.Field].(map[string]interface{}); ok {
							if val, ok := result[resKey]; ok {
								instance.Context[ctxKey] = val
							}
						}
					}
				}
			}
		}
	}

	return stepResult, executedTx, stepErr
}

func (w *Worker) executeLedgerStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, *types.Transaction, error) {
	action := step.LedgerExecute

	// Check if this is a token operation
	if action.TokenAddress != "" {
		return w.executeTokenLedgerStep(instance, action, stepLog)
	}

	fromValue, fromInContext := getValueFromContext(instance.Context, action.From.(string))
	var fromAddress string
	if fromInContext {
		var ok bool
		fromAddress, ok = fromValue.(string)
		if !ok {
			return nil, nil, fmt.Errorf("ledgerExecute failed: context value for 'from' key '%s' is not a string address", action.From)
		}
	} else {
		fromAddress = action.From.(string)
	}

	toValue, toInContext := getValueFromContext(instance.Context, action.To.(string))
	var toAddress string
	if toInContext {
		var ok bool
		toAddress, ok = toValue.(string)
		if !ok {
			return nil, nil, fmt.Errorf("ledgerExecute failed: context value for 'to' key '%s' is not a string address", action.To)
		}
	} else {
		toAddress = action.To.(string)
	}

	var amountMicro uint64
	switch v := action.Amount.(type) {
	case string:
		amountValue, ok := getValueFromContext(instance.Context, v)
		if !ok {
			parsedFloat, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("ledgerExecute failed: 'amount' string '%s' not in context and not a valid number", v)
			}
			amountValue = parsedFloat
		}
		floatAmount, err := util.ToFloat64(amountValue)
		if err != nil {
			return nil, nil, fmt.Errorf("ledgerExecute failed: context value for 'amount' key '%s' is not a valid number: %w", v, err)
		}
		amountMicro = uint64(floatAmount)
	case float64:
		amountMicro = uint64(v)
	case int:
		amountMicro = uint64(v)
	case json.Number:
		floatAmount, err := v.Float64()
		if err != nil {
			return nil, nil, fmt.Errorf("ledgerExecute failed: invalid json number for amount")
		}
		amountMicro = uint64(floatAmount)
	default:
		return nil, nil, fmt.Errorf("ledgerExecute failed: 'amount' has an invalid type %T", v)
	}

	reqBodyMap := map[string]interface{}{
		"from":   fromAddress,
		"to":     toAddress,
		"amount": amountMicro,
	}
	reqBodyBytes, _ := json.Marshal(reqBodyMap)
	stepLog.Request = logging.RequestDetails{
		Method: "INTERNAL_CALL",
		URL:    "ledger.delegatedTransfer",
		Body:   string(reqBodyBytes),
	}

	// Use the DeveloperID from the instance as the spender
	// This is the service owner who created the workflow
	if instance.DeveloperID == "" {
		return nil, nil, fmt.Errorf("cannot execute ledger step: workflow instance is missing developer ID (service owner)")
	}

	tx := types.Transaction{
		ID:        fmt.Sprintf("tx-delegated-%d", time.Now().UnixNano()),
		Type:      model.DelegatedTransferTransaction,
		From:      fromAddress,
		To:        toAddress,
		Amount:    amountMicro,
		Spender:   instance.DeveloperID, // Service owner (developer) is the spender
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	processedTx, err := w.storage.BroadcastDelegatedTransaction(w.ctx, tx)
	if err != nil {
		stepLog.Response.StatusCode = 500
		stepLog.Response.Body = err.Error()
		return nil, nil, fmt.Errorf("ledgerExecute transaction failed: %w", err)
	}

	respBodyMap := map[string]interface{}{"transactionId": processedTx.ID}
	respBodyBytes, _ := json.Marshal(respBodyMap)
	stepLog.Response.StatusCode = 200
	stepLog.Response.Body = string(respBodyBytes)

	log.Printf("WORKER: Instance %s executed delegated transfer %s (spender: %s)", instance.ID, processedTx.ID, instance.DeveloperID)
	return nil, processedTx, nil
}

// executeTokenLedgerStep handles token transfer operations in durable workflows
func (w *Worker) executeTokenLedgerStep(instance *Instance, action *model.LedgerExecuteAction, stepLog *logging.PipelineStepLog) (map[string]interface{}, *types.Transaction, error) {
	// Resolve token address
	tokenAddress := action.TokenAddress

	// Resolve from address
	fromValue, fromInContext := getValueFromContext(instance.Context, action.From.(string))
	var fromAddress string
	if fromInContext {
		var ok bool
		fromAddress, ok = fromValue.(string)
		if !ok {
			return nil, nil, fmt.Errorf("tokenLedgerExecute failed: context value for from key '%s' is not a string address", action.From)
		}
	} else {
		fromAddress = action.From.(string)
	}

	// Resolve to address
	toValue, toInContext := getValueFromContext(instance.Context, action.To.(string))
	var toAddress string
	if toInContext {
		var ok bool
		toAddress, ok = toValue.(string)
		if !ok {
			return nil, nil, fmt.Errorf("tokenLedgerExecute failed: context value for to key '%s' is not a string address", action.To)
		}
	} else {
		toAddress = action.To.(string)
	}

	// Resolve amount (already in token's micro units)
	var amountMicro uint64
	switch v := action.Amount.(type) {
	case string:
		amountValue, ok := getValueFromContext(instance.Context, v)
		if !ok {
			parsedFloat, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("tokenLedgerExecute failed: amount string '%s' not in context and not a valid number", v)
			}
			amountValue = parsedFloat
		}
		floatAmount, err := util.ToFloat64(amountValue)
		if err != nil {
			return nil, nil, fmt.Errorf("tokenLedgerExecute failed: context value for amount key '%s' is not a valid number: %w", v, err)
		}
		amountMicro = uint64(floatAmount)
	case float64:
		amountMicro = uint64(v)
	case int:
		amountMicro = uint64(v)
	case json.Number:
		floatAmount, err := v.Float64()
		if err != nil {
			return nil, nil, fmt.Errorf("tokenLedgerExecute failed: invalid json number for amount")
		}
		amountMicro = uint64(floatAmount)
	default:
		return nil, nil, fmt.Errorf("tokenLedgerExecute failed: amount has an invalid type %T", v)
	}

	// Build request body for logging
	reqBodyMap := map[string]interface{}{
		"from":         fromAddress,
		"to":           toAddress,
		"amount":       amountMicro,
		"tokenAddress": tokenAddress,
	}
	reqBodyBytes, _ := json.Marshal(reqBodyMap)
	stepLog.Request = logging.RequestDetails{
		Method: "INTERNAL_CALL",
		URL:    "token.transfer",
		Body:   string(reqBodyBytes),
	}

	// Use the DeveloperID from the instance as the spender
	// This is the service owner who created the workflow
	if instance.DeveloperID == "" {
		return nil, nil, fmt.Errorf("cannot execute token ledger step: workflow instance is missing developer ID (service owner)")
	}

	// Create token transfer transaction
	tx := types.Transaction{
		ID:           fmt.Sprintf("tx-token-transfer-workflow-%d", time.Now().UnixNano()),
		Type:         model.TokenTransferTransaction,
		From:         fromAddress,
		To:           toAddress,
		Amount:       amountMicro,
		TokenAddress: tokenAddress,
		Spender:      instance.DeveloperID, // Service owner/developer is the spender
		Timestamp:    time.Now(),
		CreatedAt:    time.Now(),
	}

	// Submit through ledger
	processedTx, err := w.storage.BroadcastDelegatedTransaction(w.ctx, tx)
	if err != nil {
		stepLog.Response.StatusCode = 500
		stepLog.Response.Body = err.Error()
		return nil, nil, fmt.Errorf("tokenLedgerExecute transaction failed: %w", err)
	}

	// Build response
	respBodyMap := map[string]interface{}{
		"transactionId": processedTx.ID,
	}
	respBodyBytes, _ := json.Marshal(respBodyMap)
	stepLog.Response.StatusCode = 200
	stepLog.Response.Body = string(respBodyBytes)

	log.Printf("WORKER: Instance %s executed token transfer %s (token: %s, spender: %s)",
		instance.ID, processedTx.ID, tokenAddress, instance.DeveloperID)

	return nil, processedTx, nil
}

func (w *Worker) evaluateConditional(step model.PipelineStep, context map[string]interface{}) (bool, error) {
	if step.Conditional == "" {
		return true, nil
	}
	expression, err := govaluate.NewEvaluableExpression(step.Conditional)
	if err != nil {
		return false, err
	}
	result, err := expression.Evaluate(context)
	if err != nil {
		return false, err
	}
	boolResult, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("conditional did not return a boolean value")
	}
	return boolResult, nil
}

func (w *Worker) executeNestedWorkflowStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	// CRITICAL: Use the workflow instance's DeveloperID for nested workflow lookup
	developerID := instance.DeveloperID
	if developerID == "" {
		return nil, fmt.Errorf("workflow instance missing developer ID, cannot resolve nested workflow")
	}

	log.Printf("WORKER: Instance %s looking up nested workflow '%s' for developer '%s'", instance.ID, step.CallWorkflow, developerID)

	targetWorkflow, err := w.storage.GetMiddlewarePipeline(developerID, step.CallWorkflow)
	if err != nil || targetWorkflow == nil {
		return nil, fmt.Errorf("nested workflow '%s' not found for developer %s: %w", step.CallWorkflow, developerID, err)
	}

	if targetWorkflow.IsDurable {
		log.Printf("WORKER: Instance %s is requesting start of a nested durable workflow '%s'", instance.ID, step.CallWorkflow)
		servers, err := w.storage.GetService(systemDeveloperAddress, internalGrapthwayServiceName, "internal")
		if err != nil || len(servers) == 0 {
			return nil, fmt.Errorf("no available gateway nodes to start nested durable workflow '%s'", step.CallWorkflow)
		}
		return nil, fmt.Errorf("starting a nested durable workflow from a worker is not fully implemented")
	}

	log.Printf("WORKER: Instance %s is delegating nested in-memory workflow '%s' to another node.", instance.ID, step.CallWorkflow)

	var servers []router.ServiceInstance
	var discoveryErr error
	for i := 0; i < 5; i++ {
		servers, discoveryErr = w.storage.GetService(systemDeveloperAddress, internalGrapthwayServiceName, "internal")
		if discoveryErr == nil && len(servers) > 0 {
			break
		}
		servers, discoveryErr = w.storage.GetService(systemDeveloperAddress, internalGrapthwayServiceName, "")
		if discoveryErr == nil && len(servers) > 0 {
			break
		}
		log.Printf("WORKER: Could not find gateway node for workflow delegation (attempt %d/5). Retrying in 2 seconds...", i+1)
		time.Sleep(2 * time.Second)
	}

	if discoveryErr != nil || len(servers) == 0 {
		return nil, fmt.Errorf("no available gateway nodes to execute nested workflow '%s' after multiple retries", step.CallWorkflow)
	}

	serverInstance := router.NewServiceRouter().PickInstance(servers)

	payload := map[string]interface{}{
		"workflowName":     step.CallWorkflow,
		"developerAddress": developerID, // Use the workflow instance's developer ID
		"context":          instance.Context,
		"traceId":          instance.ParentTraceID,
		"headers":          instance.OriginalHeaders,
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", serverInstance.URL+"/admin/internal/execute-workflow", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create internal request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Grapthway-Node-Address", w.nodeIdentity.Address)
	digest := sha256.Sum256(payloadBytes)
	sig, err := w.nodeIdentity.Sign(digest[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign internal request: %w", err)
	}
	req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(sig))

	stepLog.Request = logging.RequestDetails{Method: "POST", URL: req.URL.String(), Headers: req.Header.Clone(), Body: string(payloadBytes)}

	resp, err := common.GetHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{StatusCode: resp.StatusCode, Headers: resp.Header.Clone(), Body: string(respBody)}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("internal workflow execution failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result model.ExecutionResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal internal workflow response: %w", err)
	}

	return result.Context, nil
}

func (w *Worker) createRequestSignature(body []byte) ([]byte, error) {
	digest := sha256.Sum256(body)
	return w.nodeIdentity.Sign(digest[:])
}

func (w *Worker) executeRESTStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	developerAddress, serviceName, err := util.ParseCompositeKey(step.Service)
	if err != nil {
		return nil, fmt.Errorf("invalid service format in pipeline step: %s", step.Service)
	}
	instances, err := w.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(instances) == 0 {
		return nil, fmt.Errorf("service '%s' unavailable", step.Service)
	}
	serviceInstance := router.NewServiceRouter().PickInstance(instances)
	fullURL := serviceInstance.URL + step.Path

	var bodyBytes []byte
	var reqBodyString string

	// ✅ CRITICAL: For GET requests, convert BodyMapping to query parameters
	if strings.ToUpper(step.Method) == "GET" {
		if len(step.BodyMapping) > 0 {
			parsedURL, parseErr := url.Parse(fullURL)
			if parseErr == nil {
				q := parsedURL.Query()

				for paramKey, contextKey := range step.BodyMapping {
					if val, ok := getValueFromContext(instance.Context, contextKey); ok {
						switch v := val.(type) {
						case string:
							q.Set(paramKey, v)
						case []interface{}:
							for _, item := range v {
								q.Add(paramKey, fmt.Sprintf("%v", item))
							}
						default:
							q.Set(paramKey, fmt.Sprintf("%v", val))
						}
					} else {
						// Use as static value if not found in context
						q.Set(paramKey, contextKey)
					}
				}

				parsedURL.RawQuery = q.Encode()
				fullURL = parsedURL.String()
			}
		}

		// GET requests have no body
		bodyBytes = []byte{}
		reqBodyString = ""
	} else {
		// For POST/PUT/etc, build JSON body as normal
		reqBodyReader, reqBodyStr, err := buildRequestBodyFromContext(step.BodyMapping, instance.Context)
		if err != nil {
			return nil, fmt.Errorf("failed to build request body: %w", err)
		}
		reqBodyString = reqBodyStr

		if reqBodyReader != nil {
			bodyBytes, err = io.ReadAll(reqBodyReader)
			if err != nil {
				return nil, fmt.Errorf("failed to read request body for signing: %w", err)
			}
		}
	}

	req, err := http.NewRequest(step.Method, fullURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header = preparePipelineHeaders(instance.OriginalHeaders, step, instance.Context)

	signature, err := w.createRequestSignature(bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign REST request: %w", err)
	}
	req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	req.Header.Set("X-Grapthway-Node-Address", w.nodeIdentity.Address)
	req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(w.nodeIdentity.PublicKey)))

	stepLog.Request = logging.RequestDetails{Method: req.Method, URL: req.URL.String(), Headers: req.Header.Clone(), Body: reqBodyString}

	timeout := common.ParseTimeout(step.TTL, 60*time.Second)
	client := common.GetHTTPClientWithTimeout(timeout)
	log.Printf("🕐 WORKER REST %s %s with timeout: %v (Instance: %s)", step.Method, fullURL, timeout, instance.ID)

	resp, err := client.Do(req)
	if err != nil {
		router := router.NewServiceRouter()
		router.ReportConnectionFailure(serviceInstance.URL)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{StatusCode: resp.StatusCode, Headers: resp.Header.Clone(), Body: string(respBody)}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("service returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if len(respBody) > 0 && strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal REST response from %s: %w", fullURL, err)
		}
		return result, nil
	}

	// ✅ Fallback for non-JSON or empty responses
	return map[string]interface{}{}, nil
}

func (w *Worker) executeGraphQLStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	developerAddress, serviceName, err := util.ParseCompositeKey(step.Service)
	if err != nil {
		return nil, fmt.Errorf("invalid service format in pipeline step: %s", step.Service)
	}
	instances, err := w.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(instances) == 0 {
		return nil, fmt.Errorf("service '%s' unavailable", step.Service)
	}
	serviceInstance := router.NewServiceRouter().PickInstance(instances)

	resolvedArgs := make(map[string]interface{})
	if step.ArgsMapping != nil {
		for argName, contextKey := range step.ArgsMapping {
			if val, ok := getValueFromContext(instance.Context, contextKey); ok {
				resolvedArgs[argName] = val
			} else {
				resolvedArgs[argName] = contextKey
			}
		}
	}

	operationType := "query"
	if step.Operation != "" {
		operationType = step.Operation
	} else {
		if strings.HasPrefix(strings.ToLower(step.Field), "create") || strings.HasPrefix(strings.ToLower(step.Field), "update") || strings.HasPrefix(strings.ToLower(step.Field), "delete") {
			operationType = "mutation"
		}
	}

	query := buildPipelineQuery(operationType, step.Field, resolvedArgs, step.Selection)
	reqBodyMap := map[string]interface{}{"query": query}
	jsonBody, _ := json.Marshal(reqBodyMap)

	req, _ := http.NewRequest("POST", serviceInstance.URL, bytes.NewBuffer(jsonBody))
	req.Header = preparePipelineHeaders(instance.OriginalHeaders, step, instance.Context)

	signature, err := w.createRequestSignature(jsonBody)
	if err != nil {
		return nil, fmt.Errorf("failed to sign GraphQL request: %w", err)
	}
	req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	req.Header.Set("X-Grapthway-Node-Address", w.nodeIdentity.Address)
	req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(w.nodeIdentity.PublicKey)))

	stepLog.Request = logging.RequestDetails{Method: req.Method, URL: req.URL.String(), Headers: req.Header.Clone(), Body: string(jsonBody)}

	timeout := common.ParseTimeout(step.TTL, 60*time.Second)
	client := common.GetHTTPClientWithTimeout(timeout)
	log.Printf("🕐 WORKER GraphQL POST %s with timeout: %v (Instance: %s)", serviceInstance.URL, timeout, instance.ID)

	resp, err := client.Do(req)
	if err != nil {
		router := router.NewServiceRouter() // Or pass router as dependency
		router.ReportConnectionFailure(serviceInstance.URL)
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{StatusCode: resp.StatusCode, Headers: resp.Header.Clone(), Body: string(respBody)}

	var result model.GraphQLResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GraphQL response: %w", err)
	}
	if len(result.Errors) > 0 {
		return result.Data, fmt.Errorf("graphql error: %s", result.Errors[0].Message)
	}
	return result.Data, nil
}

func getValueFromContext(context map[string]interface{}, key string) (interface{}, bool) {
	parts := strings.Split(key, ".")
	var current interface{} = context
	for i, part := range parts {
		currentMap, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		value, exists := currentMap[part]
		if !exists {
			return nil, false
		}
		if i == len(parts)-1 {
			return value, true
		}
		current = value
	}
	return nil, false
}

func buildRequestBodyFromContext(bodyMapping map[string]string, context map[string]interface{}) (io.Reader, string, error) {
	if bodyMapping == nil || len(bodyMapping) == 0 {
		return nil, "", nil
	}
	bodyData := make(map[string]interface{})
	for bodyKey, contextKeyOrStaticValue := range bodyMapping {
		if contextValue, exists := getValueFromContext(context, contextKeyOrStaticValue); exists {
			bodyData[bodyKey] = contextValue
		} else {
			bodyData[bodyKey] = contextKeyOrStaticValue
		}
	}
	jsonBody, err := json.Marshal(bodyData)
	if err != nil {
		return nil, "", fmt.Errorf("failed to marshal bodyMapping: %w", err)
	}
	return bytes.NewBuffer(jsonBody), string(jsonBody), nil
}

func preparePipelineHeaders(originalHeaders http.Header, step model.PipelineStep, pipelineContext map[string]interface{}) http.Header {
	reqHeaders := http.Header{}
	reqHeaders.Set("Content-Type", "application/json")
	if originalHeaders != nil {
		for _, hdr := range step.PassHeaders {
			if val := originalHeaders.Get(hdr); val != "" {
				reqHeaders.Set(hdr, val)
			}
		}
	}
	for ctxKey, val := range pipelineContext {
		headerKey := "X-Ctx-" + ctxKey
		var headerVal string
		switch v := val.(type) {
		case string:
			headerVal = v
		default:
			if jsonVal, err := json.Marshal(v); err == nil {
				headerVal = string(jsonVal)
			} else {
				headerVal = fmt.Sprintf("%v", v)
			}
		}
		reqHeaders.Set(headerKey, headerVal)
	}
	return reqHeaders
}

func buildPipelineQuery(operationType, fieldName string, args map[string]interface{}, selection []string) string {
	var argsParts []string
	for key, val := range args {
		argsParts = append(argsParts, fmt.Sprintf("%s: %s", key, util.ValueToString(val, nil)))
	}
	argsStr := ""
	if len(argsParts) > 0 {
		argsStr = fmt.Sprintf("(%s)", strings.Join(argsParts, ", "))
	}
	if len(selection) > 0 {
		if len(selection) == 1 && selection[0] == "_scalar" {
			return fmt.Sprintf("%s { %s%s }", operationType, fieldName, argsStr)
		}
		selectionStr := strings.Join(selection, " ")
		return fmt.Sprintf("%s { %s%s { %s } }", operationType, fieldName, argsStr, selectionStr)
	}
	return fmt.Sprintf("%s { %s%s { %s } }", operationType, fieldName, argsStr, "__typename")
}

func toPipelineStepLog(step model.PipelineStep) logging.PipelineStepLog {
	if step.LedgerExecute != nil {
		return logging.PipelineStepLog{
			Service:     "ledger-service",
			Field:       "delegatedTransfer",
			Conditional: step.Conditional,
		}
	}
	if step.CallWorkflow != "" {
		return logging.PipelineStepLog{
			Service:      "workflow-engine",
			Field:        step.CallWorkflow,
			CallWorkflow: step.CallWorkflow,
			Conditional:  step.Conditional,
			ArgsMapping:  step.ArgsMapping,
		}
	}
	return logging.PipelineStepLog{
		Service:      step.Service,
		Field:        step.Field,
		Path:         step.Path,
		Method:       step.Method,
		Concurrent:   step.Concurrent,
		Assign:       step.Assign,
		ArgsMapping:  step.ArgsMapping,
		CallWorkflow: step.CallWorkflow,
		Conditional:  step.Conditional,
	}
}
