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
		logEntry.PreSuccessSteps = append(logEntry.PreSuccessSteps, stepLog)
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
		// REST step — routes through gateway so pre-pipelines (auth, billing) run
		stepResult, stepErr = w.executeRESTStep(instance, step, &stepLog)
	} else if step.Service != "" && step.Field != "" {
		// GraphQL step — routes through gateway so schema merging and pre-pipelines run
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
			for ctxKey, resKey := range step.Assign {
				if resKey == "*" {
					if step.Field != "" {
						instance.Context[ctxKey] = stepResult[step.Field]
					} else {
						instance.Context[ctxKey] = stepResult
					}
					continue
				}

				// Priority 1: direct root-level key (REST responses)
				if val, ok := stepResult[resKey]; ok {
					instance.Context[ctxKey] = val
					continue
				}

				// Priority 2: dot-path traversal (e.g. "data.product.id")
				if val, ok := getValueFromContext(stepResult, resKey); ok {
					instance.Context[ctxKey] = val
					continue
				}

				// Priority 3: GraphQL field envelope unwrap
				if step.Field != "" {
					if fieldData, ok := stepResult[step.Field]; ok {
						switch fd := fieldData.(type) {
						case map[string]interface{}:
							if val, ok := getValueFromContext(fd, resKey); ok {
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

// ─── pickGatewayInstance ──────────────────────────────────────────────────────
//
// Discovers a healthy gateway node from the service registry.
// The gateway registers itself via selfAnnounce() under systemDeveloperAddress /
// internalGrapthwayServiceName.  We retry a few times because the worker may
// start before the gateway has had a chance to announce itself.
func (w *Worker) pickGatewayInstance() (*router.ServiceInstance, error) {
	var instances []router.ServiceInstance
	var err error

	for i := 0; i < 5; i++ {
		instances, err = w.storage.GetService(systemDeveloperAddress, internalGrapthwayServiceName, "internal")
		if err == nil && len(instances) > 0 {
			break
		}
		// Fallback: try without subgraph filter
		instances, err = w.storage.GetService(systemDeveloperAddress, internalGrapthwayServiceName, "")
		if err == nil && len(instances) > 0 {
			break
		}
		log.Printf("WORKER: Could not find gateway instance (attempt %d/5). Retrying in 2s...", i+1)
		time.Sleep(2 * time.Second)
	}

	if err != nil || len(instances) == 0 {
		return nil, fmt.Errorf("no available gateway nodes after multiple retries")
	}

	inst := router.NewServiceRouter().PickInstance(instances)
	return &inst, nil
}

func (w *Worker) executeLedgerStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, *types.Transaction, error) {
	now := time.Now()
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

	if instance.DeveloperID == "" {
		return nil, nil, fmt.Errorf("cannot execute ledger step: workflow instance is missing developer ID (service owner)")
	}

	tx := types.Transaction{
		ID:        util.GenerateTxID("tx-delegated-", fromAddress, toAddress, amountMicro, now.UnixNano()),
		Type:      model.DelegatedTransferTransaction,
		From:      fromAddress,
		To:        toAddress,
		Amount:    amountMicro,
		Spender:   instance.DeveloperID,
		Timestamp: now,
		CreatedAt: now,
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

// executeTokenLedgerStep handles token transfer operations in durable workflows.
func (w *Worker) executeTokenLedgerStep(instance *Instance, action *model.LedgerExecuteAction, stepLog *logging.PipelineStepLog) (map[string]interface{}, *types.Transaction, error) {
	now := time.Now()
	tokenAddress := action.TokenAddress

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

	if instance.DeveloperID == "" {
		return nil, nil, fmt.Errorf("cannot execute token ledger step: workflow instance is missing developer ID (service owner)")
	}

	tx := types.Transaction{
		ID:           util.GenerateTxID("tx-delegated-token-", fromAddress, toAddress, amountMicro, now.UnixNano()),
		Type:         model.TokenTransferTransaction,
		From:         fromAddress,
		To:           toAddress,
		Amount:       amountMicro,
		TokenAddress: tokenAddress,
		Spender:      instance.DeveloperID,
		Timestamp:    now,
		CreatedAt:    now,
	}

	processedTx, err := w.storage.BroadcastDelegatedTransaction(w.ctx, tx)
	if err != nil {
		stepLog.Response.StatusCode = 500
		stepLog.Response.Body = err.Error()
		return nil, nil, fmt.Errorf("tokenLedgerExecute transaction failed: %w", err)
	}

	respBodyMap := map[string]interface{}{"transactionId": processedTx.ID}
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

	gatewayInst, err := w.pickGatewayInstance()
	if err != nil {
		return nil, fmt.Errorf("no available gateway nodes to execute nested workflow '%s': %w", step.CallWorkflow, err)
	}

	payload := map[string]interface{}{
		"workflowName":     step.CallWorkflow,
		"developerAddress": developerID,
		"context":          instance.Context,
		"traceId":          instance.ParentTraceID,
		"headers":          instance.OriginalHeaders,
	}
	payloadBytes, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", gatewayInst.URL+"/admin/internal/execute-workflow", bytes.NewBuffer(payloadBytes))
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

// ─── executeRESTStep ──────────────────────────────────────────────────────────
//
// Routes the request through a gateway node instead of calling the upstream
// service directly. This ensures:
//
//  1. The service's registered pre-pipeline runs (auth validation, billing, etc.)
//  2. The gateway's rate-limiting and middleware chain applies.
//  3. The request signature comes from a known gateway node rather than an
//     anonymous worker, so downstream GrapthwayMiddleware accepts it.
//
// URL pattern: {gatewayURL}/{developerAddress}/{servicePath}/{stepPath}
// e.g.         http://gw:5001/0xDEV.../product-service/api/admin/tiers/abc
func (w *Worker) executeRESTStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	resolvedService := resolveServiceKey(step.Service, instance.Context)
	developerAddress, serviceName, err := util.ParseCompositeKey(resolvedService)
	if err != nil {
		return nil, fmt.Errorf("invalid service format in pipeline step: %s", step.Service)
	}

	// Look up the target service to validate it exists and get its registered path.
	serviceInstances, err := w.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(serviceInstances) == 0 {
		return nil, fmt.Errorf("service '%s' unavailable", resolvedService)
	}
	// We only need the service metadata (Path) to build the gateway URL correctly.
	// The actual instance selection is done by the gateway itself.
	serviceInst := router.NewServiceRouter().PickInstance(serviceInstances)

	// Route through the gateway so pipelines and middleware fire.
	gatewayInst, err := w.pickGatewayInstance()
	if err != nil {
		return nil, fmt.Errorf("no gateway available to route REST step for service '%s': %w", resolvedService, err)
	}

	// Interpolate $variables in the step path before building the URL.
	interpolatedPath := interpolatePathVars(step.Path, instance.Context)

	// The gateway expects:  /{developerAddress}/{servicePath_and_rest}
	// servicePath is registered as e.g. "/product-service/api"
	// interpolatedPath is the full path the service registered, e.g. "/product-service/api/admin/tiers/abc"
	// so we just append it after the developer address.
	//
	// If the step path already includes the service base path (the common pattern)
	// we use it directly. If it doesn't (bare path like "/admin/tiers/abc"), we
	// prepend the service's registered path.
	fullServicePath := interpolatedPath
	if serviceInst.Path != "" && !strings.HasPrefix(interpolatedPath, serviceInst.Path) {
		fullServicePath = serviceInst.Path + interpolatedPath
	}
	gatewayURL := fmt.Sprintf("%s/%s%s", gatewayInst.URL, developerAddress, fullServicePath)

	var bodyBytes []byte
	var reqBodyString string

	if strings.ToUpper(step.Method) == "GET" {
		// For GET: convert BodyMapping to query parameters.
		if len(step.BodyMapping) > 0 {
			parsedURL, parseErr := url.Parse(gatewayURL)
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
						q.Set(paramKey, contextKey)
					}
				}
				parsedURL.RawQuery = q.Encode()
				gatewayURL = parsedURL.String()
			}
		}
		bodyBytes = []byte{}
		reqBodyString = ""
	} else {
		// For POST/PUT/PATCH/DELETE: build JSON body from BodyMapping.
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

	req, err := http.NewRequest(step.Method, gatewayURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, err
	}

	// Pass original request headers (Authorization etc.) so the gateway's
	// pre-pipeline can pick them up via PassHeaders.
	req.Header = preparePipelineHeaders(instance.OriginalHeaders, step, instance.Context)

	// Sign the request with the worker's node identity so the downstream service's
	// GrapthwayMiddleware accepts it. The gateway will re-sign when forwarding
	// to the upstream service, but this signature covers the gateway leg.
	signature, err := w.createRequestSignature(bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign REST request: %w", err)
	}
	req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	req.Header.Set("X-Grapthway-Node-Address", w.nodeIdentity.Address)
	req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(w.nodeIdentity.PublicKey)))

	stepLog.Request = logging.RequestDetails{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header.Clone(),
		Body:    reqBodyString,
	}

	timeout := common.ParseTimeout(step.TTL, 60*time.Second)
	client := common.GetHTTPClientWithTimeout(timeout)
	log.Printf("🕐 WORKER REST (via gateway) %s %s with timeout: %v (Instance: %s)", step.Method, gatewayURL, timeout, instance.ID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       string(respBody),
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("service returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if len(respBody) > 0 && strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal REST response from %s: %w", gatewayURL, err)
		}
		return result, nil
	}

	return map[string]interface{}{}, nil
}

// ─── executeGraphQLStep ───────────────────────────────────────────────────────
//
// Routes the GraphQL request through a gateway node instead of posting directly
// to the upstream service. This ensures:
//
//  1. Schema stitching / federation is applied by the gateway's merged schema.
//  2. The service's registered GraphQL pre-pipeline runs (auth, billing, etc.).
//  3. The correct /graphql endpoint URL is used regardless of what the service
//     registered as its base URL.
//
// URL pattern: {gatewayURL}/{developerAddress}/{subgraphName}/graphql
// e.g.         http://gw:5001/0xDEV.../my-subgraph/graphql
func (w *Worker) executeGraphQLStep(instance *Instance, step model.PipelineStep, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	resolvedService := resolveServiceKey(step.Service, instance.Context)
	developerAddress, serviceName, err := util.ParseCompositeKey(resolvedService)
	if err != nil {
		return nil, fmt.Errorf("invalid service format in pipeline step: %s", step.Service)
	}

	// Look up the target service to get its subgraph name (needed for the gateway URL).
	serviceInstances, err := w.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(serviceInstances) == 0 {
		return nil, fmt.Errorf("service '%s' unavailable", resolvedService)
	}
	serviceInst := router.NewServiceRouter().PickInstance(serviceInstances)

	// Route through the gateway.
	gatewayInst, err := w.pickGatewayInstance()
	if err != nil {
		return nil, fmt.Errorf("no gateway available to route GraphQL step for service '%s': %w", resolvedService, err)
	}

	// Build the correct gateway GraphQL URL:
	// /{developerAddress}/{subgraphName}/graphql
	subgraphName := serviceInst.Subgraph
	if subgraphName == "" {
		subgraphName = serviceName // fall back to service name as subgraph
	}
	graphqlURL := fmt.Sprintf("%s/%s/%s/graphql", gatewayInst.URL, developerAddress, subgraphName)

	// Resolve arguments from workflow context.
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

	// Determine operation type (query vs mutation).
	operationType := "query"
	if step.Operation != "" {
		operationType = step.Operation
	} else if strings.ToLower(step.Method) == "mutation" {
		operationType = "mutation"
	} else {
		lower := strings.ToLower(step.Field)
		if strings.HasPrefix(lower, "create") ||
			strings.HasPrefix(lower, "update") ||
			strings.HasPrefix(lower, "delete") ||
			strings.HasPrefix(lower, "insert") ||
			strings.HasPrefix(lower, "upsert") ||
			strings.HasPrefix(lower, "remove") {
			operationType = "mutation"
		}
	}

	query := buildPipelineQuery(operationType, step.Field, resolvedArgs, step.Selection)
	reqBodyMap := map[string]interface{}{"query": query}
	jsonBody, _ := json.Marshal(reqBodyMap)

	req, err := http.NewRequest("POST", graphqlURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create GraphQL request: %w", err)
	}

	// Pass original headers (Authorization etc.) through.
	req.Header = preparePipelineHeaders(instance.OriginalHeaders, step, instance.Context)
	req.Header.Set("Content-Type", "application/json")

	// Sign with node identity.
	signature, err := w.createRequestSignature(jsonBody)
	if err != nil {
		return nil, fmt.Errorf("failed to sign GraphQL request: %w", err)
	}
	req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	req.Header.Set("X-Grapthway-Node-Address", w.nodeIdentity.Address)
	req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(w.nodeIdentity.PublicKey)))

	stepLog.Request = logging.RequestDetails{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header.Clone(),
		Body:    string(jsonBody),
	}

	timeout := common.ParseTimeout(step.TTL, 60*time.Second)
	client := common.GetHTTPClientWithTimeout(timeout)
	log.Printf("🕐 WORKER GraphQL (via gateway) POST %s with timeout: %v (Instance: %s)", graphqlURL, timeout, instance.ID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       string(respBody),
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("gateway returned status %d for GraphQL request: %s", resp.StatusCode, string(respBody))
	}

	var result model.GraphQLResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GraphQL response: %w", err)
	}
	if len(result.Errors) > 0 {
		return result.Data, fmt.Errorf("graphql error: %s", result.Errors[0].Message)
	}
	return result.Data, nil
}

// interpolatePathVars replaces $-prefixed variables in a path template with
// values resolved from the workflow instance context. Supports:
//
//	$request.body.<field>  →  instance.Context["request"]["body"]["<field>"]
//	$args.<field>          →  instance.Context["args"]["<field>"]
//	$<field>               →  instance.Context["<field>"]
//
// Multiple injections per path are fully supported.
// Unknown tokens are left unchanged so callers can detect missing values.
func interpolatePathVars(path string, ctx map[string]interface{}) string {
	if ctx == nil || !strings.Contains(path, "$") {
		return path
	}
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		if !strings.Contains(seg, "$") {
			continue
		}
		result := seg
		for {
			dollarIdx := strings.Index(result, "$")
			if dollarIdx == -1 {
				break
			}
			tail := result[dollarIdx+1:]
			endIdx := strings.IndexAny(tail, "/?&=")
			var token string
			if endIdx == -1 {
				token = tail
			} else {
				token = tail[:endIdx]
			}
			if token == "" {
				break
			}
			val, found := getValueFromContext(ctx, token)
			if !found {
				break
			}
			result = strings.Replace(result, "$"+token, fmt.Sprintf("%v", val), 1)
		}
		segments[i] = result
	}
	return strings.Join(segments, "/")
}

// resolveServiceKey resolves a $-prefixed variable in the developer-address
// portion of a composite service key ("developerAddress:serviceName").
func resolveServiceKey(service string, ctx map[string]interface{}) string {
	if ctx == nil || !strings.Contains(service, "$") {
		return service
	}
	colonIdx := strings.Index(service, ":")
	if colonIdx == -1 {
		return service
	}
	addrPart := service[:colonIdx]
	svcPart := service[colonIdx:]

	if !strings.HasPrefix(addrPart, "$") {
		return service
	}

	token := addrPart[1:]
	if token == "" {
		return service
	}

	val, found := getValueFromContext(ctx, token)
	if !found {
		return service
	}
	return fmt.Sprintf("%v", val) + svcPart
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
