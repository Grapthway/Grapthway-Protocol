package pipeline

import (
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
	"sync"
	"time"

	"grapthway/pkg/common"
	"grapthway/pkg/crypto"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/router"
	"grapthway/pkg/schema"
	"grapthway/pkg/util"

	"github.com/Knetic/govaluate"
	json "github.com/json-iterator/go"
)

type DurableWorkflowStarter interface {
	StartWorkflow(workflowName string, initialContext map[string]interface{}, parentTraceID string, isInternal bool, originalHeaders http.Header, developerID string) (string, error)
}

type PipelineExecutor struct {
	storage        model.Storage
	router         *router.ServiceRouter
	logger         *logging.Logger
	merger         *schema.Merger
	workflowEngine DurableWorkflowStarter
}

func NewPipelineExecutor(storage model.Storage, router *router.ServiceRouter, logger *logging.Logger, merger *schema.Merger, wfEngine DurableWorkflowStarter) *PipelineExecutor {
	return &PipelineExecutor{
		storage:        storage,
		router:         router,
		logger:         logger,
		merger:         merger,
		workflowEngine: wfEngine,
	}
}

func (pe *PipelineExecutor) getOperationType(fieldName string, s *model.PipelineStep) string {
	if s.Operation != "" {
		return s.Operation
	}
	schema := pe.merger.GetSchema("")
	if schema == nil {
		return "query"
	}
	if mutationType := schema.MutationType(); mutationType != nil {
		if _, ok := mutationType.Fields()[fieldName]; ok {
			return "mutation"
		}
	}
	return "query"
}

func (pe *PipelineExecutor) ExecutePrePipeline(pipeline *model.PipelineConfig, ctx context.Context, initialContext map[string]interface{}) (*model.ExecutionResult, error) {
	if pipeline == nil {
		return &model.ExecutionResult{Context: initialContext, Transactions: []types.Transaction{}}, nil
	}
	return pe.executePipeline(pipeline.Pre, ctx, initialContext, true)
}

func (pe *PipelineExecutor) ExecutePostPipeline(pipeline *model.PipelineConfig, mainResponse map[string]interface{}, ctx context.Context) (map[string]interface{}, error) {
	if pipeline == nil {
		return mainResponse, nil
	}
	mergedContext := make(map[string]interface{})
	pCtx, pCtxOk := ctx.Value("pipelineContext").(*model.ExecutionResult)
	if pCtxOk && pCtx.Context != nil {
		for k, v := range pCtx.Context {
			mergedContext[k] = v
		}
	}
	for k, v := range mainResponse {
		mergedContext[k] = v
	}
	allPostSteps := pipeline.Post
	if len(allPostSteps) == 0 {
		return mainResponse, nil
	}
	result, err := pe.executePipeline(allPostSteps, ctx, mergedContext, false)
	if err != nil {
		log.Printf("Error during post-pipeline execution: %v", err)
	}

	if pCtxOk && result != nil {
		pCtx.Transactions = append(pCtx.Transactions, result.Transactions...)
	}

	for k, v := range result.Context {
		if _, exists := mainResponse[k]; !exists {
			mainResponse[k] = v
		}
	}
	return mainResponse, nil
}

func (pe *PipelineExecutor) executePipeline(steps []model.PipelineStep, ctx context.Context, initialContext map[string]interface{}, isPrePipeline bool) (*model.ExecutionResult, error) {
	result := &model.ExecutionResult{
		Context:      make(map[string]interface{}),
		PostSteps:    []model.PipelineStep{},
		Transactions: []types.Transaction{},
	}

	if initialContext != nil {
		for k, v := range initialContext {
			result.Context[k] = v
		}
	}

	if len(steps) == 0 {
		return result, nil
	}

	logEntry, _ := ctx.Value("log").(*logging.LogEntry)
	wg, _ := ctx.Value("waitgroup").(*sync.WaitGroup)
	var logMutex sync.Mutex
	var executedBlockingSteps []model.PipelineStep

	ctxWithResult := context.WithValue(ctx, "pipelineContext", result)

	for _, step := range steps {
		stepLog := toPipelineStepLog(step)
		shouldExecute, err := pe.evaluateConditional(step, result.Context)
		if err != nil {
			err = fmt.Errorf("step '%s' conditional evaluation failed: %w", step.Field, err)
			stepLog.Error = err.Error()
			logMutex.Lock()
			pe.appendLogStep(logEntry, stepLog, false, isPrePipeline)
			logMutex.Unlock()
			return nil, err
		}
		if !shouldExecute {
			log.Printf("TraceID %s: Step '%s' skipped due to conditional.", logEntry.TraceID, step.Field)
			continue
		}
		var stepErr error
		var workflowPostSteps []model.PipelineStep
		retryAttempts := 1
		if step.RetryPolicy != nil {
			retryAttempts = step.RetryPolicy.Attempts + 1
		}
		for i := 0; i < retryAttempts; i++ {
			stepLog.RetryCount = i
			if step.Concurrent {
				if wg != nil {
					wg.Add(1)
				}
				go func(s model.PipelineStep, sLog logging.PipelineStepLog) {
					if wg != nil {
						defer wg.Done()
					}
					_, err := pe.executeSingleStep(s, &sLog, ctxWithResult, result.Context)
					logMutex.Lock()
					pe.appendLogStep(logEntry, sLog, err == nil, isPrePipeline)
					logMutex.Unlock()
				}(step, stepLog)
				stepErr = nil
				break
			}
			workflowPostSteps, stepErr = pe.executeSingleStep(step, &stepLog, ctxWithResult, result.Context)
			if stepErr == nil {
				if len(workflowPostSteps) > 0 {
					result.PostSteps = append(result.PostSteps, workflowPostSteps...)
				}
				break
			}
			if i < retryAttempts-1 && step.RetryPolicy != nil {
				log.Printf("TraceID %s: Step '%s' failed, retrying in %d seconds... Error: %v", logEntry.TraceID, step.Field, step.RetryPolicy.DelaySeconds, stepErr)
				time.Sleep(time.Duration(step.RetryPolicy.DelaySeconds) * time.Second)
			}
		}
		if step.Concurrent {
			continue
		}
		logMutex.Lock()
		pe.appendLogStep(logEntry, stepLog, stepErr == nil, isPrePipeline)
		logMutex.Unlock()
		if stepErr != nil {
			errorMessage := fmt.Sprintf("step '%s' failed: %v", step.Field, stepErr)
			if step.OnError != nil {
				if step.OnError.Message != "" {
					errorMessage = step.OnError.Message
				}
				if step.OnError.Stop != nil && *step.OnError.Stop {
					pe.executeRollback(executedBlockingSteps, ctx, logEntry, result)
					return nil, fmt.Errorf(errorMessage)
				}
			}
			if isPrePipeline {
				pe.executeRollback(executedBlockingSteps, ctx, logEntry, result)
				return nil, fmt.Errorf(errorMessage)
			}
		} else {
			executedBlockingSteps = append(executedBlockingSteps, step)
		}
	}
	return result, nil
}

func (pe *PipelineExecutor) executeSingleStep(step model.PipelineStep, stepLog *logging.PipelineStepLog, parentCtx context.Context, pipelineContext map[string]interface{}) ([]model.PipelineStep, error) {
	if step.LedgerExecute != nil {
		err := pe.executeLedgerAction(step, parentCtx, pipelineContext)
		return nil, err
	}

	if step.CallWorkflow != "" {
		return pe.executeWorkflowCall(step, parentCtx, pipelineContext)
	}

	developerAddress, serviceName, err := util.ParseCompositeKey(step.Service)
	if err != nil {
		return nil, fmt.Errorf("invalid service format in pipeline step: %s", step.Service)
	}

	instances, err := pe.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(instances) == 0 {
		return nil, fmt.Errorf("pipeline service %s unavailable", step.Service)
	}
	instance := pe.router.PickInstance(instances)
	originalHeaders, _ := parentCtx.Value("headers").(http.Header)
	reqHeaders := preparePipelineHeaders(originalHeaders, step, pipelineContext)
	var respData map[string]interface{}
	var respErr error
	if instance.Type == "rest" {
		var requestBodyReader io.Reader
		var requestBodyString string
		var bodyErr error

		// ✅ For GET requests, BodyMapping becomes query params (handled in executeRESTRequest)
		// For other methods, BodyMapping becomes JSON body
		if strings.ToUpper(step.Method) != "GET" {
			requestBodyReader, requestBodyString, bodyErr = buildRequestBodyFromContext(step.BodyMapping, pipelineContext)
			if bodyErr != nil {
				return nil, fmt.Errorf("pipeline failed to build request body for %s: %w", step.Service, bodyErr)
			}
		}

		respData, respErr = pe.executeRESTRequest(instance.URL, step, reqHeaders, requestBodyReader, requestBodyString, stepLog)
	} else {
		resolvedArgs := make(map[string]interface{})
		if step.ArgsMapping != nil {
			for argName, contextKey := range step.ArgsMapping {
				if val, ok := getValueFromContext(pipelineContext, contextKey); ok {
					resolvedArgs[argName] = val
				} else {
					resolvedArgs[argName] = contextKey
				}
			}
		}
		operationType := pe.getOperationType(step.Field, &step)
		query := buildPipelineQuery(operationType, step.Field, resolvedArgs, step.Selection)
		gqlResp, err := pe.executeGraphQLRequest(instance.URL, query, reqHeaders, stepLog, step)
		respErr = err
		if gqlResp != nil && gqlResp.Data != nil {
			respData = gqlResp.Data
		}
	}
	if respErr == nil && respData != nil {
		dataToAssign := respData
		if step.Field != "" {
			if fieldData, ok := respData[step.Field]; ok {
				dataToAssign = map[string]interface{}{step.Field: fieldData}
			}
		} else if step.Service != "" {
			if serviceData, ok := respData[step.Service]; ok {
				dataToAssign = map[string]interface{}{step.Service: serviceData}
			}
		}
		assignDataToContext(pipelineContext, step.Assign, dataToAssign, step.Field)
	}
	return nil, respErr
}

// Helper function to resolve a string value. It first tries to find a key in the context,
// and if it fails, it assumes the input string is a literal value.
func (pe *PipelineExecutor) resolveStringValue(input string, context map[string]interface{}) (string, error) {
	if val, ok := getValueFromContext(context, input); ok {
		if strVal, ok := val.(string); ok {
			return strVal, nil
		}
		return "", fmt.Errorf("context value for key '%s' is not a string", input)
	}
	return input, nil
}

// Helper function to resolve a numeric amount. It handles context lookups for string keys,
// as well as literal numbers (float64, int, json.Number). It ensures the final value is in micro-GCU.
func (pe *PipelineExecutor) resolveAmountValue(input interface{}, context map[string]interface{}) (uint64, error) {
	switch v := input.(type) {
	case string:
		amountValue, ok := getValueFromContext(context, v)
		if !ok {
			parsedFloat, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return 0, fmt.Errorf("'amount' string '%s' is not in context and not a valid number", v)
			}
			amountValue = parsedFloat
		}
		floatAmount, err := util.ToFloat64(amountValue)
		if err != nil {
			return 0, fmt.Errorf("resolved amount value '%v' is not a valid number: %w", amountValue, err)
		}
		return uint64(floatAmount), nil
	case float64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case json.Number:
		floatAmount, err := v.Float64()
		if err != nil {
			return 0, fmt.Errorf("'amount' JSON number '%s' is not a valid float", v.String())
		}
		return uint64(floatAmount), nil
	default:
		return 0, fmt.Errorf("'amount' has an invalid type %T", input)
	}
}

func (pe *PipelineExecutor) executeLedgerAction(step model.PipelineStep, parentCtx context.Context, pipelineContext map[string]interface{}) error {
	action := step.LedgerExecute
	logEntry, _ := parentCtx.Value("log").(*logging.LogEntry)

	// Check if this is a token operation
	if action.TokenAddress != "" {
		// This is a token transfer operation
		return pe.executeTokenTransfer(action, parentCtx, pipelineContext)
	}

	fromAddress, err := pe.resolveStringValue(action.From.(string), pipelineContext)
	if err != nil {
		return fmt.Errorf("ledgerExecute failed to resolve 'from' address: %w", err)
	}

	toAddress, err := pe.resolveStringValue(action.To.(string), pipelineContext)
	if err != nil {
		return fmt.Errorf("ledgerExecute failed to resolve 'to' address: %w", err)
	}

	amountMicro, err := pe.resolveAmountValue(action.Amount, pipelineContext)
	if err != nil {
		return fmt.Errorf("ledgerExecute failed to resolve 'amount': %w", err)
	}

	// Get the route owner (service owner) as the spender
	// The route owner is the one who created the service/workflow
	serviceDeveloper, ok := parentCtx.Value("serviceDeveloper").(string)
	if !ok || serviceDeveloper == "" {
		return fmt.Errorf("ledgerExecute failed: could not determine service owner (spender)")
		// Fallback to wallet_address for backward compatibility
		// serviceDeveloper, ok = parentCtx.Value("wallet_address").(string)
		// if !ok || routeOwner == "" {
		// 	return fmt.Errorf("ledgerExecute failed: could not determine service owner (spender)")
		// }
	}

	tx := types.Transaction{
		ID:        fmt.Sprintf("tx-delegated-%d", time.Now().UnixNano()),
		Type:      model.DelegatedTransferTransaction,
		From:      fromAddress,
		To:        toAddress,
		Amount:    amountMicro,
		Spender:   serviceDeveloper, // Service owner is the spender
		Timestamp: time.Now(),
		CreatedAt: time.Now(),
	}

	if _, err := pe.storage.SubmitDelegatedTransaction(parentCtx, tx); err != nil {
		return fmt.Errorf("ledgerExecute transaction failed: %w", err)
	}

	if pCtx, ok := parentCtx.Value("pipelineContext").(*model.ExecutionResult); ok && pCtx != nil {
		pCtx.Transactions = append(pCtx.Transactions, tx)
	}

	log.Printf("TraceID %s: Successfully executed delegated transfer of %d from %s to %s (spender: %s)", logEntry.TraceID, amountMicro, fromAddress, toAddress, serviceDeveloper)
	return nil
}

// New function to handle token transfers in pipelines
func (pe *PipelineExecutor) executeTokenTransfer(action *model.LedgerExecuteAction, parentCtx context.Context, pipelineContext map[string]interface{}) error {
	logEntry, _ := parentCtx.Value("log").(*logging.LogEntry)

	// Resolve token address
	tokenAddress, err := pe.resolveStringValue(action.TokenAddress, pipelineContext)
	if err != nil {
		return fmt.Errorf("failed to resolve token address: %w", err)
	}

	// Resolve from address
	fromAddress, err := pe.resolveStringValue(action.From.(string), pipelineContext)
	if err != nil {
		return fmt.Errorf("failed to resolve 'from' address: %w", err)
	}

	// Resolve to address
	toAddress, err := pe.resolveStringValue(action.To.(string), pipelineContext)
	if err != nil {
		return fmt.Errorf("failed to resolve 'to' address: %w", err)
	}

	// Resolve amount (already in token's micro units)
	var amountMicro uint64
	switch v := action.Amount.(type) {
	case string:
		amountValue, ok := getValueFromContext(pipelineContext, v)
		if !ok {
			parsedFloat, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("'amount' string '%s' is not in context and not a valid number", v)
			}
			amountValue = parsedFloat
		}
		floatAmount, err := util.ToFloat64(amountValue)
		if err != nil {
			return fmt.Errorf("resolved amount value '%v' is not a valid number: %w", amountValue, err)
		}
		amountMicro = uint64(floatAmount)
	case float64:
		amountMicro = uint64(v)
	case int:
		amountMicro = uint64(v)
	case json.Number:
		floatAmount, err := v.Float64()
		if err != nil {
			return fmt.Errorf("'amount' JSON number '%s' is not a valid float", v.String())
		}
		amountMicro = uint64(floatAmount)
	default:
		return fmt.Errorf("'amount' has an invalid type %T", v)
	}

	// Get service developer as spender (for allowance-based token transfers)
	serviceDeveloper, ok := parentCtx.Value("serviceDeveloper").(string)
	if !ok || serviceDeveloper == "" {
		return fmt.Errorf("could not determine service owner (spender)")
	}

	// Create token transfer transaction
	tx := types.Transaction{
		ID:           fmt.Sprintf("tx-token-transfer-pipeline-%d", time.Now().UnixNano()),
		Type:         model.TokenTransferTransaction,
		From:         fromAddress,
		To:           toAddress,
		Amount:       amountMicro,
		TokenAddress: tokenAddress,
		Spender:      serviceDeveloper, // Track who initiated this transfer
		Timestamp:    time.Now(),
		CreatedAt:    time.Now(),
	}

	// Submit through ledger
	if _, err := pe.storage.SubmitDelegatedTransaction(parentCtx, tx); err != nil {
		return fmt.Errorf("token transfer transaction failed: %w", err)
	}

	// Record in execution result
	if pCtx, ok := parentCtx.Value("pipelineContext").(*model.ExecutionResult); ok && pCtx != nil {
		pCtx.Transactions = append(pCtx.Transactions, tx)
	}

	log.Printf("TraceID %s: Successfully executed token transfer of %d (token: %s) from %s to %s",
		logEntry.TraceID, amountMicro, tokenAddress, fromAddress, toAddress)
	return nil
}

func (pe *PipelineExecutor) executeWorkflowCall(step model.PipelineStep, parentCtx context.Context, parentPipelineContext map[string]interface{}) ([]model.PipelineStep, error) {
	logEntry, _ := parentCtx.Value("log").(*logging.LogEntry)
	originalHeaders, _ := parentCtx.Value("headers").(http.Header)

	// CRITICAL FIX: Determine the correct developer ID for workflow lookup
	// Priority: serviceDeveloper > wallet_address > developerID
	developerID, ok := parentCtx.Value("serviceDeveloper").(string)
	if !ok || developerID == "" {
		developerID, ok = parentCtx.Value("wallet_address").(string)
		if !ok || developerID == "" {
			developerID, _ = parentCtx.Value("developerID").(string)
			if developerID == "" {
				return nil, fmt.Errorf("cannot determine developer context for workflow call")
			}
		}
	}

	log.Printf("WORKFLOW CALL: Looking up workflow '%s' for developer '%s'", step.CallWorkflow, developerID)

	targetWorkflow, err := pe.storage.GetMiddlewarePipeline(developerID, step.CallWorkflow)
	if err != nil || targetWorkflow == nil {
		return nil, fmt.Errorf("workflow '%s' not found for developer %s: %w", step.CallWorkflow, developerID, err)
	}

	if step.ArgsMapping == nil {
		step.ArgsMapping = map[string]string{"args": "args"}
	}

	initialChildContext := make(map[string]interface{})
	if step.ArgsMapping != nil {
		for childKey, parentKey := range step.ArgsMapping {
			if val, ok := getValueFromContext(parentPipelineContext, parentKey); ok {
				initialChildContext[childKey] = val
			} else {
				initialChildContext[childKey] = parentKey
			}
		}
	}

	if targetWorkflow.IsDurable {
		if pe.workflowEngine == nil {
			return nil, fmt.Errorf("durable workflow engine is not initialized")
		}
		instanceID, err := pe.workflowEngine.StartWorkflow(step.CallWorkflow, initialChildContext, logEntry.TraceID, targetWorkflow.IsInternal, originalHeaders, developerID)
		if err != nil {
			return nil, fmt.Errorf("failed to start durable workflow '%s': %w", step.CallWorkflow, err)
		}
		logEntry.WorkflowInstanceID = instanceID
		log.Printf("TraceID %s: Started durable workflow '%s' with InstanceID %s", logEntry.TraceID, step.CallWorkflow, instanceID)
		return targetWorkflow.Post, nil
	}

	result, err := pe.ExecutePrePipeline(targetWorkflow, parentCtx, initialChildContext)
	if err == nil && result != nil {
		for key, value := range result.Context {
			parentPipelineContext[key] = value
		}
		return append(result.PostSteps, targetWorkflow.Post...), nil
	}
	return nil, err
}

func (pe *PipelineExecutor) appendLogStep(entry *logging.LogEntry, stepLog logging.PipelineStepLog, success bool, isPre bool) {
	if isPre {
		if success {
			entry.PreSuccessSteps = append(entry.PreSuccessSteps, stepLog)
		} else {
			entry.PreFailureSteps = append(entry.PreFailureSteps, stepLog)
		}
	} else {
		if success {
			entry.PostSuccessSteps = append(entry.PostSuccessSteps, stepLog)
		} else {
			entry.PostFailureSteps = append(entry.PostFailureSteps, stepLog)
		}
	}
}

func (pe *PipelineExecutor) evaluateConditional(step model.PipelineStep, context map[string]interface{}) (bool, error) {
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

func (pe *PipelineExecutor) executeRollback(steps []model.PipelineStep, ctx context.Context, logEntry *logging.LogEntry, pipelineResult *model.ExecutionResult) {
	log.Printf("ROLLBACK: Initiating rollback for %d steps for TraceID %s.", len(steps), logEntry.TraceID)
	originalHeaders, _ := ctx.Value("headers").(http.Header)

	if pipelineResult != nil && len(pipelineResult.Transactions) > 0 {
		log.Printf("ROLLBACK: Reversing %d ledger transactions for TraceID %s.", len(pipelineResult.Transactions), logEntry.TraceID)
		for _, tx := range pipelineResult.Transactions {
			if tx.Type != model.DelegatedTransferTransaction {
				continue
			}

			// Create a debit transaction to take the money back from the original recipient.
			rollbackDebitTx := types.Transaction{
				ID:        fmt.Sprintf("tx-rollback-debit-%s", tx.ID),
				Type:      model.RollbackDebitTransaction,
				From:      tx.To, // Debit the original recipient
				Amount:    tx.Amount,
				Timestamp: time.Now(),
				CreatedAt: time.Now(),
			}

			// Create a credit transaction to give the money back to the original sender.
			rollbackCreditTx := types.Transaction{
				ID:        fmt.Sprintf("tx-rollback-credit-%s", tx.ID),
				Type:      model.RollbackCreditTransaction,
				To:        tx.From, // Credit the original sender
				Amount:    tx.Amount,
				Timestamp: time.Now(),
				CreatedAt: time.Now(),
			}

			// Submit both system transactions to the ledger.
			if _, err := pe.storage.SubmitDelegatedTransaction(context.Background(), rollbackDebitTx); err != nil {
				log.Printf("CRITICAL_ROLLBACK_FAILURE for TraceID %s: Failed to submit rollback debit for tx %s: %v", logEntry.TraceID, tx.ID, err)
			} else {
				log.Printf("ROLLBACK: Submitted rollback debit for tx %s successfully.", tx.ID)
			}

			if _, err := pe.storage.SubmitDelegatedTransaction(context.Background(), rollbackCreditTx); err != nil {
				log.Printf("CRITICAL_ROLLBACK_FAILURE for TraceID %s: Failed to submit rollback credit for tx %s: %v", logEntry.TraceID, tx.ID, err)
			} else {
				log.Printf("ROLLBACK: Submitted rollback credit for tx %s successfully.", tx.ID)
			}
		}
	}

	for i := len(steps) - 1; i >= 0; i-- {
		step := steps[i]
		if step.OnError != nil && len(step.OnError.Rollback) > 0 {
			for _, rollbackStep := range step.OnError.Rollback {
				pe.executeRollbackStep(rollbackStep, originalHeaders, logEntry, pipelineResult.Context)
			}
		}
	}
}

func (pe *PipelineExecutor) executeRollbackStep(step model.PipelineStep, originalHeaders http.Header, logEntry *logging.LogEntry, pipelineContext map[string]interface{}) {
	stepLog := toPipelineStepLog(step)
	stepLog.IsRollbackStep = true

	developerAddress, serviceName, err := util.ParseCompositeKey(step.Service)
	if err != nil {
		stepLog.Error = fmt.Sprintf("Rollback failed: invalid service format '%s'", step.Service)
		logEntry.RollbackFailure = append(logEntry.RollbackFailure, stepLog)
		log.Println(stepLog.Error)
		return
	}

	instances, err := pe.storage.GetService(developerAddress, serviceName, "")
	if err != nil || len(instances) == 0 {
		stepLog.Error = fmt.Sprintf("Rollback service %s unavailable", step.Service)
		logEntry.RollbackFailure = append(logEntry.RollbackFailure, stepLog)
		log.Println(stepLog.Error)
		return
	}
	instance := pe.router.PickInstance(instances)
	reqHeaders := preparePipelineHeaders(originalHeaders, step, pipelineContext)

	var respErr error

	if instance.Type == "rest" {
		requestBodyReader, requestBodyString, bodyErr := buildRequestBodyFromContext(step.BodyMapping, pipelineContext)
		if bodyErr != nil {
			respErr = fmt.Errorf("failed to build rollback request body: %w", bodyErr)
		} else {
			_, respErr = pe.executeRESTRequest(instance.URL, step, reqHeaders, requestBodyReader, requestBodyString, &stepLog)
		}
	} else {
		resolvedArgs := make(map[string]interface{})
		if step.ArgsMapping != nil {
			for argName, contextKey := range step.ArgsMapping {
				if val, ok := getValueFromContext(pipelineContext, contextKey); ok {
					resolvedArgs[argName] = val
				}
			}
		}
		operationType := pe.getOperationType(step.Field, &step)
		query := buildPipelineQuery(operationType, step.Field, resolvedArgs, step.Selection)
		_, respErr = pe.executeGraphQLRequest(instance.URL, query, reqHeaders, &stepLog, step)
	}

	if respErr != nil {
		stepLog.Error = respErr.Error()
		logEntry.RollbackFailure = append(logEntry.RollbackFailure, stepLog)
		log.Printf("ROLLBACK_FAILURE for service %s on TraceID %s: %v", step.Service, logEntry.TraceID, respErr)
	} else {
		logEntry.RollbackSuccess = append(logEntry.RollbackSuccess, stepLog)
		log.Printf("ROLLBACK_SUCCESS for service %s on TraceID %s", step.Service, logEntry.TraceID)
	}
}

func (pe *PipelineExecutor) createRequestSignature(body []byte) ([]byte, error) {
	digest := sha256.Sum256(body)
	nodeIdentity := pe.storage.GetNodeIdentity()
	return nodeIdentity.Sign(digest[:])
}

func (pe *PipelineExecutor) executeRESTRequest(baseURL string, step model.PipelineStep, headers http.Header, requestBodyReader io.Reader, bodyStr string, stepLog *logging.PipelineStepLog) (map[string]interface{}, error) {
	var bodyBytes []byte
	var body io.Reader
	var err error

	if requestBodyReader != nil {
		bodyBytes, err = io.ReadAll(requestBodyReader)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body for signing: %w", err)
		}
		body = bytes.NewBuffer(bodyBytes)
	} else {
		body = bytes.NewBuffer([]byte{})
	}

	signature, err := pe.createRequestSignature(bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign REST request: %w", err)
	}

	nodeIdentity := pe.storage.GetNodeIdentity()
	headers.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	headers.Set("X-Grapthway-Node-Address", nodeIdentity.Address)
	headers.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(nodeIdentity.PublicKey)))

	fullURL := baseURL + step.Path

	// ✅ CRITICAL: For GET requests, convert BodyMapping to query parameters
	if strings.ToUpper(step.Method) == "GET" && step.BodyMapping != nil && len(step.BodyMapping) > 0 {
		parsedURL, parseErr := url.Parse(fullURL)
		if parseErr == nil {
			q := parsedURL.Query()

			// Get context from the parent caller
			pipelineContext := make(map[string]interface{})

			// Build query params from BodyMapping
			for paramKey, contextKey := range step.BodyMapping {
				if val, ok := getValueFromContext(pipelineContext, contextKey); ok {
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

		// For GET, body should be nil
		body = nil
		bodyStr = ""
	}

	stepLog.Request = logging.RequestDetails{
		Method:  step.Method,
		URL:     fullURL,
		Headers: headers.Clone(),
		Body:    bodyStr,
	}

	req, err := http.NewRequest(step.Method, fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST request: %w", err)
	}
	req.Header = headers

	timeout := common.ParseTimeout(step.TTL, 60*time.Second)
	client := common.GetHTTPClientWithTimeout(timeout)

	log.Printf("🕐 REST request to %s with timeout: %v", fullURL, timeout)
	resp, err := client.Do(req)
	if err != nil {
		stepLog.Response.StatusCode = -1
		pe.router.ReportConnectionFailure(baseURL)
		return nil, err
	}
	defer resp.Body.Close()

	respBodyBytes, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       string(respBodyBytes),
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("service at %s returned status %d: %s", fullURL, resp.StatusCode, string(respBodyBytes))
	}

	var result map[string]interface{}
	if len(respBodyBytes) > 0 && strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(respBodyBytes, &result); err != nil {
			return nil, fmt.Errorf("failed to unmarshal REST response from %s: %w", fullURL, err)
		}
		// ✅ FIX: Return the result directly, not wrapped
		return result, nil
	}

	// ✅ Fallback for non-JSON or empty responses
	return map[string]interface{}{}, nil

}

func (pe *PipelineExecutor) executeGraphQLRequest(url, query string, headers http.Header, stepLog *logging.PipelineStepLog, step model.PipelineStep) (*model.GraphQLResponse, error) {
	reqBodyMap := map[string]interface{}{"query": query}
	jsonBody, _ := json.Marshal(reqBodyMap)

	signature, err := pe.createRequestSignature(jsonBody)
	if err != nil {
		return nil, fmt.Errorf("failed to sign GraphQL request: %w", err)
	}
	nodeIdentity := pe.storage.GetNodeIdentity()
	headers.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
	headers.Set("X-Grapthway-Node-Address", nodeIdentity.Address)
	headers.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(nodeIdentity.PublicKey)))

	stepLog.Request = logging.RequestDetails{
		Method:  "POST",
		URL:     url,
		Headers: headers.Clone(),
		Body:    string(jsonBody),
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header = headers
	req.Header.Set("Content-Type", "application/json")

	timeout := common.ParseTimeout(step.TTL, 60*time.Second) // Default 60s
	client := common.GetHTTPClientWithTimeout(timeout)

	log.Printf("🕐 GraphQL request to %s with timeout: %v", url, timeout)
	resp, err := client.Do(req)
	if err != nil {
		stepLog.Response.StatusCode = -1
		pe.router.ReportConnectionFailure(url)
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	stepLog.Response = logging.ResponseDetails{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       string(bodyBytes),
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("service at %s returned status %d: %s", url, resp.StatusCode, string(bodyBytes))
	}

	var result model.GraphQLResponse
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return nil, err
	}
	if len(result.Errors) > 0 {
		return &result, fmt.Errorf(result.Errors[0].Message)
	}
	return &result, nil
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

func assignDataToContext(pipelineContext map[string]interface{}, assignMap map[string]string, responseData map[string]interface{}, fieldKey string) {
	for ctxKey, resKey := range assignMap {
		if resKey == "*" {
			// Assign entire response or field
			if fieldKey != "" {
				if data, ok := responseData[fieldKey]; ok {
					pipelineContext[ctxKey] = data
				}
			} else {
				pipelineContext[ctxKey] = responseData
			}
			continue
		}

		// ✅ FIX: Try root level first (REST), then nested (GraphQL)
		parts := strings.Split(resKey, ".")

		// Try direct access from root (REST responses)
		if len(parts) == 1 {
			if val, ok := responseData[resKey]; ok {
				pipelineContext[ctxKey] = val
				continue
			}
		}

		// Fallback: Try nested access for GraphQL or dot-notation paths
		var currentData interface{}
		var dataFound bool

		if fieldKey != "" {
			// GraphQL: start from the field
			currentData, dataFound = responseData[fieldKey]
		} else {
			// REST: start from root
			currentData = responseData
			dataFound = true
		}

		for _, part := range parts {
			if !dataFound {
				break
			}
			if currentMap, ok := currentData.(map[string]interface{}); ok {
				currentData, dataFound = currentMap[part]
			} else {
				dataFound = false
			}
		}

		if dataFound {
			pipelineContext[ctxKey] = currentData
		}
	}
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

func toPipelineStepLog(step model.PipelineStep) logging.PipelineStepLog {
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
