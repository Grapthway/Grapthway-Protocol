package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"grapthway/pkg/common"
	"grapthway/pkg/crypto"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/pipeline"
	"grapthway/pkg/router"
	"grapthway/pkg/schema"
	"grapthway/pkg/util"

	json "github.com/json-iterator/go"

	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
)

type GatewayHandler struct {
	storage  model.Storage
	logger   *logging.Logger
	router   *router.ServiceRouter
	executor *pipeline.PipelineExecutor
}

// --- FIX START ---
// Update the constructor to accept the workflow engine instance instead of creating a new one.
func NewGatewayHandler(storage model.Storage, logger *logging.Logger, router *router.ServiceRouter, merger *schema.Merger, wfEngine pipeline.DurableWorkflowStarter) *GatewayHandler {
	return &GatewayHandler{
		storage:  storage,
		logger:   logger,
		router:   router,
		executor: pipeline.NewPipelineExecutor(storage, router, logger, merger, wfEngine),
	}
}

// --- FIX END ---

func (h *GatewayHandler) ExecutePostPipeline(pipeline *model.PipelineConfig, mainResolverResponse map[string]interface{}, originalContext context.Context) (map[string]interface{}, error) {
	return h.executor.ExecutePostPipeline(pipeline, mainResolverResponse, originalContext)
}

func generateTraceID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func (h *GatewayHandler) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var wg sync.WaitGroup
		logEntry := &logging.LogEntry{
			Timestamp:      time.Now(),
			LogType:        logging.GatewayLog,
			TraceID:        generateTraceID(),
			RequestAddress: r.RemoteAddr,
		}
		lrw := NewLoggingResponseWriter(w)

		defer func() {
			wg.Wait()
			logEntry.ClientResponse = logging.ResponseDetails{
				StatusCode: lrw.StatusCode,
				Headers:    lrw.Header(),
				Body:       lrw.Body.String(),
			}
			if logEntry.FailureMessage == "" && lrw.StatusCode >= 400 {
				logEntry.FailureMessage = fmt.Sprintf("Request failed with status code %d", lrw.StatusCode)
			}
			h.logger.Log(*logEntry)
		}()

		pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		var walletAddress, subgraphName string
		if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "graphql" {
			walletAddress = pathParts[0]
			subgraphName = strings.Join(pathParts[1:len(pathParts)-1], "/")
			logEntry.Subgraph = subgraphName
			logEntry.User = walletAddress // Route owner
		} else if len(pathParts) == 1 && pathParts[0] == "graphql" {
			logEntry.Subgraph = "main"
			subgraphName = "main"
		}

		var requestBody struct {
			Query     string                 `json:"query"`
			Variables map[string]interface{} `json:"variables"`
		}
		var bodyBytes []byte

		if r.Method == "GET" {
			query := r.URL.Query()
			requestBody.Query = query.Get("query")
			varsStr := query.Get("variables")
			if varsStr != "" {
				json.Unmarshal([]byte(varsStr), &requestBody.Variables)
			}
			logBodyMap := map[string]string{"query": requestBody.Query, "variables": varsStr}
			bodyBytes, _ = json.Marshal(logBodyMap)

		} else if r.Method == "POST" {
			var err error
			bodyBytes, err = io.ReadAll(r.Body)
			if err != nil {
				logEntry.FailureMessage = "Error reading request body"
				http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
				return
			}
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			if err := json.Unmarshal(bodyBytes, &requestBody); err != nil {
				if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
					logEntry.FailureMessage = "Invalid JSON in request body"
					http.Error(lrw, logEntry.FailureMessage, http.StatusBadRequest)
					return
				}
			}
		}

		logEntry.ClientRequest = logging.RequestDetails{
			Method:  r.Method,
			URL:     r.URL.String(),
			Headers: r.Header.Clone(),
			Body:    string(bodyBytes),
		}

		if files, ok := r.Context().Value("graphql_files").(map[string][]*multipart.FileHeader); ok {
			var filenames []string
			for _, fileHeaders := range files {
				for _, header := range fileHeaders {
					filenames = append(filenames, header.Filename)
				}
			}
			if len(filenames) > 0 {
				logEntry.UploadedFiles = filenames
			}
		}

		requestedFields, err := h.extractRequestedFields(requestBody.Query)
		if err != nil {
			logEntry.FailureMessage = fmt.Sprintf("Query parsing error: %v", err)
			http.Error(lrw, logEntry.FailureMessage, http.StatusBadRequest)
			return
		}

		ctx := context.WithValue(r.Context(), "headers", r.Header)
		ctx = context.WithValue(ctx, "log", logEntry)
		ctx = context.WithValue(ctx, "subgraph", logEntry.Subgraph)
		ctx = context.WithValue(ctx, "waitgroup", &wg)
		ctx = context.WithValue(ctx, "wallet_address", walletAddress)

		// Add routeOwner to context (for ledger operations and logging)
		if walletAddress != "" {
			ctx = context.WithValue(ctx, "routeOwner", walletAddress)
		}

		// FIX: Resolve the actual service developer who owns this subgraph
		var serviceDeveloperAddress string
		if subgraphName != "" {
			allServices, servicesErr := h.storage.GetAllServices()
			if servicesErr == nil {
				for compositeKey, instances := range allServices {
					if len(instances) > 0 && instances[0].Subgraph == subgraphName {
						developerAddr, _, parseErr := util.ParseCompositeKey(compositeKey)
						if parseErr == nil {
							serviceDeveloperAddress = developerAddr
							log.Printf("DEBUG: Resolved service developer for subgraph '%s': %s", subgraphName, serviceDeveloperAddress)
							break
						}
					}
				}
			}
		}

		// Add serviceDeveloper to context for middleware/workflow lookups and billing
		if serviceDeveloperAddress != "" {
			ctx = context.WithValue(ctx, "serviceDeveloper", serviceDeveloperAddress)
		}

		var pipelineResult *model.ExecutionResult
		if len(requestedFields) > 0 && serviceDeveloperAddress != "" {
			// Use serviceDeveloper address for middleware lookup
			pipelineConfig, _ := h.storage.GetMiddlewarePipeline(serviceDeveloperAddress, requestedFields[0])

			if pipelineConfig != nil {
				initialCtxData := map[string]interface{}{
					"args": requestBody.Variables,
				}

				pipelineResult, err = h.executor.ExecutePrePipeline(pipelineConfig, ctx, initialCtxData)
				if err != nil {
					logEntry.FailureMessage = err.Error()
					http.Error(lrw, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}

		r = r.WithContext(context.WithValue(ctx, "pipelineContext", pipelineResult))
		next.ServeHTTP(lrw, r)
	})
}

func (h *GatewayHandler) extractRequestedFields(query string) ([]string, error) {
	if query == "" {
		return []string{}, nil
	}
	doc, err := parser.Parse(parser.ParseParams{Source: query})
	if err != nil {
		return nil, err
	}
	fields := []string{}
	for _, def := range doc.Definitions {
		if op, ok := def.(*ast.OperationDefinition); ok {
			for _, sel := range op.SelectionSet.Selections {
				if field, ok := sel.(*ast.Field); ok {
					fields = append(fields, field.Name.Value)
				}
			}
		}
	}
	return fields, nil
}

type LoggingResponseWriter struct {
	http.ResponseWriter
	StatusCode int
	Body       *bytes.Buffer
}

func NewLoggingResponseWriter(w http.ResponseWriter) *LoggingResponseWriter {
	return &LoggingResponseWriter{ResponseWriter: w, StatusCode: http.StatusOK, Body: new(bytes.Buffer)}
}

func (lrw *LoggingResponseWriter) WriteHeader(code int) {
	lrw.StatusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *LoggingResponseWriter) Write(b []byte) (int, error) {
	lrw.Body.Write(b)
	return lrw.ResponseWriter.Write(b)
}

func MultipartMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			next.ServeHTTP(w, r)
			return
		}
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			http.Error(w, "failed to parse multipart form", http.StatusBadRequest)
			return
		}
		operationsJSON := r.FormValue("operations")
		if operationsJSON == "" {
			http.Error(w, "multipart form missing 'operations' field", http.StatusBadRequest)
			return
		}
		ctx := context.WithValue(r.Context(), "graphql_files", r.MultipartForm.File)
		r.Body = io.NopCloser(strings.NewReader(operationsJSON))
		r.Header.Set("Content-Type", "application/json")
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type RestProxyHandler struct {
	storage      model.Storage
	logger       *logging.Logger
	router       *router.ServiceRouter
	executor     *pipeline.PipelineExecutor
	nodeIdentity *crypto.Identity
}

// --- FIX START ---
// Update the constructor to accept the workflow engine instance instead of creating a new one.
func NewRestProxyHandler(storage model.Storage, logger *logging.Logger, router *router.ServiceRouter, merger *schema.Merger, wfEngine pipeline.DurableWorkflowStarter, nodeIdentity *crypto.Identity) *RestProxyHandler {
	return &RestProxyHandler{
		storage:      storage,
		logger:       logger,
		router:       router,
		executor:     pipeline.NewPipelineExecutor(storage, router, logger, merger, wfEngine),
		nodeIdentity: nodeIdentity,
	}
}

// --- FIX END ---

func (h *RestProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	allServices, err := h.storage.GetAllServices()
	if err != nil {
		http.Error(w, "Could not retrieve services", http.StatusInternalServerError)
		return
	}

	var targetInstance *router.ServiceInstance
	var matchedDevAddr string
	var matchedServiceName string
	var matchedPipeline *model.PipelineConfig
	var longestMatch string

	// IMPROVED PATH PARSING
	fullPath := strings.Trim(r.URL.Path, "/")
	pathParts := strings.Split(fullPath, "/")

	var walletAddress, requestPath string

	// Check if first segment is a wallet address
	if len(pathParts) >= 1 && strings.HasPrefix(pathParts[0], "0x") && len(pathParts[0]) == 42 {
		walletAddress = pathParts[0]
		// Rest of the path after wallet address
		if len(pathParts) > 1 {
			requestPath = "/" + strings.Join(pathParts[1:], "/")
		} else {
			requestPath = "/"
		}
	} else {
		// No wallet address in path, this shouldn't happen for REST
		log.Printf("REST: No valid wallet address found in path: %s", r.URL.Path)
		http.NotFound(w, r)
		return
	}

	log.Printf("REST: Parsed walletAddress=%s, requestPath=%s", walletAddress, requestPath)

	// Find matching REST service by longest path prefix
	for _, instances := range allServices {
		if len(instances) > 0 && instances[0].Type == "rest" && instances[0].DeveloperAddress == walletAddress {
			servicePath := instances[0].Path
			// Match if request path starts with service path
			if strings.HasPrefix(requestPath, servicePath) && len(servicePath) > len(longestMatch) {
				longestMatch = servicePath
				matchedDevAddr = instances[0].DeveloperAddress
				matchedServiceName = instances[0].Service
			}
		}
	}

	if matchedServiceName == "" {
		log.Printf("REST: No matching service found for wallet=%s, path=%s", walletAddress, requestPath)
		http.NotFound(w, r)
		return
	}

	log.Printf("REST: Matched service=%s, developer=%s, servicePath=%s", matchedServiceName, matchedDevAddr, longestMatch)

	tenantInstances, _ := h.storage.GetService(matchedDevAddr, matchedServiceName, "")
	if len(tenantInstances) > 0 {
		inst := h.router.PickInstance(tenantInstances)
		targetInstance = &inst
	}

	if targetInstance == nil {
		log.Printf("REST: No healthy instance found for service=%s", matchedServiceName)
		http.NotFound(w, r)
		return
	}

	// Get REST pipelines for this service
	restPipelines, _ := h.storage.GetRestPipelineMap(matchedDevAddr, matchedServiceName)

	// Build route keys with actual request path (not service path)
	// The pipeline keys should match what's in registry.go
	routeKeys := []string{
		// Exact match: "GET /api/v1/profile"
		r.Method + " " + requestPath,
		// Without query params: "GET /api/v1/profile"
		r.Method + " " + strings.Split(requestPath, "?")[0],
	}

	// Try to find a matching pipeline
	for _, routeKey := range routeKeys {
		if pipeline, exists := restPipelines[routeKey]; exists {
			matchedPipeline = pipeline
			log.Printf("REST: Matched pipeline for route: %s", routeKey)
			break
		}
	}

	// Continue with existing prefix matching logic...
	if matchedPipeline == nil {
		cleanPath := strings.Split(requestPath, "?")[0]
		for routeKey, pipeline := range restPipelines {
			parts := strings.SplitN(routeKey, " ", 2)
			if len(parts) == 2 && parts[0] == r.Method {
				pattern := parts[1]
				if strings.HasPrefix(cleanPath, pattern) {
					matchedPipeline = pipeline
					log.Printf("REST: Matched pipeline via prefix for route: %s", routeKey)
					break
				}
			}
		}
	}

	if matchedPipeline == nil {
		log.Printf("REST: No pipeline found for %s %s, using fast proxy", r.Method, requestPath)
		h.fastProxy(w, r, targetInstance, requestPath)
		return
	}

	log.Printf("REST: Using pipelined proxy for %s %s", r.Method, requestPath)
	h.pipelinedProxy(w, r, targetInstance, *matchedPipeline, walletAddress, requestPath)
}

func (h *RestProxyHandler) fastProxy(w http.ResponseWriter, r *http.Request, targetInstance *router.ServiceInstance, requestPath string) {
	var wg sync.WaitGroup

	logEntry := logging.LogEntry{
		Timestamp:      time.Now(),
		LogType:        logging.GatewayLog,
		TraceID:        generateTraceID(),
		RequestAddress: r.RemoteAddr,
		Subgraph:       targetInstance.Subgraph,
		User:           targetInstance.DeveloperAddress,
	}
	lrw := NewLoggingResponseWriter(w)

	defer func() {
		wg.Wait()
		logEntry.ClientResponse = logging.ResponseDetails{
			StatusCode: lrw.StatusCode,
			Headers:    lrw.Header(),
			Body:       lrw.Body.String(),
		}
		if logEntry.FailureMessage == "" && lrw.StatusCode >= 400 {
			logEntry.FailureMessage = fmt.Sprintf("Request failed with status code %d", lrw.StatusCode)
		}
		h.logger.Log(logEntry)
	}()

	bodyBytes, bodyReadErr := io.ReadAll(r.Body)
	if bodyReadErr != nil {
		logEntry.FailureMessage = "Failed to read request body: " + bodyReadErr.Error()
		http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
		return
	}
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	logEntry.ClientRequest = logging.RequestDetails{
		Method:  r.Method,
		URL:     r.URL.String(),
		Headers: r.Header.Clone(),
		Body:    string(bodyBytes),
	}

	targetUrl, err := url.Parse(targetInstance.URL)
	if err != nil {
		logEntry.FailureMessage = "Invalid target URL for service: " + targetInstance.Service
		http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(targetUrl)

	// ✅ USE SHARED TRANSPORT with 30s timeout
	proxy.Transport = common.GetTransportWithTimeout(30 * time.Second)
	originalDirector := proxy.Director

	servicePath := targetInstance.Path
	backendPath := strings.TrimPrefix(requestPath, servicePath)
	if backendPath == "" {
		backendPath = "/"
	}

	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = targetUrl.Host
		req.URL.Path = backendPath
		req.URL.RawQuery = r.URL.RawQuery // ✅ CRITICAL: Preserve query params

		req.Header = r.Header.Clone()

		requestHash := sha256.Sum256(bodyBytes)
		signature, err := h.nodeIdentity.Sign(requestHash[:])
		if err == nil {
			req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
			req.Header.Set("X-Grapthway-Node-Address", h.nodeIdentity.Address)
			req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(h.nodeIdentity.PublicKey)))
		}
		fmt.Printf("Proxy outgoing headers: %+v\n", req.Header)
	}

	downstreamURL := targetUrl.String() + backendPath
	if r.URL.RawQuery != "" {
		downstreamURL += "?" + r.URL.RawQuery
	}

	log.Printf("REST FastProxy: Forwarding to %s", downstreamURL)
	logEntry.DownstreamRequest = logging.RequestDetails{
		Method:  r.Method,
		URL:     downstreamURL,
		Headers: r.Header.Clone(),
		Body:    string(bodyBytes),
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		respBodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading downstream response body: %v", err)
		}
		resp.Body.Close()
		resp.Body = io.NopCloser(bytes.NewBuffer(respBodyBytes))

		logEntry.DownstreamResponse = logging.ResponseDetails{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header.Clone(),
			Body:       string(respBodyBytes),
		}
		return nil
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logEntry.FailureMessage = "Proxy error: " + err.Error()
		h.router.ReportConnectionFailure(targetInstance.URL)
		lrw.WriteHeader(http.StatusBadGateway)
		lrw.Write([]byte("Proxy Error: " + err.Error()))
	}

	proxy.ServeHTTP(lrw, r)
}

func (h *RestProxyHandler) pipelinedProxy(w http.ResponseWriter, r *http.Request, targetInstance *router.ServiceInstance, pipeline model.PipelineConfig, walletAddress, requestPath string) {
	var wg sync.WaitGroup

	logEntry := &logging.LogEntry{
		Timestamp:      time.Now(),
		LogType:        logging.GatewayLog,
		TraceID:        generateTraceID(),
		RequestAddress: r.RemoteAddr,
		Subgraph:       targetInstance.Subgraph,
		User:           walletAddress,
	}
	lrw := NewLoggingResponseWriter(w)

	defer func() {
		wg.Wait()
		logEntry.ClientResponse = logging.ResponseDetails{
			StatusCode: lrw.StatusCode,
			Headers:    lrw.Header(),
			Body:       lrw.Body.String(),
		}
		if logEntry.FailureMessage == "" && lrw.StatusCode >= 400 {
			logEntry.FailureMessage = fmt.Sprintf("Request failed with status code %d", lrw.StatusCode)
		}
		h.logger.Log(*logEntry)
	}()

	bodyBytes, bodyReadErr := io.ReadAll(r.Body)
	if bodyReadErr != nil {
		logEntry.FailureMessage = "Failed to read request body: " + bodyReadErr.Error()
		http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
		return
	}
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	logEntry.ClientRequest = logging.RequestDetails{
		Method:  r.Method,
		URL:     r.URL.String(),
		Headers: r.Header.Clone(),
		Body:    string(bodyBytes),
	}

	ctx := context.WithValue(r.Context(), "headers", r.Header)
	ctx = context.WithValue(ctx, "log", logEntry)
	ctx = context.WithValue(ctx, "waitgroup", &wg)
	ctx = context.WithValue(ctx, "wallet_address", walletAddress)
	ctx = context.WithValue(ctx, "routeOwner", walletAddress)

	serviceDeveloper := targetInstance.DeveloperAddress
	if serviceDeveloper != "" {
		ctx = context.WithValue(ctx, "serviceDeveloper", serviceDeveloper)
		log.Printf("REST Pipeline: Set serviceDeveloper to %s for billing", serviceDeveloper)
	}

	initialCtxData := make(map[string]interface{})

	// ✅ Add query parameters to pipeline context
	if r.URL.RawQuery != "" {
		queryParams := r.URL.Query()
		queryParamsMap := make(map[string]interface{})
		for key, values := range queryParams {
			if len(values) == 1 {
				queryParamsMap[key] = values[0]
			} else {
				queryParamsMap[key] = values
			}
		}
		initialCtxData["query"] = queryParamsMap
	}

	var requestBodyData map[string]interface{}
	if len(bodyBytes) > 0 {
		if json.Unmarshal(bodyBytes, &requestBodyData) == nil {
			initialCtxData["request"] = map[string]interface{}{
				"body": requestBodyData,
			}
		}
	}

	pipelineResult, err := h.executor.ExecutePrePipeline(&pipeline, ctx, initialCtxData)
	if err != nil {
		logEntry.FailureMessage = "Pre-pipeline execution failed: " + err.Error()
		http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
		return
	}

	targetUrl, _ := url.Parse(targetInstance.URL)

	servicePath := targetInstance.Path
	backendPath := strings.TrimPrefix(requestPath, servicePath)
	if backendPath == "" {
		backendPath = "/"
	}

	log.Printf("REST PipelinedProxy: servicePath=%s, requestPath=%s, backendPath=%s",
		servicePath, requestPath, backendPath)

	// ✅ Construct URL with query params
	downstreamURL := targetUrl.String() + backendPath
	if r.URL.RawQuery != "" {
		downstreamURL += "?" + r.URL.RawQuery
	}

	downstreamReq, err := http.NewRequest(r.Method, downstreamURL, bytes.NewBuffer(bodyBytes))

	if err != nil {
		logEntry.FailureMessage = "Failed to create downstream request: " + err.Error()
		http.Error(lrw, logEntry.FailureMessage, http.StatusInternalServerError)
		return
	}

	downstreamReq.Header = r.Header.Clone()

	for key, val := range pipelineResult.Context {
		headerKey := "X-Ctx-" + key
		switch v := val.(type) {
		case string:
			downstreamReq.Header.Set(headerKey, v)
		case fmt.Stringer:
			downstreamReq.Header.Set(headerKey, v.String())
		case int, int64, float32, float64:
			downstreamReq.Header.Set(headerKey, fmt.Sprintf("%v", v))
		}
	}

	requestHash := sha256.Sum256(bodyBytes)
	signature, err := h.nodeIdentity.Sign(requestHash[:])
	if err == nil {
		downstreamReq.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
		downstreamReq.Header.Set("X-Grapthway-Node-Address", h.nodeIdentity.Address)
		downstreamReq.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(h.nodeIdentity.PublicKey)))
	}

	logEntry.DownstreamRequest = logging.RequestDetails{
		Method:  downstreamReq.Method,
		URL:     downstreamReq.URL.String(),
		Headers: downstreamReq.Header.Clone(),
		Body:    string(bodyBytes),
	}

	client := common.GetHTTPClientWithTimeout(30 * time.Second)
	resp, err := client.Do(downstreamReq)
	if err != nil {
		logEntry.FailureMessage = "Downstream request failed: " + err.Error()
		h.router.ReportConnectionFailure(targetInstance.URL)
		http.Error(lrw, logEntry.FailureMessage, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	logEntry.DownstreamResponse = logging.ResponseDetails{
		StatusCode: resp.StatusCode,
		Headers:    resp.Header.Clone(),
		Body:       string(respBody),
	}

	var mainResponseData map[string]interface{}
	finalResponseData := respBody

	postCtx := context.WithValue(ctx, "pipelineContext", pipelineResult)
	allPostSteps := pipeline.Post
	if pipelineResult != nil {
		allPostSteps = append(allPostSteps, pipelineResult.PostSteps...)
	}

	if resp.StatusCode < 400 && len(allPostSteps) > 0 && strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		json.Unmarshal(respBody, &mainResponseData)

		postExecutionConfig := &model.PipelineConfig{Post: allPostSteps}

		enrichedResponse, postErr := h.executor.ExecutePostPipeline(postExecutionConfig, mainResponseData, postCtx)
		if postErr != nil {
			log.Printf("Post-pipeline execution error: %v", postErr)
		}
		finalResponseData, _ = json.Marshal(enrichedResponse)
	}

	for key, values := range resp.Header {
		for _, value := range values {
			lrw.Header().Add(key, value)
		}
	}
	lrw.WriteHeader(resp.StatusCode)
	lrw.Write(finalResponseData)
}
