package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
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

	"github.com/gorilla/websocket"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/parser"
)

// ─── GatewayHandler (GraphQL middleware) ─────────────────────────────────────

type GatewayHandler struct {
	storage  model.Storage
	logger   *logging.Logger
	router   *router.ServiceRouter
	executor *pipeline.PipelineExecutor
}

func NewGatewayHandler(storage model.Storage, logger *logging.Logger, router *router.ServiceRouter, merger *schema.Merger, wfEngine pipeline.DurableWorkflowStarter) *GatewayHandler {
	return &GatewayHandler{
		storage:  storage,
		logger:   logger,
		router:   router,
		executor: pipeline.NewPipelineExecutor(storage, router, logger, merger, wfEngine),
	}
}

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
			logEntry.User = walletAddress
		} else if len(pathParts) == 1 && pathParts[0] == "graphql" {
			logEntry.Subgraph = "main"
			subgraphName = "main"
		}

		var requestBody struct {
			Query     string                 `json:"query"`
			Variables map[string]interface{} `json:"variables"`
		}
		var bodyBytes []byte

		// For GraphQL middleware we only need to parse GET and POST bodies.
		// Other HTTP methods (PATCH, PUT, DELETE, etc.) are REST requests routed
		// through RestProxyHandler and never reach this middleware as GraphQL.
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
		} else {
			// For non-GET/POST methods reaching the GraphQL middleware, read the
			// body for logging but do not attempt GraphQL parsing.
			bodyBytes, _ = io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
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

		if walletAddress != "" {
			ctx = context.WithValue(ctx, "routeOwner", walletAddress)
		}

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

		if serviceDeveloperAddress != "" {
			ctx = context.WithValue(ctx, "serviceDeveloper", serviceDeveloperAddress)
		}

		var pipelineResult *model.ExecutionResult
		if len(requestedFields) > 0 && serviceDeveloperAddress != "" {
			pipelineConfig, _ := h.storage.GetMiddlewarePipeline(serviceDeveloperAddress, requestedFields[0])

			if pipelineConfig != nil {
				initialCtxData := map[string]interface{}{
					// GraphQL variables exposed as "args" for $args.field path interpolation
					"args": requestBody.Variables,
					// Also expose variables under request.body for $request.body.field interpolation
					"request": map[string]interface{}{
						"body": requestBody.Variables,
					},
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

// ─── LoggingResponseWriter ────────────────────────────────────────────────────

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

// Hijack lets httputil.ReverseProxy tunnel a WebSocket connection through the
// LoggingResponseWriter wrapper. Without this, the reverse proxy cannot obtain
// the raw TCP conn and the WebSocket handshake fails.
func (lrw *LoggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := lrw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not support hijacking")
	}
	return hijacker.Hijack()
}

// ─── RestProxyHandler ─────────────────────────────────────────────────────────

type RestProxyHandler struct {
	storage      model.Storage
	logger       *logging.Logger
	router       *router.ServiceRouter
	executor     *pipeline.PipelineExecutor
	nodeIdentity *crypto.Identity
}

func NewRestProxyHandler(storage model.Storage, logger *logging.Logger, router *router.ServiceRouter, merger *schema.Merger, wfEngine pipeline.DurableWorkflowStarter, nodeIdentity *crypto.Identity) *RestProxyHandler {
	return &RestProxyHandler{
		storage:      storage,
		logger:       logger,
		router:       router,
		executor:     pipeline.NewPipelineExecutor(storage, router, logger, merger, wfEngine),
		nodeIdentity: nodeIdentity,
	}
}

// ─── matchRoutePattern ────────────────────────────────────────────────────────
//
// Returns true when actualPath matches patternPath where any segment in
// patternPath that starts with ':' is treated as a wildcard.
//
// Examples:
//
//	matchRoutePattern("/product-service/api/admin/tiers/abc-123",
//	                  "/product-service/api/admin/tiers/:tier_id")  → true
//
//	matchRoutePattern("/product-service/api/admin/tiers",
//	                  "/product-service/api/admin/tiers/:tier_id")  → false  (segment count differs)
//
//	matchRoutePattern("/product-service/api/tiers",
//	                  "/product-service/api/tiers")                 → true   (exact)
func matchRoutePattern(actualPath, patternPath string) bool {
	// Fast path: exact match (covers routes without params).
	if actualPath == patternPath {
		return true
	}

	actualSegs := strings.Split(strings.Trim(actualPath, "/"), "/")
	patternSegs := strings.Split(strings.Trim(patternPath, "/"), "/")

	if len(actualSegs) != len(patternSegs) {
		return false
	}

	for i, pat := range patternSegs {
		if strings.HasPrefix(pat, ":") {
			// Wildcard segment — matches any non-empty value.
			if actualSegs[i] == "" {
				return false
			}
			continue
		}
		if pat != actualSegs[i] {
			return false
		}
	}
	return true
}

// ─── matchPipeline ────────────────────────────────────────────────────────────
//
// Finds the best-matching pipeline for (method, path) from the registered map.
//
// Match priority (highest to lowest):
//  1. Exact match:      "PATCH /foo/bar/abc"  == key "PATCH /foo/bar/abc"
//  2. Param match:      "PATCH /foo/bar/abc"  matches key "PATCH /foo/bar/:id"
//  3. Prefix match:     "GET /foo/bar/abc"    HasPrefix of key "GET /foo/bar"
//     (retained for backward compat with wildcard-suffix registrations)
//
// The longest matching pattern wins in cases 2 and 3 to prevent a short prefix
// from shadowing a more specific parameterised route.
func matchPipeline(method, requestPath string, restPipelines model.RestPipelineMap) *model.PipelineConfig {
	cleanPath := strings.Split(requestPath, "?")[0]

	// ── Pass 1: exact match (no query string) ────────────────────────────────
	for _, candidate := range []string{
		method + " " + requestPath,
		method + " " + cleanPath,
	} {
		if p, ok := restPipelines[candidate]; ok {
			log.Printf("REST: Matched pipeline (exact) for route: %s", candidate)
			return p
		}
	}

	// ── Pass 2: param-pattern match ──────────────────────────────────────────
	// Walk every registered key; keep the one whose pattern is longest
	// (most specific) among all matches.
	var bestParam *model.PipelineConfig
	bestParamLen := -1

	for routeKey, p := range restPipelines {
		parts := strings.SplitN(routeKey, " ", 2)
		if len(parts) != 2 || parts[0] != method {
			continue
		}
		pattern := parts[1]
		if matchRoutePattern(cleanPath, pattern) {
			if len(pattern) > bestParamLen {
				bestParamLen = len(pattern)
				cp := p // copy before taking address
				bestParam = cp
				log.Printf("REST: Matched pipeline (param-pattern) for route: %s", routeKey)
			}
		}
	}
	if bestParam != nil {
		return bestParam
	}

	// ── Pass 3: prefix match (legacy fallback) ───────────────────────────────
	// Kept so that registrations like "GET /api" still catch "GET /api/any/sub"
	// when no more-specific rule is registered.
	var bestPrefix *model.PipelineConfig
	bestPrefixLen := -1

	for routeKey, p := range restPipelines {
		parts := strings.SplitN(routeKey, " ", 2)
		if len(parts) != 2 || parts[0] != method {
			continue
		}
		prefix := parts[1]
		if strings.HasPrefix(cleanPath, prefix) && len(prefix) > bestPrefixLen {
			bestPrefixLen = len(prefix)
			cp := p
			bestPrefix = cp
			log.Printf("REST: Matched pipeline (prefix) for route: %s", routeKey)
		}
	}
	return bestPrefix
}

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

	fullPath := strings.Trim(r.URL.Path, "/")
	pathParts := strings.Split(fullPath, "/")

	var walletAddress, requestPath string

	if len(pathParts) >= 1 && strings.HasPrefix(pathParts[0], "0x") && len(pathParts[0]) == 42 {
		walletAddress = pathParts[0]
		if len(pathParts) > 1 {
			requestPath = "/" + strings.Join(pathParts[1:], "/")
		} else {
			requestPath = "/"
		}
	} else {
		log.Printf("REST: No valid wallet address found in path: %s", r.URL.Path)
		http.NotFound(w, r)
		return
	}

	log.Printf("REST: Parsed walletAddress=%s, requestPath=%s", walletAddress, requestPath)

	for _, instances := range allServices {
		if len(instances) > 0 && instances[0].Type == "rest" && instances[0].DeveloperAddress == walletAddress {
			servicePath := instances[0].Path
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

	restPipelines, _ := h.storage.GetRestPipelineMap(matchedDevAddr, matchedServiceName)

	// Use the unified param-aware pipeline matcher for ALL HTTP methods.
	matchedPipeline = matchPipeline(r.Method, requestPath, restPipelines)

	if matchedPipeline == nil {
		log.Printf("REST: No pipeline found for %s %s, using fast proxy", r.Method, requestPath)
		h.fastProxy(w, r, targetInstance, requestPath)
		return
	}

	// WebSocket upgrades must use the WS-aware pipelined proxy.
	// pipelinedProxy uses http.NewRequest + http.Client which drops the Upgrade
	// header and makes hijacking impossible.
	if websocket.IsWebSocketUpgrade(r) {
		log.Printf("REST: WebSocket upgrade detected for %s %s — using WS-aware pipelined proxy", r.Method, requestPath)
		h.wsPipelinedProxy(w, r, targetInstance, *matchedPipeline, walletAddress, requestPath)
		return
	}

	log.Printf("REST: Using pipelined proxy for %s %s", r.Method, requestPath)
	h.pipelinedProxy(w, r, targetInstance, *matchedPipeline, walletAddress, requestPath)
}

// ─── fastProxy ────────────────────────────────────────────────────────────────

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
		req.URL.RawQuery = r.URL.RawQuery

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

	// Do NOT install ModifyResponse for WebSocket upgrades — it buffers the
	// entire response body which prevents the reverse proxy from hijacking the
	// connection for the WS tunnel.
	if !websocket.IsWebSocketUpgrade(r) {
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
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logEntry.FailureMessage = "Proxy error: " + err.Error()
		h.router.ReportConnectionFailure(targetInstance.URL)
		lrw.WriteHeader(http.StatusBadGateway)
		lrw.Write([]byte("Proxy Error: " + err.Error()))
	}

	proxy.ServeHTTP(lrw, r)
}

// ─── pipelinedProxy (REST) ────────────────────────────────────────────────────

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
		// Also expose as "args" so pipeline steps can use $args.field in path templates
		initialCtxData["args"] = queryParamsMap
	}

	var requestBodyData map[string]interface{}
	if len(bodyBytes) > 0 {
		if json.Unmarshal(bodyBytes, &requestBodyData) == nil {
			initialCtxData["request"] = map[string]interface{}{
				"body": requestBodyData,
			}
			// Also merge body fields directly into "args" if it's a JSON body POST/PUT/PATCH/etc.
			// so $args.field resolves from body fields when no query string is present.
			if initialCtxData["args"] == nil {
				initialCtxData["args"] = requestBodyData
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

	// Start from original client headers, then layer pipeline context (X-Ctx-*)
	// and node identity headers on top.
	downstreamReq.Header = r.Header.Clone()
	h.injectPipelineHeaders(downstreamReq.Header, pipelineResult)

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

// ─── wsPipelinedProxy ─────────────────────────────────────────────────────────
//
// Behaves identically to pipelinedProxy for the pre-pipeline phase:
//   - promotes ?token query param → Authorization header (browser WS cannot
//     send custom headers during the upgrade handshake)
//   - builds the same context values (headers, log, waitgroup, wallet_address,
//     routeOwner, serviceDeveloper)
//   - runs the pre-pipeline so all PassHeaders (e.g. Authorization) reach the
//     auth service and all pipeline-assigned X-Ctx-* values are produced
//   - forwards the WebSocket upgrade via httputil.ReverseProxy (which supports
//     http.Hijacker) with the full effective header set:
//     original client headers + X-Ctx-* from pipeline + node identity headers
//
// Post-pipeline is intentionally skipped: WebSocket is a full-duplex stream
// with no single response body to enrich.
func (h *RestProxyHandler) wsPipelinedProxy(w http.ResponseWriter, r *http.Request, targetInstance *router.ServiceInstance, pipeline model.PipelineConfig, walletAddress, requestPath string) {
	var wg sync.WaitGroup

	logEntry := &logging.LogEntry{
		Timestamp:      time.Now(),
		LogType:        logging.GatewayLog,
		TraceID:        generateTraceID(),
		RequestAddress: r.RemoteAddr,
		Subgraph:       targetInstance.Subgraph,
		User:           walletAddress,
	}

	defer func() {
		// WebSocket connections have no conventional response body to log.
		logEntry.ClientResponse = logging.ResponseDetails{StatusCode: http.StatusSwitchingProtocols}
		if logEntry.FailureMessage != "" {
			logEntry.ClientResponse.StatusCode = http.StatusBadGateway
		}
		h.logger.Log(*logEntry)
	}()

	// ── 1. Effective upgrade headers ─────────────────────────────────────────
	//
	// Browsers cannot set the Authorization header on a WebSocket upgrade
	// handshake. Any browser client (including the Grapthway terminal HTML)
	// therefore passes the JWT as ?token=<value>. We promote it to the
	// Authorization header here — before the pre-pipeline runs — so that
	// preparePipelineHeaders in executor.go correctly picks it up via
	// PassHeaders: ["Authorization"] and forwards it to the auth service.
	//
	// We work on a clone so the original *http.Request is never mutated.
	// All downstream operations (context, proxy director) use upgradeHeaders.
	upgradeHeaders := r.Header.Clone()
	if upgradeHeaders.Get("Authorization") == "" {
		if token := r.URL.Query().Get("token"); token != "" {
			if !strings.HasPrefix(strings.ToLower(token), "bearer ") {
				token = "Bearer " + token
			}
			upgradeHeaders.Set("Authorization", token)
			log.Printf("WS: promoted ?token → Authorization for wallet=%s path=%s", walletAddress, requestPath)
		}
	}

	// Log the effective headers (with Authorization resolved).
	logEntry.ClientRequest = logging.RequestDetails{
		Method:  r.Method,
		URL:     r.URL.String(),
		Headers: upgradeHeaders.Clone(),
		Body:    "", // WS upgrade carries no body
	}

	// ── 2. Pipeline context — mirrors pipelinedProxy exactly ─────────────────
	//
	// "headers" must be upgradeHeaders (not r.Header) so that executor.go:993
	//   originalHeaders.Get("Authorization")
	// returns the token when building the auth service request.
	ctx := context.WithValue(r.Context(), "headers", upgradeHeaders)
	ctx = context.WithValue(ctx, "log", logEntry)
	ctx = context.WithValue(ctx, "waitgroup", &wg)
	ctx = context.WithValue(ctx, "wallet_address", walletAddress)
	ctx = context.WithValue(ctx, "routeOwner", walletAddress)

	serviceDeveloper := targetInstance.DeveloperAddress
	if serviceDeveloper != "" {
		ctx = context.WithValue(ctx, "serviceDeveloper", serviceDeveloper)
		log.Printf("WS Pipeline: Set serviceDeveloper to %s for billing", serviceDeveloper)
	}

	// ── 3. Initial pipeline context data ─────────────────────────────────────
	//
	// WS upgrades carry no body, but query params are available.
	// We expose them under both "query" (legacy) and "args" so that
	// $args.<field> path interpolation works identically to REST/GraphQL.
	initialCtxData := make(map[string]interface{})
	if r.URL.RawQuery != "" {
		queryParamsMap := make(map[string]interface{})
		for key, values := range r.URL.Query() {
			if len(values) == 1 {
				queryParamsMap[key] = values[0]
			} else {
				queryParamsMap[key] = values
			}
		}
		initialCtxData["query"] = queryParamsMap
		// Also expose as "args" so pipeline steps can use $args.field
		initialCtxData["args"] = queryParamsMap
	}

	// ── 4. Run pre-pipeline ───────────────────────────────────────────────────
	//
	// This is identical to pipelinedProxy. The pipeline calls the auth service
	// (or any configured pre-step), collects the results (e.g. user object,
	// valid flag) into pipelineResult.Context, which injectPipelineHeaders will
	// later serialise as X-Ctx-User, X-Ctx-Valid, etc. on the downstream request.
	pipelineResult, err := h.executor.ExecutePrePipeline(&pipeline, ctx, initialCtxData)
	if err != nil {
		logEntry.FailureMessage = "WS pre-pipeline failed: " + err.Error()
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// ── 5. Resolve downstream target ─────────────────────────────────────────
	targetUrl, err := url.Parse(targetInstance.URL)
	if err != nil {
		logEntry.FailureMessage = "Invalid target URL: " + err.Error()
		http.Error(w, "invalid backend URL", http.StatusInternalServerError)
		return
	}

	servicePath := targetInstance.Path
	backendPath := strings.TrimPrefix(requestPath, servicePath)
	if backendPath == "" {
		backendPath = "/"
	}

	log.Printf("WS PipelinedProxy: servicePath=%s, requestPath=%s, backendPath=%s",
		servicePath, requestPath, backendPath)

	// ── 6. Forward via httputil.ReverseProxy (supports http.Hijacker) ────────
	//
	// Unlike http.NewRequest + http.Client (used by pipelinedProxy for REST),
	// httputil.ReverseProxy detects the Upgrade header and switches to tunnel
	// mode via http.Hijacker, completing the WebSocket handshake end-to-end.
	proxy := httputil.NewSingleHostReverseProxy(targetUrl)
	proxy.Transport = common.GetTransportWithTimeout(0) // 0 = no timeout for WS streams

	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = targetUrl.Scheme
		req.URL.Host = targetUrl.Host
		req.Host = targetUrl.Host
		req.URL.Path = backendPath
		req.URL.RawQuery = r.URL.RawQuery

		// Base: upgradeHeaders — contains all original client headers
		// (Upgrade, Connection, Sec-WebSocket-Key/Version/Extensions, etc.)
		// PLUS the Authorization header (either original or promoted from ?token).
		req.Header = upgradeHeaders.Clone()

		// Layer 1: X-Ctx-* headers from the pre-pipeline result.
		// e.g. X-Ctx-User (serialised JSON of the user object from auth service)
		//      X-Ctx-Valid (boolean string "true"/"false")
		// server-agent's VerifyUserAccess middleware reads exactly these headers.
		h.injectPipelineHeaders(req.Header, pipelineResult)

		// Layer 2: Node identity headers (signature over empty body for WS).
		emptyHash := sha256.Sum256([]byte{})
		signature, err := h.nodeIdentity.Sign(emptyHash[:])
		if err == nil {
			req.Header.Set("X-Grapthway-Signature", hex.EncodeToString(signature))
			req.Header.Set("X-Grapthway-Node-Address", h.nodeIdentity.Address)
			req.Header.Set("X-Grapthway-Node-Public-Key", hex.EncodeToString(crypto.FromECDSAPub(h.nodeIdentity.PublicKey)))
		}

		logEntry.DownstreamRequest = logging.RequestDetails{
			Method:  req.Method,
			URL:     req.URL.String(),
			Headers: req.Header.Clone(),
		}
	}

	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logEntry.FailureMessage = "WS proxy error: " + err.Error()
		h.router.ReportConnectionFailure(targetInstance.URL)
		http.Error(w, "WebSocket proxy error: "+err.Error(), http.StatusBadGateway)
	}

	// LoggingResponseWriter forwards Hijack() so wrapping is safe here.
	lrw := NewLoggingResponseWriter(w)
	proxy.ServeHTTP(lrw, r)
}

// ─── injectPipelineHeaders ────────────────────────────────────────────────────
//
// Serialises every value in pipelineResult.Context into an X-Ctx-<key> header
// on the provided header map. This is the mechanism by which pipeline output
// (e.g. authenticated user object, valid flag) reaches the downstream service.
//
// Maps and slices are JSON-encoded so complex objects survive the header wire
// format intact (server-agent's VerifyUserAccess uses json.Unmarshal on them).
func (h *RestProxyHandler) injectPipelineHeaders(headers http.Header, result *model.ExecutionResult) {
	if result == nil {
		return
	}
	for key, val := range result.Context {
		headerKey := "X-Ctx-" + key
		var headerVal string
		switch v := val.(type) {
		case string:
			headerVal = v
		case bool:
			headerVal = fmt.Sprintf("%v", v)
		case int, int64, float32, float64:
			headerVal = fmt.Sprintf("%v", v)
		case fmt.Stringer:
			headerVal = v.String()
		default:
			if jsonVal, err := json.Marshal(v); err == nil {
				headerVal = string(jsonVal)
			} else {
				headerVal = fmt.Sprintf("%v", v)
			}
		}
		if headerVal != "" {
			headers.Set(headerKey, headerVal)
		}
	}
}
