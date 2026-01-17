package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "go.uber.org/automaxprocs"

	"github.com/dgraph-io/badger/v4"
	jsoniter "github.com/json-iterator/go"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/handler"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"grapthway/pkg/common"
	"grapthway/pkg/config"
	"grapthway/pkg/crypto"
	"grapthway/pkg/dependency"
	"grapthway/pkg/dht"
	ledgeraggregator "grapthway/pkg/economic/worker/ledger-aggregator"
	penaltybox "grapthway/pkg/economic/worker/penalty-box"
	"grapthway/pkg/gateway"
	"grapthway/pkg/ledger"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	"grapthway/pkg/p2p"
	"grapthway/pkg/pipeline"
	"grapthway/pkg/rest"
	"grapthway/pkg/rest/middleware"
	"grapthway/pkg/router"
	"grapthway/pkg/schema"
	"grapthway/pkg/tls"
	"grapthway/pkg/util"
	"grapthway/pkg/workflow"
)

var (
	cfg                    *config.Config
	storageService         model.Storage
	schemaMerger           *schema.Merger
	logger                 *logging.Logger
	nodeIdentity           *crypto.Identity
	p2pNode                *p2p.Node
	dhtService             *dht.DHT
	ledgerClient           *ledger.Client
	startTime              time.Time
	upgrader               = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }, Subprotocols: []string{"graphql-ws"}}
	gatewayHandler         *gateway.GatewayHandler
	restProxy              *gateway.RestProxyHandler
	hardwareMonitor        *monitoring.Monitor
	networkHardwareMonitor *monitoring.NetworkHardwareMonitor
	workflowEngine         *workflow.Engine
	pipelineExecutor       *pipeline.PipelineExecutor
	ledgerAggregator       *ledgeraggregator.LedgerAggregator
	db                     *badger.DB
	penaltyBox             *penaltybox.DeveloperPenaltyBox
)

const (
	internalGrapthwayServiceName = "grapthway-internal-api"
	systemDeveloperAddress       = "grapthway-system"
	TxBatchSize                  = 10000
	TxBatchMaxDelay              = 10 * time.Second
)

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK, new(bytes.Buffer)}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	lrw.body.Write(b)
	return lrw.ResponseWriter.Write(b)
}

func gatewayLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" || websocket.IsWebSocketUpgrade(r) {
			next.ServeHTTP(w, r)
			return
		}

		var logType logging.LogType
		path := r.URL.Path
		if strings.HasPrefix(path, "/public/") {
			next.ServeHTTP(w, r)
			return
		} else if strings.HasPrefix(path, "/admin/") {
			logType = logging.AdminLog
		} else {
			logType = logging.GatewayLog
		}

		startTime := time.Now()

		bodyBytes, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = fmt.Sprintf("trace-%d", time.Now().UnixNano())
		}

		logEntry := logging.LogEntry{
			Timestamp:      startTime,
			LogType:        logType,
			TraceID:        traceID,
			RequestAddress: r.RemoteAddr,
			User:           r.Header.Get("X-Grapthway-Developer-ID"),
			ClientRequest: logging.RequestDetails{
				Method:  r.Method,
				URL:     r.URL.String(),
				Headers: r.Header.Clone(),
				Body:    string(bodyBytes),
			},
		}

		lrw := newLoggingResponseWriter(w)
		ctx := context.WithValue(r.Context(), "log", &logEntry)

		next.ServeHTTP(lrw, r.WithContext(ctx))

		logEntry.ClientResponse = logging.ResponseDetails{
			StatusCode: lrw.statusCode,
			Headers:    lrw.Header(),
			Body:       lrw.body.String(),
		}

		logEntry.Sanitize()
		logger.Log(logEntry)
	})
}

// statefulTopics are required for all nodes to maintain a consistent view of services, pipelines, and workflows.
var statefulTopics = map[string]bool{
	gateway.ConfigUpdateTopic:  true,
	p2p.StateRequestTopic:      true,
	p2p.StateResponseTopic:     true,
	p2p.DHTRequestTopic:        true,
	p2p.DHTResponseTopic:       true,
	p2p.InstanceUpdateTopic:    true,
	p2p.LedgerTransactionTopic: true,
	p2p.LedgerRequestTopic:     true,
	p2p.HardwareStatsTopic:     true,
	workflow.DurableTaskTopic:  true,
}

// consensusTopics are only for server nodes participating in block production and validation.
var consensusTopics = map[string]bool{
	p2p.BlockProposalTopic:    true,
	p2p.BlockShredTopic:       true,
	p2p.PreValidatedTxTopic:   true,
	p2p.NonceUpdateTopic:      true,
	p2p.BlockPreProposalTopic: true,
	p2p.ValidatorUpdateTopic:  true,
}

func subscribeToAllTopics(node *p2p.Node) (map[string]<-chan *pubsub.Message, error) {
	isWorker := node.Role == "worker"
	statelessTopics := []string{
		p2p.RecoveryProposalTopic,
	}

	allTopics := make([]string, 0, len(statefulTopics)+len(statelessTopics)+len(consensusTopics))
	for topic := range statefulTopics {
		allTopics = append(allTopics, topic)
	}
	allTopics = append(allTopics, statelessTopics...)

	// Only subscribe to consensus topics if the node is a "server".
	if !isWorker {
		for topic := range consensusTopics {
			allTopics = append(allTopics, topic)
		}
	} else {
		log.Println("NODE ROLE: Worker. Skipping subscription to consensus-related topics.")
	}

	subscriptions := make(map[string]<-chan *pubsub.Message)
	for _, topicName := range allTopics {
		waitForSync := true
		channel, err := node.Subscribe(topicName, waitForSync)
		if err != nil {
			return nil, fmt.Errorf("FATAL: Could not subscribe to topic %s: %w", topicName, err)
		}
		subscriptions[topicName] = channel
	}
	return subscriptions, nil
}

func getDynamicBadgerOptions(dbPath string) badger.Options {
	numCPU := runtime.GOMAXPROCS(0)
	if numCPU == 0 {
		numCPU = 1 // Ensure at least 1
	}

	// Base values for a low-resource environment
	baseIndexCacheSize := 64 << 20 // 64 MB
	baseMemTableSize := 64 << 20   // 64 MB
	baseNumCompactors := 4
	baseNumMemtables := 5

	// Scale resources with available CPUs
	// More CPUs can handle more concurrent compactions and memtables.
	// We also cautiously increase cache sizes, assuming more CPUs implies more RAM.
	opts := badger.DefaultOptions(dbPath).
		WithIndexCacheSize(int64(baseIndexCacheSize + (numCPU * 16 << 20))). // Add 16MB per CPU
		WithMemTableSize(int64(baseMemTableSize + (numCPU * 16 << 20))).     // Add 16MB per CPU
		WithNumCompactors(baseNumCompactors + numCPU).
		WithNumMemtables(baseNumMemtables + numCPU).
		WithValueLogFileSize(1 << 30) // 1 GB value log file size remains a reasonable default

	log.Printf("⚙️  Configuring BadgerDB with dynamic options for %d CPUs:", numCPU)
	log.Printf("    - NumCompactors: %d", baseNumCompactors+numCPU)
	log.Printf("    - NumMemtables: %d", baseNumMemtables+numCPU)
	log.Printf("    - IndexCacheSize: %d MB", (baseIndexCacheSize+(numCPU*16<<20))/(1024*1024))
	log.Printf("    - MemTableSize: %d MB", (baseMemTableSize+(numCPU*16<<20))/(1024*1024))

	return opts
}

func main() {
	cfg = config.NewConfig()
	log.Printf("🚀 Starting Grapthway Protocol Node")
	startTime = time.Now()
	common.GetHTTPClient()

	log.Printf("🛡️  Rate limiting enabled: %.2f requests/second with a burst of %d per developer.", middleware.RateLimitRPS, middleware.RateLimitBurst)
	log.Printf("💰 Staking requirement set to %.2f GCU per CPU core.", middleware.GCUPerCore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize TLS Manager
	tlsManager := tls.NewManager(tls.Config{
		Enabled:     cfg.TLSEnabled,
		Domains:     cfg.TLSDomains,
		Email:       cfg.TLSEmail,
		CertDir:     cfg.TLSCertDir,
		RenewalDays: 10,
		Staging:     cfg.TLSStaging,
	})

	if tlsManager != nil {
		tlsManager.StartAutoRenewal(ctx)
	}

	var err error
	dbPath := "/data/badger"
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Fatalf("🚨 FATAL: Failed to create BadgerDB directory: %v", err)
	}

	opts := getDynamicBadgerOptions(dbPath)

	db, err = badger.Open(opts)
	if err != nil {
		log.Fatalf("🚨 FATAL: Could not open BadgerDB: %v", err)
	}
	defer db.Close()

	if cfg.NodeOperatorPrivateKey != "" {
		nodeIdentity, err = crypto.IdentityFromPrivateKeyHex(cfg.NodeOperatorPrivateKey)
		if err != nil {
			log.Fatalf("🚨 FATAL: Could not load identity from private key: %v", err)
		}
		log.Println("🆔 Loaded Node Operator wallet from private key.")
	} else {
		nodeIdentity, err = crypto.NewIdentity()
		if err != nil {
			log.Fatalf("🚨 FATAL: Could not create temporary node identity: %v", err)
		}
		log.Println("⚠️ WARNING: No private key provided. Created a temporary, ephemeral wallet for this session.")
		log.Printf("🔑 Temporary Private Key (save this for persistence): %s", hex.EncodeToString(nodeIdentity.PrivateKey.D.Bytes()))
	}
	log.Printf("🆔 Node Operator (Owner) Address: %s", nodeIdentity.Address)

	libp2pPrivKey, err := nodeIdentity.ToLibp2pPrivateKey()
	if err != nil {
		log.Fatalf("🚨 FATAL: Could not convert identity to libp2p private key: %v", err)
	}

	go func() {
		var m runtime.MemStats
		for {
			time.Sleep(5 * time.Second)
			runtime.ReadMemStats(&m)
			if m.Alloc > 1.5*1024*1024*1024 {
				log.Printf("MEMORY_WARNING: High memory usage detected: %v MB", m.Alloc/1024/1024)
			}
		}
	}()

	logger = logging.NewLogger(cfg.LogRetentionDays)
	go logger.StartCleanupRoutine()
	hardwareMonitor = monitoring.NewMonitor(2 * time.Second)
	dhtService = dht.NewDHT(ctx)

	networkHardwareMonitor = monitoring.NewNetworkHardwareMonitor(ctx)

	ledgerClient = ledger.NewClient(ctx, db, networkHardwareMonitor, hardwareMonitor)
	penaltyBox = penaltybox.NewDeveloperPenaltyBox(ctx, ledgerClient)
	penaltyBox.StartRecheckRoutine()

	ledgerAggregator = ledgeraggregator.NewLedgerAggregator(ctx, ledgerClient, penaltyBox, TxBatchSize, TxBatchMaxDelay, nodeIdentity)
	ledgerAggregator.Start()

	log.Println("MAIN: Waiting for initial hardware monitor collection...")
	time.Sleep(3 * time.Second)
	if hardwareMonitor.GetHardwareInfo().CgroupLimits.CPUQuotaCores <= 0 {
		log.Println("MAIN: WARNING: Initial hardware collection did not yield valid CPU data. Node may be rejected by peers.")
	} else {
		log.Println("MAIN: Initial hardware collection successful.")
	}

	nodeRole := os.Getenv("GRAPTHWAY_ROLE")
	if nodeRole != "worker" {
		nodeRole = "server"
	}

	p2pNode, err = p2p.NewNode(ctx, cfg.BootstrapPeers, libp2pPrivKey, nodeIdentity.Address, ledgerClient, hardwareMonitor, middleware.GCUPerCore, networkHardwareMonitor, nodeIdentity, nodeRole)
	if err != nil {
		log.Fatalf("🚨 FATAL: Could not start P2P node: %v", err)
	}
	log.Printf("🆔 Node Operational Peer ID: %s", p2pNode.Host.ID().String())
	log.Printf("🆔 Node Role: %s", p2pNode.Role)

	topicChannels, err := subscribeToAllTopics(p2pNode)
	if err != nil {
		log.Fatalf(err.Error())
	}

	var preProposalChan, blockShredChan, validatorUpdateChan <-chan *pubsub.Message
	if !isWorker(nodeRole) {
		preProposalChan = topicChannels[p2p.BlockPreProposalTopic]
		blockShredChan = topicChannels[p2p.BlockShredTopic]
		validatorUpdateChan = topicChannels[p2p.ValidatorUpdateTopic]
	}

	ledgerClient.StartNetworking(
		p2pNode,
		topicChannels[p2p.LedgerTransactionTopic],
		topicChannels[p2p.LedgerRequestTopic],
		topicChannels[p2p.BlockProposalTopic],
		blockShredChan,
		topicChannels[p2p.PreValidatedTxTopic],
		topicChannels[p2p.NonceUpdateTopic],
		preProposalChan,
		validatorUpdateChan,
	)
	networkHardwareMonitor.Start(p2pNode.DHT, topicChannels[p2p.HardwareStatsTopic])

	schemaUpdateChan := make(chan string, 10)
	storageService = gateway.NewDHTStorage(ctx, p2pNode, dhtService, topicChannels, schemaUpdateChan, ledgerClient)

	go broadcastHardwareStats(ctx)

	if isWorker(nodeRole) {
		log.Println("⚙️ Starting grapthway in WORKER mode.")
		worker := workflow.NewWorker(ctx, storageService, logger, p2pNode, dhtService, nodeIdentity, topicChannels[workflow.DurableTaskTopic])
		worker.Start()
		select {} // Block indefinitely as a worker
	}

	log.Println("📡 Starting grapthway in SERVER mode.")

	workflowEngine = workflow.NewEngine(storageService, p2pNode, dhtService, topicChannels[p2p.RecoveryProposalTopic])
	schemaMerger = schema.NewMerger(storageService, nodeIdentity)
	serviceRouter := router.NewServiceRouter()
	pipelineExecutor = pipeline.NewPipelineExecutor(storageService, serviceRouter, logger, schemaMerger, workflowEngine)
	schemaMerger.SetPostPipelineExecutor(pipelineExecutor)

	go watchForSchemaUpdates(ctx, schemaUpdateChan)
	go watchForReconciliationTriggers(ctx)

	log.Println("SYNC: Requesting current network state from peers...")
	storageService.RequestNetworkState()
	time.Sleep(5 * time.Second)
	log.Println("SYNC: Initial state synchronization complete. Starting server...")

	gatewayHandler = gateway.NewGatewayHandler(storageService, logger, serviceRouter, schemaMerger, workflowEngine)
	restProxy = gateway.NewRestProxyHandler(storageService, logger, serviceRouter, schemaMerger, workflowEngine, nodeIdentity)

	r := mux.NewRouter()
	r.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	r.StrictSlash(true)

	deps := dependency.NewDependencies(
		ledgerClient,
		hardwareMonitor,
		networkHardwareMonitor,
		p2pNode,
		storageService,
		penaltyBox,
		nodeIdentity,
		ledgerAggregator,
		logger,
		dhtService,
		workflowEngine,
		startTime,
		gateway.MultipartMiddleware,
	)

	// Pass single deps object to router
	rest.Router(r, deps)

	r.HandleFunc("/admin/logs/live", liveLogHandler).Methods("GET")

	adminRouter := r.PathPrefix("/admin").Subrouter()
	adminRouter.Use(adminAuthMiddleware)
	adminRouter.HandleFunc("/logs/{log_type}", getAdminLogsHandler).Methods("GET", "OPTIONS")
	adminRouter.HandleFunc("/workflows/monitoring", getAdminWorkflowsHandler).Methods("GET", "OPTIONS")
	adminRouter.HandleFunc("/wallet/create", createWalletHandler).Methods("POST", "OPTIONS")

	r.HandleFunc("/schema-introspection", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		introspectionQuery := `
		query IntrospectionQuery {
			__schema {
				queryType { name }
				mutationType { name }
				subscriptionType { name }
				types {
					kind
					name
					description
					fields(includeDeprecated: true) {
						name
						description
						type {
							kind
							name
							ofType {
								kind
								name
							}
						}
					}
				}
			}
		}`
		subgraphName := r.URL.Query().Get("subgraph_name")
		if subgraphName == "" {
			subgraphName = "main"
		}
		result := graphql.Do(graphql.Params{
			Schema:        *schemaMerger.GetSchema(subgraphName),
			RequestString: introspectionQuery,
		})
		jsoniter.NewEncoder(w).Encode(result)
	}).Methods("GET")

	internalRouter := r.PathPrefix("/admin/internal").Subrouter()
	internalRouter.HandleFunc("/execute-workflow", executeWorkflowHandler).Methods("POST")

	unifiedGraphQLHandler := func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		walletAddress := vars["wallet_address"]
		subgraphName := vars["subgraph_name"]
		if websocket.IsWebSocketUpgrade(r) {
			websocketHandler(w, r, walletAddress, subgraphName)
			return
		}
		if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			http.Error(w, "SSE not yet implemented", http.StatusNotImplemented)
			return
		}
		processGraphQLRequest(w, r, walletAddress, subgraphName)
	}

	r.Handle("/{wallet_address}/{subgraph_name}/graphql",
		deps.MiddlewareChain.FullChain(gatewayHandler.Middleware(http.HandlerFunc(unifiedGraphQLHandler))),
	).Methods("POST", "GET", "OPTIONS")

	r.Handle("/graphql",
		deps.MiddlewareChain.FullChain(gatewayHandler.Middleware(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				processGraphQLRequest(w, r, "", "main")
			}),
		)),
	).Methods("POST", "GET", "OPTIONS")

	// Clean REST proxy routes
	r.PathPrefix("/{wallet_address}/").Handler(
		deps.MiddlewareChain.SimpleChain(http.HandlerFunc(restProxy.ServeHTTP)),
	).Methods("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS")

	r.PathPrefix("/").Handler(
		deps.MiddlewareChain.SimpleChain(http.HandlerFunc(restProxy.ServeHTTP)),
	)

	go selfAnnounce(storageService)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for range ticker.C {
			cleanupStaleServices(60 * time.Second)
		}
	}()

	// Apply CORS middleware
	corsOpts := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization", "X-Requested-With", "Accept", "Origin", "Upgrade", "X-Grapthway-Developer-ID", "X-Grapthway-Admin-Address", "X-Grapthway-Admin-Signature", "X-Grapthway-User-Address", "X-Grapthway-User-PublicKey", "X-Grapthway-User-Signature", "Idempotency-Key"}),
		handlers.AllowCredentials(),
	)

	// Create the final HTTP handler with middleware chain
	finalHandler := corsOpts(gatewayLoggingMiddleware(r))

	// Wrap with Let's Encrypt HTTP-01 challenge handler if TLS is enabled
	if tlsManager != nil {
		finalHandler = tlsManager.HTTPHandler(finalHandler)
	}

	// Start servers based on TLS configuration
	if tlsManager != nil {
		// TLS enabled: start both HTTP (for redirects and ACME) and HTTPS
		go startHTTPServer(cfg.HTTPPort, finalHandler)
		startHTTPSServer(cfg.HTTPSPort, finalHandler, tlsManager)
	} else {
		// TLS disabled: run legacy HTTP-only server
		port := os.Getenv("PORT")
		if port == "" {
			port = "5000"
		}
		log.Printf("Server running on port %s (HTTP only)", port)
		log.Fatal(http.ListenAndServe(":"+port, finalHandler))
	}
}

func startHTTPServer(port string, handler http.Handler) {
	log.Printf("HTTP server running on port %s (ACME challenges + redirects)", port)

	redirectHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Let ACME challenges pass through
		if strings.HasPrefix(r.URL.Path, "/.well-known/acme-challenge/") {
			handler.ServeHTTP(w, r)
			return
		}

		// Redirect everything else to HTTPS
		target := "https://" + r.Host + r.RequestURI
		http.Redirect(w, r, target, http.StatusMovedPermanently)
	})

	if err := http.ListenAndServe(":"+port, redirectHandler); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func startHTTPSServer(port string, handler http.Handler, tlsManager *tls.Manager) {
	log.Printf("HTTPS server running on port %s with automatic Let's Encrypt", port)

	server := &http.Server{
		Addr:      ":" + port,
		Handler:   handler,
		TLSConfig: tlsManager.GetTLSConfig(),
	}

	// ListenAndServeTLS with empty cert paths uses TLSConfig.GetCertificate
	if err := server.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("HTTPS server failed: %v", err)
	}
}

func isWorker(role string) bool {
	return role == "worker"
}

func watchForSchemaUpdates(ctx context.Context, updateChan <-chan string) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Schema update watcher shutting down.")
			return
		case serviceName := <-updateChan:
			log.Printf("MAIN: Received schema update signal for service '%s'. Reloading merger.", serviceName)
			schemaMerger.ReloadSchema()
			log.Printf("MAIN: Triggering blue-green cleanup for service '%s'.", serviceName)
			cleanupStaleServicesByVersion(serviceName)
		}
	}
}

func watchForReconciliationTriggers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p2pNode.ReconciliationTrigger:
			log.Println("MAIN: Reconciliation triggered by new peer connection.")
			storageService.RequestNetworkState()
			ledgerClient.ProactiveReconcile()
		}
	}
}

func adminAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		adminAddress := r.Header.Get("X-Grapthway-Admin-Address")
		signatureHex := r.Header.Get("X-Grapthway-Admin-Signature")

		if adminAddress != nodeIdentity.Address {
			http.Error(w, "Address does not match this node's operator", http.StatusForbidden)
			return
		}

		signature, err := hex.DecodeString(signatureHex)
		if err != nil {
			http.Error(w, "Invalid signature format", http.StatusBadRequest)
			return
		}

		hash := sha256.Sum256([]byte(adminAddress))
		if !crypto.VerifySignature(hash[:], signature, nodeIdentity.PublicKey) {
			log.Printf("[AUTH-FAIL] Admin signature failed for address %s", adminAddress)
			http.Error(w, "Invalid signature for node operator", http.StatusForbidden)
			return
		}

		log.Printf("AUTH: Admin request successfully verified from %s", adminAddress)
		next.ServeHTTP(w, r)
	})
}

func createWalletHandler(w http.ResponseWriter, r *http.Request) {
	identity, err := ledgerClient.CreateNewWallet()
	if err != nil {
		http.Error(w, "Failed to create wallet: "+err.Error(), http.StatusInternalServerError)
		return
	}
	response := map[string]interface{}{
		"address":    identity.Address,
		"publicKey":  crypto.PublicKeyToHex(identity.PublicKey),
		"privateKey": hex.EncodeToString(identity.PrivateKey.D.Bytes()),
		"message":    "Wallet created successfully and registered on the ledger. Secure your private key!",
	}
	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(response)
}

func getAdminLogsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	logTypeStr, ok := vars["log_type"]
	if !ok {
		http.Error(w, "Log type not specified", http.StatusBadRequest)
		return
	}

	var logType logging.LogType
	switch logTypeStr {
	case "gateway":
		logType = logging.GatewayLog
	case "schema":
		logType = logging.SchemaLog
	case "admin":
		logType = logging.AdminLog
	case "ledger":
		logType = logging.LedgerLog
	default:
		http.Error(w, "Invalid log type requested", http.StatusBadRequest)
		return
	}
	getLogsByType(w, r, logType)
}

func selfAnnounce(storage model.Storage) {
	selfURL := os.Getenv("SELF_URL")
	if selfURL == "" {
		log.Println("WARN: SELF_URL not set for gateway server.")
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	for ; ; <-ticker.C {
		storage.UpdateServiceRegistration(
			systemDeveloperAddress,
			internalGrapthwayServiceName,
			selfURL,
			"internal",
			"",
			"rest",
			"/admin/internal",
		)
	}
}

func executeWorkflowHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		DeveloperAddress string                 `json:"developerAddress"`
		WorkflowName     string                 `json:"workflowName"`
		Context          map[string]interface{} `json:"context"`
		TraceID          string                 `json:"traceId"`
		Headers          http.Header            `json:"headers"`
	}
	if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.DeveloperAddress == "" {
		http.Error(w, "DeveloperAddress is required", http.StatusBadRequest)
		return
	}
	targetWorkflow, err := storageService.GetMiddlewarePipeline(payload.DeveloperAddress, payload.WorkflowName)
	if err != nil || targetWorkflow == nil {
		http.Error(w, "Workflow not found", http.StatusNotFound)
		return
	}
	logEntry := &logging.LogEntry{
		Timestamp:      time.Now(),
		LogType:        logging.GatewayLog,
		TraceID:        payload.TraceID,
		RequestAddress: "internal-worker-request",
	}
	ctx := context.WithValue(context.Background(), "log", logEntry)
	ctx = context.WithValue(ctx, "headers", payload.Headers)
	ctx = context.WithValue(ctx, "developerID", payload.DeveloperAddress)
	result, err := pipelineExecutor.ExecutePrePipeline(targetWorkflow, ctx, payload.Context)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(result)
}

func getAdminWorkflowsHandler(w http.ResponseWriter, r *http.Request) {
	if workflowEngine == nil {
		http.Error(w, "Durable workflow engine is not enabled on this node", http.StatusServiceUnavailable)
		return
	}
	liveInstances := workflowEngine.GetActiveInstances()
	checkpointedData := dhtService.GetAllWithPrefix("workflow:instance:")
	allInstancesMap := make(map[string]*workflow.Instance)
	for _, instanceBytes := range checkpointedData {
		var instance workflow.Instance
		if err := jsoniter.Unmarshal(instanceBytes, &instance); err == nil {
			if existing, ok := allInstancesMap[instance.ID]; !ok || instance.Version > existing.Version {
				allInstancesMap[instance.ID] = &instance
			}
		}
	}
	for _, instance := range liveInstances {
		allInstancesMap[instance.ID] = instance
	}
	allInstances := make([]*workflow.Instance, 0, len(allInstancesMap))
	for _, instance := range allInstancesMap {
		allInstances = append(allInstances, instance)
	}
	filters := map[string]string{
		"startDate":    r.URL.Query().Get("startDate"),
		"endDate":      r.URL.Query().Get("endDate"),
		"workflowName": r.URL.Query().Get("workflowName"),
		"status":       r.URL.Query().Get("status"),
		"serviceName":  r.URL.Query().Get("serviceName"),
	}
	var filteredInstances []*workflow.Instance
	for _, instance := range allInstances {
		if matchFilter(instance, filters) {
			filteredInstances = append(filteredInstances, instance)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(filteredInstances)
}

func matchFilter(instance *workflow.Instance, filter map[string]string) bool {
	if name, ok := filter["workflowName"]; ok && name != "" && !strings.Contains(instance.WorkflowName, name) {
		return false
	}
	if status, ok := filter["status"]; ok && status != "" && string(instance.Status) != status {
		return false
	}
	if serviceName, ok := filter["serviceName"]; ok && serviceName != "" && !strings.Contains(instance.AssociatedService, serviceName) {
		return false
	}
	if startDate, ok := filter["startDate"]; ok && startDate != "" {
		if t, err := time.Parse(time.RFC3339, startDate); err == nil {
			if instance.CreatedAt.Before(t) {
				return false
			}
		}
	}
	if endDate, ok := filter["endDate"]; ok && endDate != "" {
		if t, err := time.Parse(time.RFC3339, endDate); err == nil {
			if instance.CreatedAt.After(t) {
				return false
			}
		}
	}
	return true
}

func broadcastHardwareStats(ctx context.Context) {
	time.Sleep(10 * time.Second)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			broadcast()
		case <-ctx.Done():
			return
		}
	}
}

func broadcast() {
	if hardwareMonitor == nil || p2pNode == nil || networkHardwareMonitor == nil {
		return
	}
	localStats := hardwareMonitor.GetHardwareInfo()
	localStats.NodeID = p2pNode.Host.ID().String()
	localStats.Role = os.Getenv("GRAPTHWAY_ROLE")
	if localStats.Role == "" {
		localStats.Role = "server"
	}
	knownPeers := networkHardwareMonitor.GetPeerStats()
	gossipPayload := model.HardwareStatsGossip{
		SenderStats: localStats,
		KnownPeers:  knownPeers,
	}
	statsBytes, err := jsoniter.Marshal(gossipPayload)
	if err != nil {
		return
	}
	p2pNode.Gossip(p2p.HardwareStatsTopic, statsBytes)
}

func getLogsByType(w http.ResponseWriter, r *http.Request, logType logging.LogType) {
	start, end, maxStr := r.URL.Query().Get("start"), r.URL.Query().Get("end"), r.URL.Query().Get("max")
	var max int
	if maxStr != "" {
		var err error
		if max, err = strconv.Atoi(maxStr); err != nil {
			http.Error(w, "Invalid 'max' parameter", http.StatusBadRequest)
			return
		}
	}
	logs, err := logger.GetLogs(logType, start, end, max)
	if err != nil {
		http.Error(w, "Failed to retrieve logs", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(logs)
}

func processGraphQLRequest(w http.ResponseWriter, r *http.Request, walletAddress, subgraphName string) {
	currentSchema := schemaMerger.GetSchema(subgraphName)
	if currentSchema == nil {
		http.Error(w, "Schema not found or not ready for subgraph: "+subgraphName, http.StatusServiceUnavailable)
		return
	}

	ctx := context.WithValue(r.Context(), "wallet_address", walletAddress)

	h := handler.New(&handler.Config{
		Schema:   currentSchema,
		Pretty:   true,
		GraphiQL: false,
	})
	h.ServeHTTP(w, r.WithContext(ctx))
}

func websocketHandler(w http.ResponseWriter, r *http.Request, walletAddress, subgraphName string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	currentSchema := schemaMerger.GetSchema(walletAddress)
	if currentSchema == nil {
		return
	}
	conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"connection_ack"}`))
	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var msg map[string]interface{}
		if err := jsoniter.Unmarshal(p, &msg); err != nil {
			continue
		}
		msgType, _ := msg["type"].(string)
		switch msgType {
		case "start", "subscribe":
			payload, _ := msg["payload"].(map[string]interface{})
			query, _ := payload["query"].(string)
			variables, _ := payload["variables"].(map[string]interface{})
			ctx := context.WithValue(r.Context(), "wallet_address", walletAddress)
			ctx = context.WithValue(ctx, "subgraph", subgraphName)
			result := graphql.Do(graphql.Params{
				Schema:         *currentSchema,
				RequestString:  query,
				VariableValues: variables,
				Context:        ctx,
			})
			response, _ := jsoniter.Marshal(map[string]interface{}{"type": "data", "id": msg["id"], "payload": result})
			if err := conn.WriteMessage(messageType, response); err != nil {
				break
			}
		case "stop", "complete":
		case "connection_init":
			continue
		}
	}
}

func liveLogHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade to websocket: %v", err)
		return
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		log.Println("Live log client failed to send auth message in time.")
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Authentication timeout"))
		return
	}

	var authPayload struct {
		Type      string `json:"type"`
		Address   string `json:"address"`
		Signature string `json:"signature"`
	}

	if err := jsoniter.Unmarshal(msg, &authPayload); err != nil || authPayload.Type != "auth" {
		log.Println("Invalid auth message from live log client.")
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Invalid auth message"))
		return
	}

	signatureBytes, err := hex.DecodeString(authPayload.Signature)
	if err != nil {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Invalid signature format"))
		return
	}

	hash := sha256.Sum256([]byte(authPayload.Address))

	if authPayload.Address != nodeIdentity.Address || !crypto.VerifySignature(hash[:], signatureBytes, nodeIdentity.PublicKey) {
		log.Printf("[AUTH-FAIL] Live log signature verification failed for address %s", authPayload.Address)
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Authentication failed"))
		return
	}

	log.Printf("[AUTH-SUCCESS] Live log client authenticated: %s", authPayload.Address)
	conn.SetReadDeadline(time.Time{})

	logger.AddLiveClient(conn)
	defer logger.RemoveLiveClient(conn)

	for {
		if _, _, err := conn.NextReader(); err != nil {
			break
		}
	}
}

func cleanupStaleServicesByVersion(serviceName string) {
	log.Printf("CLEANUP: Stale service cleanup by version needs review for composite key model.")
}

func cleanupStaleServices(ttl time.Duration) {
	services, _ := storageService.GetAllServices()
	now := time.Now()
	needsReload := false
	for compositeKey, instances := range services {
		devAddr, serviceName, err := util.ParseCompositeKey(compositeKey)
		if err != nil {
			continue
		}
		if serviceName == internalGrapthwayServiceName {
			continue
		}
		var activeInstances []router.ServiceInstance
		for _, instance := range instances {
			if now.Sub(instance.LastSeen) <= ttl {
				activeInstances = append(activeInstances, instance)
			}
		}
		if len(activeInstances) < len(instances) {
			var removedGraphQL bool
			for _, instance := range instances {
				if !containsInstance(activeInstances, instance) && instance.Type == "graphql" {
					removedGraphQL = true
					break
				}
			}
			if removedGraphQL {
				needsReload = true
			}
			if len(activeInstances) == 0 {
				log.Printf("CLEANUP: Service '%s' has no active instances (TTL: %v). Removing service configuration.", compositeKey, ttl)
				storageService.RemoveService(devAddr, serviceName)
			} else {
				storageService.UpdateServiceInstances(devAddr, serviceName, activeInstances)
			}
		}
	}
	if needsReload {
		schemaMerger.ReloadSchema()
	}
}

func containsInstance(slice []router.ServiceInstance, item router.ServiceInstance) bool {
	for _, s := range slice {
		if s.URL == item.URL && s.Service == item.Service {
			return true
		}
	}
	return false
}
