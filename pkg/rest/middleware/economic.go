package middleware

import (
	"bytes"
	"context"
	"grapthway/pkg/crypto"
	ledgeraggregator "grapthway/pkg/economic/worker/ledger-aggregator"
	penaltybox "grapthway/pkg/economic/worker/penalty-box"
	"grapthway/pkg/ledger"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	"grapthway/pkg/p2p"
	"grapthway/pkg/util"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/time/rate"
)

var (
	limiterMutex                 = &sync.Mutex{}
	GCUPerCore                   = 100.0
	developerRateLimiters        = make(map[string]*rate.Limiter)
	RateLimitRPS                 = 10.0
	RateLimitBurst               = 20
	requestComputeCost    uint64 = 1000
)

func EconomicMiddleware(
	next http.Handler,
	ledgerClient *ledger.Client,
	hardwareMonitor *monitoring.Monitor,
	p2pNode *p2p.Node,
	storageService model.Storage,
	penaltyBox *penaltybox.DeveloperPenaltyBox,
	nodeIdentity *crypto.Identity,
	ledgerAggregator *ledgeraggregator.LedgerAggregator,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		// Check node stake
		hwInfo := hardwareMonitor.GetHardwareInfo()
		requiredStakeFloat := hwInfo.CgroupLimits.CPUQuotaCores * GCUPerCore
		if requiredStakeFloat > 0 {
			requiredStake := uint64(requiredStakeFloat * model.GCU_MICRO_UNIT)

			var allocatedStake uint64
			validators := ledgerClient.GetValidatorSet()
			found := false
			for _, v := range validators {
				if v.PeerID == p2pNode.Host.ID() {
					allocatedStake = v.TotalStake
					found = true
					break
				}
			}

			if allocatedStake < requiredStake {
				log.Printf("WARN: Node is understaked (Required: %d, Has: %d, Found in validator set: %v). Rejecting request.", requiredStake, allocatedStake, found)
				http.Error(w, "Service Unavailable: Node is not sufficiently staked", http.StatusServiceUnavailable)
				return
			}
		}

		// Skip introspection queries
		if strings.Contains(r.URL.Path, "graphql") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				var reqBody struct {
					OperationName string `json:"operationName"`
				}
				if jsoniter.Unmarshal(bodyBytes, &reqBody) == nil && reqBody.OperationName == "IntrospectionQuery" {
					next.ServeHTTP(w, r)
					return
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		}

		// Determine route owner and service developer
		pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		var routeOwner string
		var serviceDeveloper string

		// 1. GraphQL routes: /{wallet}/{subgraph}/graphql
		if len(pathParts) >= 3 && pathParts[len(pathParts)-1] == "graphql" && strings.HasPrefix(pathParts[0], "0x") {
			routeOwner = pathParts[0]
			subgraphName := strings.Join(pathParts[1:len(pathParts)-1], "/")

			// Find the service that handles this subgraph
			allServices, err := storageService.GetAllServices()
			if err == nil {
				for compositeKey, instances := range allServices {
					if len(instances) > 0 && instances[0].Subgraph == subgraphName {
						developerAddr, _, parseErr := util.ParseCompositeKey(compositeKey)
						if parseErr == nil {
							serviceDeveloper = developerAddr
							log.Printf("ECONOMIC: Resolved GraphQL service developer '%s' for subgraph '%s'", serviceDeveloper, subgraphName)
							break
						}
					}
				}
			}
		} else if len(pathParts) >= 2 && strings.HasPrefix(pathParts[0], "0x") && !strings.HasPrefix(r.URL.Path, "/api/v1") {
			// 2. REST routes: /{wallet}/{...path}
			routeOwner = pathParts[0]
			requestPath := "/" + strings.Join(pathParts[1:], "/")

			// Find REST service by path matching
			allServices, err := storageService.GetAllServices()
			if err == nil {
				var longestMatch string
				for compositeKey, instances := range allServices {
					if len(instances) > 0 && instances[0].Type == "rest" && instances[0].DeveloperAddress == routeOwner {
						if strings.HasPrefix(requestPath, instances[0].Path) && len(instances[0].Path) > len(longestMatch) {
							longestMatch = instances[0].Path
							developerAddr, _, _ := util.ParseCompositeKey(compositeKey)
							serviceDeveloper = developerAddr
							log.Printf("ECONOMIC: Resolved REST service developer '%s' for path '%s'", serviceDeveloper, requestPath)
						}
					}
				}
			}
		} else if strings.HasPrefix(r.URL.Path, "/api/v1") {
			// 3. Direct /api/v1 calls (user is both route owner and service developer)
			authenticatedUser, ok := r.Context().Value("authenticatedUser").(string)
			if ok && authenticatedUser != "" {
				routeOwner = authenticatedUser
				serviceDeveloper = authenticatedUser
				log.Printf("ECONOMIC: Direct /api/v1 call, user '%s' is both route owner and service developer", authenticatedUser)
			}
		} else if strings.HasPrefix(r.URL.Path, "/public") || strings.HasPrefix(r.URL.Path, "/admin") {
			// 4. Public/admin endpoints - no billing
			next.ServeHTTP(w, r)
			return
		}

		// Validate addresses
		if routeOwner == "" {
			log.Printf("ECONOMIC: No route owner found for path: %s", r.URL.Path)
			http.Error(w, "Missing route owner or authentication", http.StatusForbidden)
			return
		}

		// CRITICAL: Use serviceDeveloper for billing if found, otherwise fall back to routeOwner
		billingTarget := serviceDeveloper
		if billingTarget == "" {
			billingTarget = routeOwner
			log.Printf("ECONOMIC: No service developer found, using route owner for billing: %s", routeOwner)
		} else {
			log.Printf("ECONOMIC: Billing service developer: %s (route owner: %s)", billingTarget, routeOwner)
		}

		// Check penalty box for the billing target (service developer)
		if penaltyBox.IsPenalized(billingTarget) {
			http.Error(w, "Service developer has insufficient GCU balance. Service unavailable.", http.StatusPaymentRequired)
			return
		}

		// Rate limit by billing target
		limiterMutex.Lock()
		limiter, exists := developerRateLimiters[billingTarget]
		if !exists {
			limiter = rate.NewLimiter(rate.Limit(RateLimitRPS), RateLimitBurst)
			developerRateLimiters[billingTarget] = limiter
		}
		limiterMutex.Unlock()
		if !limiter.Allow() {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		// Create fee info - charge the SERVICE DEVELOPER, not the route owner
		feeInfo := model.FeeInfo{
			DeveloperID:    billingTarget, // Service developer gets charged
			NodeOperatorID: nodeIdentity.Address,
			Cost:           requestComputeCost,
		}

		// In economic middleware
		if !ledgerAggregator.SubmitFee(feeInfo) {
			log.Printf("WARN: Ledger aggregator channel is full. Dropping fee info for %s.", billingTarget)
			http.Error(w, "Server too busy, please try again", http.StatusServiceUnavailable)
			return
		}

		// Add BOTH routeOwner and serviceDeveloper to context
		ctx := context.WithValue(r.Context(), model.FeeInfoKey, feeInfo)
		ctx = context.WithValue(ctx, "routeOwner", routeOwner)
		if serviceDeveloper != "" {
			ctx = context.WithValue(ctx, "serviceDeveloper", serviceDeveloper)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
