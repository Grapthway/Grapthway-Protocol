package middleware

import (
	"grapthway/pkg/crypto"
	ledgeraggregator "grapthway/pkg/economic/worker/ledger-aggregator"
	penaltybox "grapthway/pkg/economic/worker/penalty-box"
	"grapthway/pkg/ledger"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	"grapthway/pkg/p2p"
	"net/http"
)

// Chain holds configured middleware functions
type MiddlewareChain struct {
	// Single middleware wrappers
	WithEconomic  func(http.Handler) http.Handler
	WithUserAuth  func(http.Handler) http.Handler
	WithMultipart func(http.Handler) http.Handler

	// Pre-composed chains
	FullChain    func(http.Handler) http.Handler // Multipart -> UserAuth -> Economic
	SimpleChain  func(http.Handler) http.Handler // UserAuth -> Economic
	EconomicOnly func(http.Handler) http.Handler // Just Economic

	// Flexible composition
	Apply func(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler
}

// EconomicDeps holds all dependencies for economic middleware
type EconomicDeps struct {
	LedgerClient     *ledger.Client
	HardwareMonitor  *monitoring.Monitor
	P2PNode          *p2p.Node
	StorageService   model.Storage
	PenaltyBox       *penaltybox.DeveloperPenaltyBox
	NodeIdentity     *crypto.Identity
	LedgerAggregator *ledgeraggregator.LedgerAggregator
}

// NewChain creates a new middleware chain with dependencies
func NewChain(deps *EconomicDeps, multipartMiddleware func(http.Handler) http.Handler) *MiddlewareChain {
	// Single middleware wrappers
	withEconomic := func(handler http.Handler) http.Handler {
		return EconomicMiddleware(
			handler,
			deps.LedgerClient,
			deps.HardwareMonitor,
			deps.P2PNode,
			deps.StorageService,
			deps.PenaltyBox,
			deps.NodeIdentity,
			deps.LedgerAggregator,
		)
	}

	withUserAuth := func(handler http.Handler) http.Handler {
		return UserAuthMiddleware(handler)
	}

	withMultipart := func(handler http.Handler) http.Handler {
		if multipartMiddleware != nil {
			return multipartMiddleware(handler)
		}
		return handler
	}

	// Pre-composed chains
	fullChain := func(handler http.Handler) http.Handler {
		return withMultipart(
			withUserAuth(
				withEconomic(handler),
			),
		)
	}

	simpleChain := func(handler http.Handler) http.Handler {
		return withUserAuth(withEconomic(handler))
	}

	economicOnly := func(handler http.Handler) http.Handler {
		return withEconomic(handler)
	}

	// Flexible composition function
	apply := func(handler http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
		// Apply middlewares in reverse order (last wraps first)
		for i := len(middlewares) - 1; i >= 0; i-- {
			handler = middlewares[i](handler)
		}
		return handler
	}

	return &MiddlewareChain{
		// Single middleware
		WithEconomic:  withEconomic,
		WithUserAuth:  withUserAuth,
		WithMultipart: withMultipart,

		// Pre-composed chains
		FullChain:    fullChain,
		SimpleChain:  simpleChain,
		EconomicOnly: economicOnly,

		// Flexible composition
		Apply: apply,
	}
}

// Compose creates a custom middleware chain from the provided middlewares
// Middlewares are applied in order: first middleware wraps outermost
func (c *MiddlewareChain) Compose(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return c.Apply(handler, middlewares...)
	}
}
