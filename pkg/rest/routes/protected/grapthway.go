package protected

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/rest/handler"
	"grapthway/pkg/rest/middleware"
	"net/http"

	"github.com/gorilla/mux"
)

func GrapthwayProtectedRoutes(
	router *mux.Router,
	deps *dependency.Dependencies,
) {
	router.Use(middleware.UserAuthMiddleware)

	router.Handle("/wallet/transfer",
		deps.MiddlewareChain.WithEconomic(http.HandlerFunc(handler.UserTransferHandler(deps))),
	).Methods("POST", "OPTIONS")

	// Loggging route
	router.HandleFunc("/logs/{log_type}", handler.GetUserLogsHandler(deps)).Methods("GET", "OPTIONS")

	// Gateway routes
	router.Handle("/workflows/monitoring", handler.GetUserWorkflowsHandler(deps)).Methods("GET", "OPTIONS")

	router.Handle("/publish-config",
		deps.MiddlewareChain.WithEconomic(handler.PublishConfigHandler(deps)),
	).Methods("POST", "OPTIONS")

	router.Handle("/health",
		deps.MiddlewareChain.WithEconomic(handler.HealthHandler(deps)),
	).Methods("POST", "OPTIONS")

	// Allowance routes
	router.Handle("/wallet/allowance/set",
		deps.MiddlewareChain.WithEconomic(handler.SetAllowanceHandler(deps)),
	).Methods("POST", "OPTIONS")

	router.Handle("/wallet/allowance/remove",
		deps.MiddlewareChain.WithEconomic(handler.RemoveAllowanceHandler(deps)),
	).Methods("POST", "OPTIONS")

	// Staking routes
	router.Handle("/staking/deposit",
		deps.MiddlewareChain.WithEconomic(handler.StakeDepositHandler(deps)),
	).Methods("POST", "OPTIONS")

	router.Handle("/staking/withdraw",
		deps.MiddlewareChain.WithEconomic(handler.StakeWithdrawalHandler(deps)),
	).Methods("POST", "OPTIONS")

	router.Handle("/staking/assign",
		deps.MiddlewareChain.WithEconomic(handler.StakeAssignHandler(deps)),
	).Methods("POST", "OPTIONS")

	router.Handle("/staking/unassign",
		deps.MiddlewareChain.WithEconomic(handler.StakeUnassignHandler(deps)),
	).Methods("POST", "OPTIONS")

}
