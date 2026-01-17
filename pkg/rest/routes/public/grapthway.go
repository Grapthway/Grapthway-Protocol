package public

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/rest/handler"

	"github.com/gorilla/mux"
)

func GrapthwayPublicRoutes(
	router *mux.Router,
	deps *dependency.Dependencies,
) {

	router.HandleFunc("/gateway-status", handler.GatewayStatusHandler(deps)).Methods("GET", "OPTIONS")
	router.HandleFunc("/hardware-stats", handler.HardwareStatsHandler(deps)).Methods("GET", "OPTIONS")
	router.HandleFunc("/services", handler.ServiceHandler(deps)).Methods("GET", "OPTIONS")
	router.HandleFunc("/pipelines", handler.GetPipelinesHandler(deps)).Methods("GET", "OPTIONS")
	router.HandleFunc("/schema", handler.SchemaHandler(deps)).Methods("GET", "OPTIONS")
	router.Handle("/wallet/balance", handler.GetBalanceHandler(deps)).Methods("GET", "OPTIONS")
	router.Handle("/wallet/history", handler.GetWalletHistoryHandler(deps)).Methods("GET", "OPTIONS")
	router.Handle("/staking/status", handler.GetStakingStatusHandler(deps)).Methods("GET", "OPTIONS")
	router.Handle("/wallet/nonce", handler.GetNonceHandler(deps)).Methods("GET", "OPTIONS")
	router.Handle("/wallet/allowance", handler.GetAllowanceHandler(deps)).Methods("GET", "OPTIONS")

}
