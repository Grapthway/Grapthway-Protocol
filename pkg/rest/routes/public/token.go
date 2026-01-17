package public

import (
	"grapthway/pkg/ledger"
	"net/http"

	"github.com/gorilla/mux"
)

func TokenPublicRoutes(router *mux.Router, ledgerClient *ledger.Client) {

	router.HandleFunc("/token/info", func(w http.ResponseWriter, r *http.Request) {
		ledger.GetTokenHandler(w, r, ledgerClient)
	}).Methods("GET", "OPTIONS")

	router.HandleFunc("/token/balance", func(w http.ResponseWriter, r *http.Request) {
		ledger.GetTokenBalanceHandler(w, r, ledgerClient)
	}).Methods("GET", "OPTIONS")

	router.HandleFunc("/token/allowance", func(w http.ResponseWriter, r *http.Request) {
		ledger.GetAllowanceHandler(w, r, ledgerClient)
	}).Methods("GET", "OPTIONS")
	router.HandleFunc("/token/history", func(w http.ResponseWriter, r *http.Request) {
		ledger.GetTokenHistoryHandler(w, r, ledgerClient)
	}).Methods("GET", "OPTIONS")

}
