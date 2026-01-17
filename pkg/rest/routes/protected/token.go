package protected

import (
	"grapthway/pkg/ledger"
	"net/http"

	"github.com/gorilla/mux"
)

func TokenProtectedRoutes(router *mux.Router, ledgerClient *ledger.Client) {
	// Token management endpoints (require authentication)
	router.HandleFunc("/token/create", func(w http.ResponseWriter, r *http.Request) {
		ledger.CreateTokenHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/transfer", func(w http.ResponseWriter, r *http.Request) {
		ledger.TransferTokenHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/allowance/set", func(w http.ResponseWriter, r *http.Request) {
		ledger.SetAllowanceHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/allowance/delete", func(w http.ResponseWriter, r *http.Request) {
		ledger.DeleteAllowanceHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/burnable/set", func(w http.ResponseWriter, r *http.Request) {
		ledger.SetBurnableHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/mintable/set", func(w http.ResponseWriter, r *http.Request) {
		ledger.SetMintableHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/config/lock", func(w http.ResponseWriter, r *http.Request) {
		ledger.LockBurnMintHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/burn", func(w http.ResponseWriter, r *http.Request) {
		ledger.ManualBurnHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/mint", func(w http.ResponseWriter, r *http.Request) {
		ledger.ManualMintHandler(w, r, ledgerClient)
	}).Methods("POST", "OPTIONS")

	router.HandleFunc("/token/owned", func(w http.ResponseWriter, r *http.Request) {
		ledger.GetOwnedTokensHandler(w, r, ledgerClient)
	}).Methods("GET", "OPTIONS")

}
