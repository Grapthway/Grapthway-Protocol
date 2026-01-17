package rest

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/rest/routes/protected"
	"grapthway/pkg/rest/routes/public"
	"net/http"

	"github.com/gorilla/mux"
)

func Router(
	router *mux.Router,
	deps *dependency.Dependencies,
) {

	publicRouter := router.PathPrefix("/public").Subrouter()
	apiRouter := router.PathPrefix("/api/v1").Subrouter()

	public.GrapthwayPublicRoutes(publicRouter, deps)
	public.TokenPublicRoutes(publicRouter, deps.LedgerClient)

	protected.GrapthwayProtectedRoutes(
		apiRouter,
		deps,
	)
	protected.TokenProtectedRoutes(apiRouter, deps.LedgerClient)

	router.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(" Grapthway Protocol Node is running"))
	}).Methods("GET", "OPTIONS")

}
