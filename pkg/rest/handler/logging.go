package handler

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/logging"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	jsoniter "github.com/json-iterator/go"
)

func GetUserLogsHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		vars := mux.Vars(r)
		logTypeStr := vars["log_type"]

		var logType logging.LogType
		switch logTypeStr {
		case "gateway":
			logType = logging.GatewayLog
		case "ledger":
			logType = logging.LedgerLog
		default:
			http.Error(w, "User can only view gateway and ledger logs", http.StatusForbidden)
			return
		}

		start, end, maxStr := r.URL.Query().Get("start"), r.URL.Query().Get("end"), r.URL.Query().Get("max")
		var max int
		if maxStr != "" {
			var err error
			if max, err = strconv.Atoi(maxStr); err != nil {
				http.Error(w, "Invalid 'max' parameter", http.StatusBadRequest)
				return
			}
		}

		allLogs, err := deps.Logger.GetLogs(logType, start, end, max)
		if err != nil {
			http.Error(w, "Failed to retrieve logs", http.StatusInternalServerError)
			return
		}

		var userLogs []logging.LogEntry
		for _, entry := range allLogs {
			// For gateway logs, check if user is the route owner (stored in User field)
			if logType == logging.GatewayLog && entry.User == userAddress {
				userLogs = append(userLogs, entry)
			} else if logType == logging.LedgerLog && (entry.TransactionInfo != nil && entry.TransactionInfo.From == userAddress) {
				userLogs = append(userLogs, entry)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(userLogs)
	}
}
