package handler

import (
	"fmt"
	"grapthway/pkg/dependency"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"net/http"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func UserTransferHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fromAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || fromAddress == "" {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		var payload struct {
			To     string  `json:"to"`
			Amount float64 `json:"amount"`
			Nonce  uint64  `json:"nonce"`
		}

		if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		amountMicro := uint64(payload.Amount * model.GCU_MICRO_UNIT)

		tx := types.Transaction{
			ID:        fmt.Sprintf("tx-%d", time.Now().UnixNano()),
			Type:      model.TransferTransaction,
			From:      fromAddress,
			To:        payload.To,
			Amount:    amountMicro,
			Nonce:     payload.Nonce,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Transfer failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		deps.Logger.Log(logging.LogEntry{
			Timestamp: time.Now(), LogType: logging.LedgerLog, Activity: "User Transfer",
			User: fromAddress,
			TransactionInfo: &logging.TransactionInfo{
				TransactionID: processedTx.ID, Type: string(processedTx.Type), From: processedTx.From, To: processedTx.To,
				Amount: payload.Amount,
			},
		})
		w.WriteHeader(http.StatusAccepted)
		jsoniter.NewEncoder(w).Encode(map[string]string{
			"message":       "Transfer accepted for processing",
			"transactionId": processedTx.ID,
			"assignedNonce": strconv.FormatUint(processedTx.Nonce, 10),
		})
	}
}

func GetBalanceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		address := r.URL.Query().Get("address")
		if address == "" {
			http.Error(w, "Missing 'address' query parameter", http.StatusBadRequest)
			return
		}
		balance, err := deps.LedgerClient.CheckBalance(address)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(map[string]interface{}{"balance": balance})
	}
}

func GetWalletHistoryHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		address := r.URL.Query().Get("address")
		if address == "" {
			http.Error(w, "Missing 'address' query parameter", http.StatusBadRequest)
			return
		}

		pageStr := r.URL.Query().Get("page")
		limitStr := r.URL.Query().Get("limit")
		startStr := r.URL.Query().Get("start")
		endStr := r.URL.Query().Get("end")

		page, err := strconv.Atoi(pageStr)
		if err != nil || page < 1 {
			page = 1
		}

		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			limit = 10
		}

		var startTime, endTime time.Time
		if startStr != "" {
			startTime, err = time.Parse(time.RFC3339, startStr)
			if err != nil {
				http.Error(w, "Invalid 'start' time format, please use RFC3339", http.StatusBadRequest)
				return
			}
		}
		if endStr != "" {
			endTime, err = time.Parse(time.RFC3339, endStr)
			if err != nil {
				http.Error(w, "Invalid 'end' time format, please use RFC3339", http.StatusBadRequest)
				return
			}
		}

		historyPage, err := deps.LedgerClient.GetTransactionHistory(address, page, limit, startTime, endTime)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type apiTransaction struct {
			ID           string                `json:"id"`
			Type         types.TransactionType `json:"type"`
			From         string                `json:"from"`
			To           string                `json:"to,omitempty"`
			Amount       float64               `json:"amount,omitempty"`
			Nonce        uint64                `json:"nonce"`
			Timestamp    time.Time             `json:"timestamp"`
			TargetNodeID string                `json:"targetNodeId,omitempty"`
			Status       string                `json:"status"`
		}

		apiTransactions := make([]apiTransaction, len(historyPage.Transactions))
		for i, txDetail := range historyPage.Transactions {
			apiTransactions[i] = apiTransaction{
				ID:           txDetail.ID,
				Type:         txDetail.Type,
				From:         txDetail.From,
				To:           txDetail.To,
				Amount:       float64(txDetail.Amount) / model.GCU_MICRO_UNIT,
				Nonce:        txDetail.Nonce,
				Timestamp:    txDetail.Timestamp,
				TargetNodeID: txDetail.TargetNodeID,
				Status:       txDetail.Status,
			}
		}

		response := map[string]interface{}{
			"transactions": apiTransactions,
			"total":        historyPage.TotalRecords,
			"totalPages":   historyPage.TotalPages,
			"currentPage":  historyPage.CurrentPage,
		}

		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(response)
	}
}

func GetNonceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		address := r.URL.Query().Get("address")
		if address == "" {
			http.Error(w, "Missing 'address' query parameter", http.StatusBadRequest)
			return
		}
		nonce, err := deps.LedgerClient.GetNonce(address)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(map[string]interface{}{"nonce": nonce})
	}
}
