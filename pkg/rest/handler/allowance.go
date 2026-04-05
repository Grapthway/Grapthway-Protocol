package handler

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/model"
	"grapthway/pkg/util"
	"net/http"
	"time"

	json "github.com/json-iterator/go"
)

func SetAllowanceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		var payload struct {
			Spender string  `json:"spender"`
			Amount  float64 `json:"amount"`
			Nonce   uint64  `json:"nonce"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if payload.Spender == "" {
			http.Error(w, "Spender address is required", http.StatusBadRequest)
			return
		}

		amountMicro := uint64(payload.Amount * model.GCU_MICRO_UNIT)

		tx := types.Transaction{
			ID:             util.GenerateTxID("tx-set-allowance-", ownerAddress, payload.Spender, amountMicro, now.UnixNano()),
			Type:           model.SetAllowanceTransaction,
			From:           ownerAddress,
			To:             payload.Spender,
			AllowanceLimit: amountMicro,
			Timestamp:      now,
			CreatedAt:      now,
			Nonce:          payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Set allowance failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message":       "Allowance set transaction accepted",
			"transactionId": processedTx.ID,
		})
	}
}

func RemoveAllowanceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		var payload struct {
			Spender string `json:"spender"`
			Nonce   uint64 `json:"nonce"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		tx := types.Transaction{
			ID:        util.GenerateTxID("tx-remove-allowance-", ownerAddress, payload.Spender, 0, now.UnixNano()),
			Type:      model.RemoveAllowanceTransaction,
			From:      ownerAddress,
			To:        payload.Spender,
			Timestamp: now,
			CreatedAt: now,
			Nonce:     payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Remove allowance failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"message":       "Remove allowance transaction accepted",
			"transactionId": processedTx.ID,
		})
	}
}

func GetAllowanceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress := r.URL.Query().Get("owner")
		spenderAddress := r.URL.Query().Get("spender")
		if ownerAddress == "" || spenderAddress == "" {
			http.Error(w, "Missing 'owner' and 'spender' query parameters", http.StatusBadRequest)
			return
		}

		account, err := deps.LedgerClient.GetLiveAccountState(ownerAddress)
		if err != nil {
			http.Error(w, "Could not retrieve account state", http.StatusInternalServerError)
			return
		}

		var allowanceMicro uint64
		if account.Allowances != nil {
			allowanceMicro = account.Allowances[spenderAddress]
		}

		allowanceFull := float64(allowanceMicro) / model.GCU_MICRO_UNIT

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"owner":     ownerAddress,
			"spender":   spenderAddress,
			"allowance": allowanceFull,
		})
	}
}
