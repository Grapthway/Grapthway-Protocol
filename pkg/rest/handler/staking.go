package handler

import (
	"fmt"
	"grapthway/pkg/dependency"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/logging"
	"grapthway/pkg/model"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/libp2p/go-libp2p/core/peer"
)

func GetStakingStatusHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress := r.URL.Query().Get("owner_address")
		if ownerAddress == "" {
			http.Error(w, "Missing 'owner_address' query parameter", http.StatusBadRequest)
			return
		}
		stakeInfo, err := deps.LedgerClient.GetStakeInfo(ownerAddress)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		type apiStakeAllocation struct {
			NodePeerID string    `json:"nodePeerId"`
			Amount     float64   `json:"amount"`
			LockedAt   time.Time `json:"lockedAt"`
		}
		type apiUnbondingStake struct {
			Amount     float64   `json:"amount"`
			UnlockTime time.Time `json:"unlockTime"`
		}
		type apiStakeInfo struct {
			TotalStake  float64              `json:"totalStake"`
			Allocations []apiStakeAllocation `json:"allocations"`
			Unbonding   []apiUnbondingStake  `json:"unbonding"`
		}

		apiResponse := apiStakeInfo{
			TotalStake:  float64(stakeInfo.TotalStake) / model.GCU_MICRO_UNIT,
			Allocations: make([]apiStakeAllocation, len(stakeInfo.Allocations)),
			Unbonding:   make([]apiUnbondingStake, len(stakeInfo.Unbonding)),
		}

		for i, alloc := range stakeInfo.Allocations {
			apiResponse.Allocations[i] = apiStakeAllocation{
				NodePeerID: alloc.NodePeerID,
				Amount:     float64(alloc.Amount) / model.GCU_MICRO_UNIT,
				LockedAt:   alloc.LockedAt,
			}
		}
		for i, unbond := range stakeInfo.Unbonding {
			apiResponse.Unbonding[i] = apiUnbondingStake{
				Amount:     float64(unbond.Amount) / model.GCU_MICRO_UNIT,
				UnlockTime: unbond.UnlockTime,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(apiResponse)
	}
}

func StakeDepositHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		var payload struct {
			Amount float64 `json:"amount"`
			Nonce  uint64  `json:"nonce"`
		}
		if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		amountMicro := uint64(payload.Amount * model.GCU_MICRO_UNIT)

		tx := types.Transaction{
			ID:        fmt.Sprintf("tx-%d-stake-deposit", time.Now().UnixNano()),
			Type:      model.StakeDepositTransaction,
			From:      ownerAddress,
			Amount:    amountMicro,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
			Nonce:     payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Stake deposit failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		deps.Logger.Log(logging.LogEntry{
			Timestamp: time.Now(), LogType: logging.LedgerLog, Activity: "Stake Deposit",
			User: ownerAddress,
			TransactionInfo: &logging.TransactionInfo{
				TransactionID: processedTx.ID, Type: string(processedTx.Type), From: processedTx.From, To: processedTx.To,
				Amount: payload.Amount,
			},
		})

		w.WriteHeader(http.StatusOK)
		jsoniter.NewEncoder(w).Encode(map[string]string{"message": "Stake deposit transaction submitted successfully", "transactionId": processedTx.ID})
	}
}

func StakeWithdrawalHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		var payload struct {
			Amount float64 `json:"amount"`
			Nonce  uint64  `json:"nonce"`
		}
		if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		amountMicro := uint64(payload.Amount * model.GCU_MICRO_UNIT)

		tx := types.Transaction{
			ID:        fmt.Sprintf("tx-%d-stake-withdraw", time.Now().UnixNano()),
			Type:      model.StakeWithdrawalTransaction,
			From:      ownerAddress,
			Amount:    amountMicro,
			Timestamp: time.Now(),
			CreatedAt: time.Now(),
			Nonce:     payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Stake withdrawal failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		deps.Logger.Log(logging.LogEntry{
			Timestamp: time.Now(), LogType: logging.LedgerLog, Activity: "Stake Withdrawal",
			User: ownerAddress,
			TransactionInfo: &logging.TransactionInfo{
				TransactionID: processedTx.ID, Type: string(processedTx.Type), From: processedTx.From, To: processedTx.To,
				Amount: payload.Amount,
			},
		})

		w.WriteHeader(http.StatusOK)
		jsoniter.NewEncoder(w).Encode(map[string]string{"message": "Stake withdrawal transaction submitted successfully", "transactionId": processedTx.ID})
	}
}

func StakeAssignHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
			return
		}

		var payload struct {
			NodePeerID string  `json:"nodePeerId"`
			Amount     float64 `json:"amount"`
			Nonce      uint64  `json:"nonce"`
		}
		if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		amountMicro := uint64(payload.Amount * model.GCU_MICRO_UNIT)

		tx := types.Transaction{
			ID:           fmt.Sprintf("tx-%d-stake-assign", time.Now().UnixNano()),
			Type:         model.StakeAssignTransaction,
			From:         ownerAddress,
			Amount:       amountMicro,
			TargetNodeID: payload.NodePeerID,
			Timestamp:    time.Now(),
			CreatedAt:    time.Now(),
			Nonce:        payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Stake assignment failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		deps.Logger.Log(logging.LogEntry{
			Timestamp: time.Now(), LogType: logging.LedgerLog, Activity: "Stake Assign",
			User: ownerAddress,
			TransactionInfo: &logging.TransactionInfo{
				TransactionID: processedTx.ID, Type: string(processedTx.Type), From: processedTx.From, To: processedTx.TargetNodeID,
				Amount: payload.Amount,
			},
		})

		w.WriteHeader(http.StatusOK)
		jsoniter.NewEncoder(w).Encode(map[string]string{"message": "Stake assignment transaction submitted successfully", "transactionId": processedTx.ID})
	}
}

func StakeUnassignHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ownerAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok || ownerAddress == "" {
			http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
			return
		}

		var payload struct {
			NodePeerID string `json:"nodePeerId"`
			Nonce      uint64 `json:"nonce"`
		}
		if err := jsoniter.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		targetPeerID, err := peer.Decode(payload.NodePeerID)
		if err != nil {
			http.Error(w, "Invalid target node Peer ID", http.StatusBadRequest)
			return
		}

		if deps.P2PNode.IsPeerConnected(targetPeerID) {
			http.Error(w, "Cannot unassign stake from a node that is currently online", http.StatusForbidden)
			return
		}

		tx := types.Transaction{
			ID:           fmt.Sprintf("tx-%d-stake-unassign", time.Now().UnixNano()),
			Type:         model.StakeUnassignTransaction,
			From:         ownerAddress,
			TargetNodeID: payload.NodePeerID,
			Timestamp:    time.Now(),
			CreatedAt:    time.Now(),
			Nonce:        payload.Nonce,
		}

		processedTx, err := deps.LedgerClient.ProcessTransactionSubmission(r.Context(), tx)
		if err != nil {
			http.Error(w, "Stake unassignment failed: "+err.Error(), http.StatusBadRequest)
			return
		}

		deps.Logger.Log(logging.LogEntry{
			Timestamp: time.Now(), LogType: logging.LedgerLog, Activity: "Stake Unassign",
			User: ownerAddress,
			TransactionInfo: &logging.TransactionInfo{
				TransactionID: processedTx.ID, Type: string(processedTx.Type), From: processedTx.From, To: processedTx.TargetNodeID,
			},
		})

		w.WriteHeader(http.StatusOK)
		jsoniter.NewEncoder(w).Encode(map[string]string{"message": "Stake unassignment transaction submitted successfully, funds are now in unbonding period", "transactionId": processedTx.ID})
	}
}
