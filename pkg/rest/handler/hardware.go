package handler

import (
	"grapthway/pkg/dependency"
	"grapthway/pkg/model"
	"net/http"

	jsoniter "github.com/json-iterator/go"
)

func HardwareStatsHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		localStats := deps.HardwareMonitor.GetHardwareInfo()
		localStats.NodeID = deps.P2PNode.Host.ID().String()
		localStats.Role = deps.P2PNode.Role
		globalStats := deps.NetworkHardwareMonitor.GetGlobalStats(localStats)
		response := struct {
			Local  model.HardwareInfo        `json:"local"`
			Global model.GlobalHardwareStats `json:"global"`
		}{
			Local:  localStats,
			Global: globalStats,
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(response)
	}
}
