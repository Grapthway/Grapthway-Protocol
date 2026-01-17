package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"grapthway/pkg/crypto"
	"grapthway/pkg/dependency"
	"grapthway/pkg/gateway"
	"grapthway/pkg/util"
	"grapthway/pkg/workflow"
	"io"
	"log"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func PublishConfigHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		devID := r.Header.Get("X-Grapthway-Developer-ID")
		configSignatureHex := r.Header.Get("X-Grapthway-Config-Signature")

		if devID == "" {
			http.Error(w, "Missing developer identity header: X-Grapthway-Developer-ID", http.StatusForbidden)
			return
		}
		if configSignatureHex == "" {
			http.Error(w, "Missing configuration signature header: X-Grapthway-Config-Signature", http.StatusBadRequest)
			return
		}

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Cannot read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		var configData struct {
			DeveloperPubKey string `json:"developerPubKey"`
			Service         string `json:"service"`
		}
		if err := jsoniter.Unmarshal(bodyBytes, &configData); err != nil {
			http.Error(w, "Invalid JSON in request body", http.StatusBadRequest)
			return
		}

		devPubKey, err := crypto.HexToPublicKey(configData.DeveloperPubKey)
		if err != nil {
			http.Error(w, "Invalid developer public key in config", http.StatusBadRequest)
			return
		}
		if crypto.PublicKeyToAddress(devPubKey) != devID {
			http.Error(w, "Developer ID in header does not match public key in payload", http.StatusForbidden)
			return
		}

		signature, err := hex.DecodeString(configSignatureHex)
		if err != nil {
			http.Error(w, "Invalid signature format", http.StatusBadRequest)
			return
		}

		hash := sha256.Sum256(bodyBytes)
		if !crypto.VerifySignature(hash[:], signature, devPubKey) {
			log.Printf("[ERROR-LOG] CONFIG SIGNATURE VERIFICATION FAILED for service '%s'", configData.Service)
			http.Error(w, "Invalid signature for configuration", http.StatusForbidden)
			return
		}
		log.Printf("[DEBUG-LOG] Signature verification successful for service '%s'", configData.Service)

		compositeKey := util.GetCompositeKey(devID, configData.Service)
		cid, err := deps.DhtService.Put(compositeKey, bodyBytes)
		if err != nil {
			http.Error(w, "Failed to store configuration in DHT", http.StatusInternalServerError)
			return
		}
		log.Printf("[DEBUG-LOG] Stored config for service '%s' in local DHT with CID: %s", compositeKey, cid)

		updateMsg := map[string]string{
			"service": compositeKey,
			"cid":     string(cid),
		}
		gossipBytes, _ := jsoniter.Marshal(updateMsg)
		log.Printf("[DEBUG-LOG] Gossiping ConfigUpdateTopic for service '%s'", compositeKey)
		if err := deps.P2PNode.Gossip(gateway.ConfigUpdateTopic, gossipBytes); err != nil {
			log.Printf("WARN: Failed to gossip configuration update for service '%s': %v", compositeKey, err)
		}

		log.Printf("[DEBUG-LOG] Triggering local FetchVerifyAndCacheConfig for '%s'", compositeKey)
		_, err = deps.StorageService.FetchVerifyAndCacheConfig(compositeKey, cid)
		if err != nil {
			log.Printf("ERROR: Failed to cache config locally for '%s' after successful publish: %v", compositeKey, err)
		}

		log.Printf("[DEBUG-LOG] Proactively gossiping network state after config update for '%s'", compositeKey)
		deps.StorageService.BroadcastCurrentState()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Configuration published successfully"))
	}
}

func HealthHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read the raw body FIRST (before any unmarshaling)
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Cannot read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()

		// Get developer ID from header
		developerAddress := r.Header.Get("X-Grapthway-Developer-ID")
		if developerAddress == "" {
			http.Error(w, "Missing developer identity header: X-Grapthway-Developer-ID", http.StatusForbidden)
			return
		}

		// Get signature from header
		configSignatureHex := r.Header.Get("X-Grapthway-Config-Signature")
		if configSignatureHex == "" {
			http.Error(w, "Missing configuration signature header: X-Grapthway-Config-Signature", http.StatusBadRequest)
			return
		}

		// NOW unmarshal the body for processing
		var reqBody struct {
			Service         string `json:"service"`
			URL             string `json:"url"`
			Type            string `json:"type"`
			Path            string `json:"path,omitempty"`
			Subgraph        string `json:"subgraph"`
			ConfigCID       string `json:"configCid"`
			DeveloperPubKey string `json:"developerPubKey"`
		}

		if err := jsoniter.Unmarshal(bodyBytes, &reqBody); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Verify public key matches developer address
		devPubKey, err := crypto.HexToPublicKey(reqBody.DeveloperPubKey)
		if err != nil {
			http.Error(w, "Invalid developer public key in request body", http.StatusBadRequest)
			return
		}

		if crypto.PublicKeyToAddress(devPubKey) != developerAddress {
			http.Error(w, "Developer ID in header does not match public key in payload", http.StatusForbidden)
			return
		}

		// Decode signature
		signature, err := hex.DecodeString(configSignatureHex)
		if err != nil {
			http.Error(w, "Invalid signature format", http.StatusBadRequest)
			return
		}

		// *** FIX: Hash the RAW body bytes, not re-marshaled ***
		hash := sha256.Sum256(bodyBytes)
		if !crypto.VerifySignature(hash[:], signature, devPubKey) {
			log.Printf("[ERROR-LOG] HEALTH SIGNATURE VERIFICATION FAILED for service '%s'", reqBody.Service)
			http.Error(w, "Invalid signature for health check", http.StatusForbidden)
			return
		}

		log.Printf("[DEBUG-LOG] Health check signature verification successful for service '%s'", reqBody.Service)

		// Continue with the rest of the handler...
		_, err = deps.StorageService.UpdateServiceRegistration(
			developerAddress,
			reqBody.Service,
			reqBody.URL,
			reqBody.Subgraph,
			reqBody.ConfigCID,
			reqBody.Type,
			reqBody.Path,
		)

		if err != nil {
			http.Error(w, "Heartbeat failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Heartbeat processed successfully"))
	}
}

func GetUserWorkflowsHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userAddress, ok := r.Context().Value("authenticatedUser").(string)
		if !ok {
			http.Error(w, "Authenticated user not found in context", http.StatusInternalServerError)
			return
		}

		liveInstances := deps.WorkflowEngine.GetActiveInstances()
		checkpointedData := deps.DhtService.GetAllWithPrefix("workflow:instance:")

		var userInstances []*workflow.Instance
		for _, instance := range liveInstances {
			if instance.DeveloperID == userAddress {
				userInstances = append(userInstances, instance)
			}
		}

		instanceMap := make(map[string]*workflow.Instance)
		for _, inst := range userInstances {
			instanceMap[inst.ID] = inst
		}

		for _, instanceBytes := range checkpointedData {
			var instance workflow.Instance
			if jsoniter.Unmarshal(instanceBytes, &instance) == nil {
				if instance.DeveloperID == userAddress {
					if existing, ok := instanceMap[instance.ID]; !ok || instance.Version > existing.Version {
						instanceMap[instance.ID] = &instance
					}
				}
			}
		}

		finalList := make([]*workflow.Instance, 0, len(instanceMap))
		for _, inst := range instanceMap {
			finalList = append(finalList, inst)
		}

		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(finalList)
	}
}

func ServiceHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, _ := deps.StorageService.GetAllServices()
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(services)
	}
}

func GetPipelinesHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pipelines, err := deps.StorageService.GetAllMiddlewarePipelines()
		if err != nil {
			http.Error(w, "Failed to retrieve pipelines", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(pipelines)
	}
}

func SchemaHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		schemas, err := deps.StorageService.GetAllSchemas()
		if err != nil {
			http.Error(w, "Failed to retrieve schemas", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(schemas)
	}
}

func GatewayStatusHandler(deps *dependency.Dependencies) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, _ := deps.StorageService.GetAllServices()
		pipelineKeys, _ := deps.StorageService.GetAllPipelineKeys()
		totalInstances := 0
		subgraphs := make(map[string]bool)
		for _, instances := range services {
			totalInstances += len(instances)
			for _, instance := range instances {
				if instance.Type == "graphql" && instance.Subgraph != "" {
					subgraphs[instance.Subgraph] = true
				}
			}
		}
		var peerStrings []string
		for _, p := range deps.P2PNode.GetPeerList() {
			peerStrings = append(peerStrings, p.String())
		}
		status := struct {
			Uptime         string   `json:"uptime"`
			NodeID         string   `json:"node_id"`
			NodeAddress    string   `json:"node_address"`
			Role           string   `json:"role"`
			Peers          []string `json:"peers"`
			TotalServices  int      `json:"total_services"`
			TotalInstances int      `json:"total_instances"`
			TotalSubgraphs int      `json:"total_subgraphs"`
			TotalPipelines int      `json:"total_pipelines"`
			StorageType    string   `json:"storage_type"`
		}{
			Uptime:         time.Since(deps.StartTime).String(),
			NodeID:         deps.P2PNode.Host.ID().String(),
			NodeAddress:    deps.NodeIdentity.Address,
			Role:           deps.P2PNode.Role,
			Peers:          peerStrings,
			TotalServices:  len(services),
			TotalInstances: totalInstances,
			TotalSubgraphs: len(subgraphs),
			TotalPipelines: len(pipelineKeys),
			StorageType:    "decentralized (p2p/dht)",
		}
		w.Header().Set("Content-Type", "application/json")
		jsoniter.NewEncoder(w).Encode(status)
	}
}
