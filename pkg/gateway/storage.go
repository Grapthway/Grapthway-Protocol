package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"grapthway/pkg/crypto"
	localdht "grapthway/pkg/dht"
	"grapthway/pkg/ledger"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/model"
	"grapthway/pkg/p2p"
	"grapthway/pkg/router"
	"grapthway/pkg/util"

	json "github.com/json-iterator/go"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

const (
	ConfigUpdateTopic = "grapthway-config-updates"
)

type SignedServiceConfig struct {
	URL             string                          `json:"url"`
	Service         string                          `json:"service"`
	Schema          string                          `json:"schema,omitempty"`
	MiddlewareMap   map[string]model.PipelineConfig `json:"middlewareMap"`
	RestPipelines   model.RestPipelineMap           `json:"restPipelines,omitempty"`
	StitchingConfig model.StitchingConfig           `json:"stitchingConfig,omitempty"`
	ServiceType     string                          `json:"type"`
	ServicePath     string                          `json:"path,omitempty"`
	DeveloperPubKey string                          `json:"developerPubKey"`
}

type VerifiedConfigCacheEntry struct {
	Config       SignedServiceConfig
	CID          localdht.CID
	LastVerified time.Time
}

type FullState struct {
	Configs   map[string]localdht.CID             `json:"configs"`
	Instances map[string][]router.ServiceInstance `json:"instances"`
}

type InstanceUpdateGossip struct {
	DeveloperAddress string                 `json:"developerAddress"`
	Instance         router.ServiceInstance `json:"instance"`
}

type DHTStorage struct {
	node                *p2p.Node
	dht                 *localdht.DHT
	ctx                 context.Context
	configUpdateChan    chan<- string
	ledgerClient        *ledger.Client
	serviceConfigs      map[string]localdht.CID
	verifiedConfigs     map[string]VerifiedConfigCacheEntry
	schemaVersions      map[string]int
	services            map[string][]router.ServiceInstance
	lock                sync.RWMutex
	processingCIDs      map[string]bool
	processingCIDsMutex sync.Mutex
}

func NewDHTStorage(ctx context.Context, node *p2p.Node, dht *localdht.DHT, topicChannels map[string]<-chan *pubsub.Message, configUpdateChan chan<- string, ledgerClient *ledger.Client) *DHTStorage {
	s := &DHTStorage{
		node:             node,
		dht:              dht,
		ctx:              ctx,
		configUpdateChan: configUpdateChan,
		ledgerClient:     ledgerClient,
		serviceConfigs:   make(map[string]localdht.CID),
		verifiedConfigs:  make(map[string]VerifiedConfigCacheEntry),
		schemaVersions:   make(map[string]int),
		services:         make(map[string][]router.ServiceInstance),
		processingCIDs:   make(map[string]bool),
	}
	s.listenForNetworkUpdates(topicChannels)
	return s
}

func (s *DHTStorage) SubmitDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	if s.ledgerClient == nil {
		return nil, fmt.Errorf("ledger client is not available on the storage service")
	}
	return s.ledgerClient.ProcessTransactionSubmission(ctx, tx)
}

func (s *DHTStorage) BroadcastDelegatedTransaction(ctx context.Context, tx types.Transaction) (*types.Transaction, error) {
	if s.ledgerClient == nil {
		return nil, fmt.Errorf("ledger client is not available on the storage service")
	}
	return s.ledgerClient.BroadcastDelegatedTransaction(ctx, tx)
}

func (s *DHTStorage) GetNodeIdentity() *crypto.Identity {
	return s.node.Identity
}

func (s *DHTStorage) listenForNetworkUpdates(topicChannels map[string]<-chan *pubsub.Message) {
	log.Println("STORAGE: Listening to pre-subscribed network topics.")
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case msg := <-topicChannels[ConfigUpdateTopic]:
				s.handleConfigUpdate(msg.Data)
			case msg := <-topicChannels[p2p.StateRequestTopic]:
				s.handleStateRequest(msg)
			case msg := <-topicChannels[p2p.StateResponseTopic]:
				s.handleStateResponse(msg.Data)
			case msg := <-topicChannels[p2p.DHTRequestTopic]:
				s.handleDHTRequest(msg.Data)
			case msg := <-topicChannels[p2p.DHTResponseTopic]:
				s.handleDHTResponse(msg.Data)
			case msg := <-topicChannels[p2p.InstanceUpdateTopic]:
				s.handleInstanceUpdate(msg.Data)
			}
		}
	}()
}

func (s *DHTStorage) handleInstanceUpdate(data []byte) {
	var updateGossip InstanceUpdateGossip
	if err := json.Unmarshal(data, &updateGossip); err != nil {
		log.Printf("STORAGE: Error unmarshalling instance update gossip: %v", err)
		return
	}
	instance := updateGossip.Instance
	compositeKey := util.GetCompositeKey(updateGossip.DeveloperAddress, instance.Service)
	s.lock.Lock()
	defer s.lock.Unlock()
	instances := s.services[compositeKey]
	found := false
	for i, inst := range instances {
		if inst.URL == instance.URL {
			instances[i] = instance
			found = true
			break
		}
	}
	if !found {
		instances = append(instances, instance)
	}
	s.services[compositeKey] = instances
}

func (s *DHTStorage) UpdateServiceRegistration(developerAddress, serviceName, url, subgraph, configCID, serviceType, servicePath string) (model.SchemaChangeResult, error) {
	if developerAddress == "" || serviceName == "" {
		return model.SchemaChangeResult{IsChanged: false}, fmt.Errorf("developer address and service name are required")
	}
	compositeKey := util.GetCompositeKey(developerAddress, serviceName)
	s.lock.Lock()
	if latestCID, ok := s.serviceConfigs[compositeKey]; ok && configCID == "" {
		configCID = string(latestCID)
	}
	now := time.Now()
	instances := s.services[compositeKey]
	var updatedInstance router.ServiceInstance
	found := false
	for i, inst := range instances {
		if inst.URL == url {
			instances[i].LastSeen = now
			instances[i].Subgraph = subgraph
			instances[i].Type = serviceType
			instances[i].Path = servicePath
			instances[i].ConfigCID = configCID
			updatedInstance = instances[i]
			found = true
			break
		}
	}
	if !found {
		updatedInstance = router.ServiceInstance{
			DeveloperAddress: developerAddress,
			Service:          serviceName,
			URL:              url,
			Subgraph:         subgraph,
			LastSeen:         now,
			Type:             serviceType,
			Path:             servicePath,
			ConfigCID:        configCID,
		}
		instances = append(instances, updatedInstance)
	}
	s.services[compositeKey] = instances
	s.lock.Unlock()
	gossipPayload := InstanceUpdateGossip{
		DeveloperAddress: developerAddress,
		Instance:         updatedInstance,
	}
	instanceBytes, _ := json.Marshal(gossipPayload)
	if err := s.node.Gossip(p2p.InstanceUpdateTopic, instanceBytes); err != nil {
		log.Printf("STORAGE: Failed to gossip instance update for '%s': %v", compositeKey, err)
	}
	return model.SchemaChangeResult{IsChanged: false}, nil
}

func (s *DHTStorage) handleConfigUpdate(data []byte) {
	var update struct {
		Service string       `json:"service"`
		CID     localdht.CID `json:"cid"`
	}
	if err := json.Unmarshal(data, &update); err != nil {
		log.Printf("[ERROR-LOG] Error unmarshalling config update gossip: %v", err)
		return
	}
	s.lock.Lock()
	currentCID, exists := s.serviceConfigs[update.Service]
	if exists && currentCID == update.CID {
		s.lock.Unlock()
		return
	}
	log.Printf("[SYNC-LOG] Received config update for '%s' with new CID: %s", update.Service, update.CID)
	s.serviceConfigs[update.Service] = update.CID
	s.lock.Unlock()
	go s.FetchVerifyAndCacheConfig(update.Service, update.CID)
}

func (s *DHTStorage) FetchVerifyAndCacheConfig(compositeKey string, expectedCID localdht.CID) (model.SchemaChangeResult, error) {
	s.lock.RLock()
	if cached, exists := s.verifiedConfigs[compositeKey]; exists && cached.CID == expectedCID {
		if time.Since(cached.LastVerified) < 1*time.Minute {
			s.lock.RUnlock()
			log.Printf("STORAGE: Config for service '%s' (CID: %s) recently verified. Skipping.", compositeKey, expectedCID)
			return model.SchemaChangeResult{IsChanged: false}, nil
		}
	}
	s.lock.RUnlock()

	s.processingCIDsMutex.Lock()
	if s.processingCIDs[string(expectedCID)] {
		s.processingCIDsMutex.Unlock()
		log.Printf("[DEBUG-LOG] Already processing CID %s. Ignoring redundant fetch.", expectedCID)
		return model.SchemaChangeResult{IsChanged: false}, nil
	}
	s.processingCIDs[string(expectedCID)] = true
	s.processingCIDsMutex.Unlock()

	defer func() {
		s.processingCIDsMutex.Lock()
		delete(s.processingCIDs, string(expectedCID))
		s.processingCIDsMutex.Unlock()
	}()

	var needsFetch bool
	configBytes, err := s.dht.Get(compositeKey)

	if err != nil {
		needsFetch = true
	} else {
		hash := sha256.Sum256(configBytes)
		actualCID := localdht.CID(hex.EncodeToString(hash[:]))
		if actualCID != expectedCID {
			log.Printf("STORAGE: Local data for '%s' is stale. Expected %s, have %s.", compositeKey, expectedCID, actualCID)
			needsFetch = true
		}
	}

	if needsFetch {
		log.Printf("STORAGE: Requesting config for '%s' from network.", compositeKey)
		request := map[string]string{"key": compositeKey}
		requestBytes, _ := json.Marshal(request)
		s.node.Gossip(p2p.DHTRequestTopic, requestBytes)
		return model.SchemaChangeResult{}, fmt.Errorf("config not found or stale, requested from network")
	}

	var config SignedServiceConfig
	if err := json.Unmarshal(configBytes, &config); err != nil {
		return model.SchemaChangeResult{}, fmt.Errorf("failed to unmarshal verified config data: %w", err)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	changeResult := model.SchemaChangeResult{}
	oldCacheEntry, exists := s.verifiedConfigs[compositeKey]

	isChanged := !exists || !reflect.DeepEqual(oldCacheEntry.Config, config)

	if isChanged {
		changeResult.IsChanged = true
		changeResult.NewCID = expectedCID
		if exists {
			changeResult.VersionBefore = s.schemaVersions[compositeKey]
		}
		newVersion := changeResult.VersionBefore + 1
		s.schemaVersions[compositeKey] = newVersion
		changeResult.VersionAfter = newVersion
		log.Printf("STORAGE: Configuration change detected for '%s'. New version: %d", compositeKey, newVersion)
	} else {
		changeResult.IsChanged = false
		changeResult.VersionAfter = s.schemaVersions[compositeKey]
	}

	s.serviceConfigs[compositeKey] = expectedCID
	s.verifiedConfigs[compositeKey] = VerifiedConfigCacheEntry{
		Config:       config,
		CID:          expectedCID,
		LastVerified: time.Now(),
	}
	log.Printf("✅ STORAGE: Fetched, verified, and cached new config for service '%s'", compositeKey)

	if changeResult.IsChanged {
		s.configUpdateChan <- compositeKey
	}

	return changeResult, nil
}

// GetMiddlewarePipeline now properly handles BOTH GraphQL fields (middlewareMap) and REST routes (restPipelines)
func (s *DHTStorage) GetMiddlewarePipeline(developerAddress, pipelineKey string) (*model.PipelineConfig, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for key, cacheEntry := range s.verifiedConfigs {
		ownerAddress, _, err := util.ParseCompositeKey(key)
		if err != nil {
			continue
		}
		if ownerAddress == developerAddress {
			// For GraphQL: Check middlewareMap using field name (e.g., "login", "getUser")
			if cacheEntry.Config.MiddlewareMap != nil {
				if pipeline, ok := cacheEntry.Config.MiddlewareMap[pipelineKey]; ok {
					pCopy := pipeline
					return &pCopy, nil
				}
			}

			// For REST: Check restPipelines using route key (e.g., "POST /login", "GET /profile")
			if cacheEntry.Config.RestPipelines != nil {
				if pipeline, ok := cacheEntry.Config.RestPipelines[pipelineKey]; ok {
					pCopy := *pipeline
					return &pCopy, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("pipeline '%s' not found for developer %s", pipelineKey, developerAddress)
}

func (s *DHTStorage) GetSchema(developerAddress, service string) (string, error) {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.RLock()
	defer s.lock.RUnlock()
	cacheEntry, ok := s.verifiedConfigs[compositeKey]
	if !ok {
		return "", fmt.Errorf("service '%s' not found for developer %s", service, developerAddress)
	}
	return cacheEntry.Config.Schema, nil
}

func (s *DHTStorage) GetAllSchemas() (map[string]string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	schemas := make(map[string]string)
	for key, cacheEntry := range s.verifiedConfigs {
		if cacheEntry.Config.Schema != "" {
			schemas[key] = cacheEntry.Config.Schema
		}
	}
	return schemas, nil
}

func (s *DHTStorage) GetService(developerAddress, service, subgraph string) ([]router.ServiceInstance, error) {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.RLock()
	defer s.lock.RUnlock()
	instances, ok := s.services[compositeKey]
	if !ok {
		return nil, nil
	}
	if subgraph == "" {
		return instances, nil
	}
	var filtered []router.ServiceInstance
	for _, instance := range instances {
		if instance.Subgraph == subgraph {
			filtered = append(filtered, instance)
		}
	}
	return filtered, nil
}

func (s *DHTStorage) GetAllServices() (map[string][]router.ServiceInstance, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	result := make(map[string][]router.ServiceInstance)
	for k, v := range s.services {
		instances := make([]router.ServiceInstance, len(v))
		copy(instances, v)
		result[k] = instances
	}
	return result, nil
}

func (s *DHTStorage) BroadcastCurrentState() {
	if !s.node.CanBroadcastState() {
		log.Println("[DEBUG-LOG] State broadcast throttled.")
		return
	}
	s.lock.RLock()
	state := FullState{
		Configs:   s.serviceConfigs,
		Instances: s.services,
	}
	s.lock.RUnlock()
	responseBytes, err := json.Marshal(state)
	if err != nil {
		log.Printf("[ERROR-LOG] Failed to marshal state for broadcast: %v", err)
		return
	}
	s.node.Gossip(p2p.StateResponseTopic, responseBytes)
}

func (s *DHTStorage) RequestNetworkState() {
	if err := s.node.Gossip(p2p.StateRequestTopic, []byte("{}")); err != nil {
		log.Printf("STORAGE: Failed to gossip state request: %v", err)
	}
}

func (s *DHTStorage) GetLatestCID(developerAddress, serviceName string) (localdht.CID, bool) {
	compositeKey := util.GetCompositeKey(developerAddress, serviceName)
	s.lock.RLock()
	defer s.lock.RUnlock()
	cid, ok := s.serviceConfigs[compositeKey]
	return cid, ok
}

func (s *DHTStorage) handleStateRequest(msg *pubsub.Message) {
	if msg.ReceivedFrom == s.node.Host.ID() {
		return
	}
	log.Println("[DEBUG-LOG] Received a network state request from a peer. Broadcasting our state in response.")
	s.BroadcastCurrentState()
}

func (s *DHTStorage) handleStateResponse(data []byte) {
	var peerState FullState
	if err := json.Unmarshal(data, &peerState); err != nil {
		log.Printf("[ERROR-LOG] Error unmarshalling state response gossip: %v", err)
		return
	}
	log.Printf("[DEBUG-LOG] Received state response from peer with %d configurations and %d service instance groups.", len(peerState.Configs), len(peerState.Instances))
	s.lock.Lock()
	for compositeKey, peerCID := range peerState.Configs {
		localCID, exists := s.serviceConfigs[compositeKey]
		if exists && localCID == peerCID {
			continue
		}

		if cached, exists := s.verifiedConfigs[compositeKey]; exists && cached.CID == peerCID {
			if time.Since(cached.LastVerified) < 1*time.Minute {
				continue
			}
		}

		log.Printf("[SYNC-LOG] Discovered new/updated service config '%s' from peer state. Fetching.", compositeKey)
		s.serviceConfigs[compositeKey] = peerCID
		go s.FetchVerifyAndCacheConfig(compositeKey, peerCID)
	}
	s.lock.Unlock()

	s.lock.Lock()
	for compositeKey, remoteInstances := range peerState.Instances {
		localInstances, exists := s.services[compositeKey]
		if !exists {
			s.services[compositeKey] = remoteInstances
			log.Printf("[SYNC-LOG] Discovered new service instances for '%s'", compositeKey)
			continue
		}
		instanceMap := make(map[string]router.ServiceInstance)
		for _, inst := range localInstances {
			instanceMap[inst.URL] = inst
		}
		for _, remoteInst := range remoteInstances {
			localInst, ok := instanceMap[remoteInst.URL]
			if !ok || remoteInst.LastSeen.After(localInst.LastSeen) {
				instanceMap[remoteInst.URL] = remoteInst
			}
		}
		mergedInstances := make([]router.ServiceInstance, 0, len(instanceMap))
		for _, inst := range instanceMap {
			mergedInstances = append(mergedInstances, inst)
		}
		s.services[compositeKey] = mergedInstances
	}
	s.lock.Unlock()
}

func (s *DHTStorage) handleDHTRequest(data []byte) {
	var request struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return
	}
	log.Printf("[DEBUG-LOG] Peer requested DHT data for key '%s'", request.Key)
	content, err := s.dht.Get(request.Key)
	if err != nil {
		log.Printf("[DEBUG-LOG] DHT data for key '%s' not found locally.", request.Key)
		return
	}
	log.Printf("[DEBUG-LOG] Found DHT data for key '%s'. Responding to peer.", request.Key)
	response := map[string]interface{}{"key": request.Key, "content": content}
	responseBytes, _ := json.Marshal(response)
	s.node.Gossip(p2p.DHTResponseTopic, responseBytes)
}

func (s *DHTStorage) handleDHTResponse(data []byte) {
	var response struct {
		Key     string `json:"key"`
		Content []byte `json:"content"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return
	}
	log.Printf("[DEBUG-LOG] Received DHT data for key '%s' from peer", response.Key)
	s.lock.RLock()
	expectedCID, ok := s.serviceConfigs[response.Key]
	s.lock.RUnlock()
	if !ok {
		log.Printf("[INFO] Received unsolicited DHT response for key '%s', ignoring.", response.Key)
		return
	}

	hash := sha256.Sum256(response.Content)
	responseCID := localdht.CID(hex.EncodeToString(hash[:]))
	if responseCID != expectedCID {
		log.Printf("[DEBUG-LOG] Ignoring DHT response with mismatched CID for key '%s': expected %s, got %s", response.Key, expectedCID, responseCID)
		return
	}

	s.processingCIDsMutex.Lock()
	if s.processingCIDs[string(expectedCID)] {
		s.processingCIDsMutex.Unlock()
		log.Printf("[DEBUG-LOG] Already processing CID %s. Ignoring redundant DHT response.", expectedCID)
		return
	}
	s.processingCIDsMutex.Unlock()
	if _, err := s.dht.Put(response.Key, response.Content); err != nil {
		log.Printf("[ERROR-LOG] Error putting received content into local DHT for key '%s': %v", response.Key, err)
		return
	}
	log.Printf("[DEBUG-LOG] Re-processing config for composite key '%s' after receiving DHT data.", response.Key)
	go s.FetchVerifyAndCacheConfig(response.Key, expectedCID)
}

// GetAllMiddlewarePipelines returns all pipelines from both middlewareMap and restPipelines
func (s *DHTStorage) GetAllMiddlewarePipelines() (map[string]map[string]model.PipelineConfig, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	allPipelines := make(map[string]map[string]model.PipelineConfig)
	for key, cacheEntry := range s.verifiedConfigs {
		ownerAddress, _, err := util.ParseCompositeKey(key)
		if err != nil {
			continue
		}

		// Initialize developer's pipeline map if needed
		if _, ok := allPipelines[ownerAddress]; !ok {
			allPipelines[ownerAddress] = make(map[string]model.PipelineConfig)
		}

		// Add REST pipelines (restPipelines)
		if len(cacheEntry.Config.RestPipelines) > 0 {
			for name, pipeline := range cacheEntry.Config.RestPipelines {
				allPipelines[ownerAddress][name] = *pipeline
			}
		}

		// Add GraphQL pipelines (middlewareMap)
		if len(cacheEntry.Config.MiddlewareMap) > 0 {
			for name, pipeline := range cacheEntry.Config.MiddlewareMap {
				// Only add if not already present from RestPipelines
				if _, exists := allPipelines[ownerAddress][name]; !exists {
					allPipelines[ownerAddress][name] = pipeline
				}
			}
		}
	}
	return allPipelines, nil
}

func (s *DHTStorage) GetAllPipelineKeys() ([]string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	var keys []string
	seen := make(map[string]bool)
	for _, cacheEntry := range s.verifiedConfigs {
		// Check RestPipelines
		for key := range cacheEntry.Config.RestPipelines {
			if !seen[key] {
				keys = append(keys, key)
				seen[key] = true
			}
		}
		// Check MiddlewareMap
		for key := range cacheEntry.Config.MiddlewareMap {
			if !seen[key] {
				keys = append(keys, key)
				seen[key] = true
			}
		}
	}
	return keys, nil
}

func (s *DHTStorage) GetRestPipelineMap(developerAddress, service string) (model.RestPipelineMap, error) {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.RLock()
	defer s.lock.RUnlock()
	if cacheEntry, ok := s.verifiedConfigs[compositeKey]; ok {
		// Return RestPipelines directly (this is the correct field)
		return cacheEntry.Config.RestPipelines, nil
	}
	return nil, fmt.Errorf("service '%s' not found for developer %s", service, developerAddress)
}

func (s *DHTStorage) SetStitchingConfig(developerAddress, service string, config model.StitchingConfig) error {
	return fmt.Errorf("SetStitchingConfig is deprecated; use a full config publication")
}

func (s *DHTStorage) GetAllStitchingConfigs() (map[string]model.StitchingConfig, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	configs := make(map[string]model.StitchingConfig)
	for key, cacheEntry := range s.verifiedConfigs {
		if cacheEntry.Config.StitchingConfig != nil {
			configs[key] = cacheEntry.Config.StitchingConfig
		}
	}
	return configs, nil
}

func (s *DHTStorage) RemoveService(developerAddress, service string) error {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.serviceConfigs, compositeKey)
	delete(s.verifiedConfigs, compositeKey)
	delete(s.schemaVersions, compositeKey)
	delete(s.services, compositeKey)
	return nil
}

func (s *DHTStorage) UpdateServiceInstances(developerAddress, service string, instances []router.ServiceInstance) error {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.services[compositeKey] = instances
	return nil
}

func (s *DHTStorage) GetSchemaVersion(developerAddress, service string) (int, error) {
	compositeKey := util.GetCompositeKey(developerAddress, service)
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.schemaVersions[compositeKey], nil
}

func (s *DHTStorage) RemoveSchema(developerAddress, service string) error { return nil }
