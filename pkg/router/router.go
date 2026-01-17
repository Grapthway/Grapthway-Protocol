package router

import (
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// InstanceHealth tracks the health and performance metrics of a service instance
type InstanceHealth struct {
	URL           string
	LastPingTime  time.Duration // Latest ping response time
	LastPingCheck time.Time     // When we last checked the ping
	BannedUntil   time.Time     // Time until which this instance is banned
	FailureCount  int           // Consecutive failure count
	mu            sync.RWMutex
}

// ServiceRouter handles thread-safe round-robin load balancing with ping-based selection.
type ServiceRouter struct {
	counters      map[string]*atomic.Uint64
	lock          sync.RWMutex
	healthTracker map[string]*InstanceHealth
	healthLock    sync.RWMutex
	pingThreshold time.Duration // Max acceptable ping time (10ms)
	banDuration   time.Duration // How long to ban unhealthy instances (5s)
	pingCacheTTL  time.Duration // How long to cache ping results (30s)
}

func NewServiceRouter() *ServiceRouter {
	return &ServiceRouter{
		counters:      make(map[string]*atomic.Uint64),
		healthTracker: make(map[string]*InstanceHealth),
		pingThreshold: 10 * time.Millisecond,
		banDuration:   5 * time.Second,
		pingCacheTTL:  30 * time.Second,
	}
}

// PickInstance selects the best available instance based on ping time and round-robin
func (r *ServiceRouter) PickInstance(instances []ServiceInstance) ServiceInstance {
	if len(instances) == 0 {
		return ServiceInstance{}
	}

	// If only one instance, return it if not banned
	if len(instances) == 1 {
		if r.isInstanceBanned(instances[0].URL) {
			log.Printf("[LoadBalancer] Single instance %s is banned, returning it anyway", instances[0].URL)
		}
		return instances[0]
	}

	serviceName := instances[0].Service

	// Filter instances: not banned + ping <= 10ms
	healthyInstances := r.filterHealthyInstances(instances)

	// If no healthy instances, use all non-banned instances as fallback
	if len(healthyInstances) == 0 {
		log.Printf("[LoadBalancer] No instances with ping <= 10ms for service %s, using all non-banned instances", serviceName)
		healthyInstances = r.filterNonBannedInstances(instances)
	}

	// If still no instances (all banned), use all instances as last resort
	if len(healthyInstances) == 0 {
		log.Printf("[LoadBalancer] All instances banned for service %s, using all instances anyway", serviceName)
		healthyInstances = instances
	}

	// Get or create counter for this service
	r.lock.RLock()
	counter, exists := r.counters[serviceName]
	r.lock.RUnlock()

	if !exists {
		r.lock.Lock()
		counter, exists = r.counters[serviceName]
		if !exists {
			counter = new(atomic.Uint64)
			r.counters[serviceName] = counter
		}
		r.lock.Unlock()
	}

	// Round-robin selection from healthy instances
	idx := counter.Add(1) % uint64(len(healthyInstances))
	selected := healthyInstances[idx]

	log.Printf("[LoadBalancer] Picking instance %d (%s) for service %s (healthy: %d, total: %d)",
		idx, selected.URL, serviceName, len(healthyInstances), len(instances))

	return selected
}

// filterHealthyInstances returns instances that are not banned and have ping <= 10ms
func (r *ServiceRouter) filterHealthyInstances(instances []ServiceInstance) []ServiceInstance {
	var healthy []ServiceInstance
	var needsPing []ServiceInstance
	now := time.Now()

	for _, instance := range instances {
		// Check if banned
		if r.isInstanceBanned(instance.URL) {
			continue
		}

		// Get or create health record
		health := r.getOrCreateHealth(instance.URL)
		health.mu.RLock()

		// Check if we need to refresh ping
		needsPingCheck := health.LastPingCheck.IsZero() || now.Sub(health.LastPingCheck) > r.pingCacheTTL

		if needsPingCheck {
			health.mu.RUnlock()
			needsPing = append(needsPing, instance)
			continue // Will check ping synchronously below
		}

		// Check if ping is acceptable
		if health.LastPingTime > 0 && health.LastPingTime <= r.pingThreshold {
			health.mu.RUnlock()
			healthy = append(healthy, instance)
		} else {
			health.mu.RUnlock()
		}
	}

	// If we have instances that need ping checks, do them synchronously
	// This prevents the race condition where we return "no healthy instances"
	// before the ping results are available
	if len(needsPing) > 0 {
		var wg sync.WaitGroup
		resultChan := make(chan ServiceInstance, len(needsPing))

		for _, instance := range needsPing {
			wg.Add(1)
			go func(inst ServiceInstance) {
				defer wg.Done()

				// Perform ping check
				pingTime := r.checkInstancePingSync(inst.URL)

				// If ping is acceptable, add to results
				if pingTime > 0 && pingTime <= r.pingThreshold {
					resultChan <- inst
				}
			}(instance)
		}

		// Wait for all pings to complete (with timeout)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All pings completed
		case <-time.After(1 * time.Second):
			// Timeout waiting for pings
			log.Printf("[LoadBalancer] Ping checks timed out after 1 second")
		}
		close(resultChan)

		// Collect results
		for inst := range resultChan {
			healthy = append(healthy, inst)
		}
	}

	return healthy
}

// filterNonBannedInstances returns all instances that are not currently banned
func (r *ServiceRouter) filterNonBannedInstances(instances []ServiceInstance) []ServiceInstance {
	var available []ServiceInstance

	for _, instance := range instances {
		if !r.isInstanceBanned(instance.URL) {
			available = append(available, instance)
		}
	}

	return available
}

// isInstanceBanned checks if an instance is currently banned
func (r *ServiceRouter) isInstanceBanned(url string) bool {
	health := r.getOrCreateHealth(url)
	health.mu.RLock()
	defer health.mu.RUnlock()

	return time.Now().Before(health.BannedUntil)
}

// getOrCreateHealth gets or creates a health record for an instance
func (r *ServiceRouter) getOrCreateHealth(url string) *InstanceHealth {
	r.healthLock.RLock()
	health, exists := r.healthTracker[url]
	r.healthLock.RUnlock()

	if exists {
		return health
	}

	r.healthLock.Lock()
	defer r.healthLock.Unlock()

	// Double-check after acquiring write lock
	health, exists = r.healthTracker[url]
	if exists {
		return health
	}

	health = &InstanceHealth{
		URL:           url,
		LastPingCheck: time.Time{}, // Force initial ping check
	}
	r.healthTracker[url] = health

	// Don't trigger background ping here - it will be done synchronously on first use

	return health
}

// checkInstancePing performs a ping check on an instance
func (r *ServiceRouter) checkInstancePing(url string) {
	pingTime := r.checkInstancePingSync(url)
	_ = pingTime // Result is already recorded in checkInstancePingSync
}

// checkInstancePingSync performs a synchronous ping check and returns the ping time
func (r *ServiceRouter) checkInstancePingSync(url string) time.Duration {
	health := r.getOrCreateHealth(url)

	// Extract host from URL
	host, err := extractHost(url)
	if err != nil {
		log.Printf("[LoadBalancer] Failed to extract host from URL %s: %v", url, err)
		r.recordPingFailure(url)
		return 0
	}

	// Perform TCP ping
	start := time.Now()
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	pingTime := time.Since(start)

	if err != nil {
		log.Printf("[LoadBalancer] Ping failed for %s: %v", url, err)
		r.recordPingFailure(url)
		return 0
	}
	conn.Close()

	// Record successful ping
	health.mu.Lock()
	health.LastPingTime = pingTime
	health.LastPingCheck = time.Now()
	health.FailureCount = 0
	health.mu.Unlock()

	log.Printf("[LoadBalancer] Ping successful for %s: %v", url, pingTime)
	return pingTime
}

// recordPingFailure records a ping failure and potentially bans the instance
func (r *ServiceRouter) recordPingFailure(url string) {
	health := r.getOrCreateHealth(url)

	health.mu.Lock()
	defer health.mu.Unlock()

	health.FailureCount++
	health.LastPingCheck = time.Now()

	// Ban after 3 consecutive failures
	if health.FailureCount >= 3 {
		health.BannedUntil = time.Now().Add(r.banDuration)
		health.FailureCount = 0 // Reset counter
		log.Printf("[LoadBalancer] Instance %s banned until %v due to repeated failures",
			url, health.BannedUntil)
	}
}

// ReportConnectionFailure should be called when a connection to an instance fails
// This immediately bans the instance for 5 seconds
func (r *ServiceRouter) ReportConnectionFailure(url string) {
	health := r.getOrCreateHealth(url)

	health.mu.Lock()
	defer health.mu.Unlock()

	health.BannedUntil = time.Now().Add(r.banDuration)
	health.FailureCount = 0
	log.Printf("[LoadBalancer] Instance %s banned until %v due to connection failure",
		url, health.BannedUntil)
}

// extractHost extracts host:port from a URL
func extractHost(urlStr string) (string, error) {
	// Handle both http://host:port and host:port formats
	scheme := "http"
	if len(urlStr) > 8 && urlStr[:8] == "https://" {
		scheme = "https"
		urlStr = urlStr[8:]
	} else if len(urlStr) > 7 && urlStr[:7] == "http://" {
		scheme = "http"
		urlStr = urlStr[7:]
	}

	// Remove path if present
	if idx := len(urlStr); idx > 0 {
		for i, c := range urlStr {
			if c == '/' {
				idx = i
				break
			}
		}
		urlStr = urlStr[:idx]
	}

	// If no port specified, add default ports
	if !hasPort(urlStr) {
		if scheme == "https" {
			urlStr = urlStr + ":443"
		} else {
			urlStr = urlStr + ":80"
		}
	}

	return urlStr, nil
}

// hasPort checks if a host string contains a port
func hasPort(host string) bool {
	for _, c := range host {
		if c == ':' {
			return true
		}
	}
	return false
}

// CleanupStaleHealth removes health records for instances that no longer exist
func (r *ServiceRouter) CleanupStaleHealth(activeURLs map[string]bool) {
	r.healthLock.Lock()
	defer r.healthLock.Unlock()

	for url := range r.healthTracker {
		if !activeURLs[url] {
			delete(r.healthTracker, url)
			log.Printf("[LoadBalancer] Removed stale health record for %s", url)
		}
	}
}

// GetHealthStatus returns the health status of all tracked instances (for monitoring)
func (r *ServiceRouter) GetHealthStatus() map[string]InstanceHealthStatus {
	r.healthLock.RLock()
	defer r.healthLock.RUnlock()

	status := make(map[string]InstanceHealthStatus)
	now := time.Now()

	for url, health := range r.healthTracker {
		health.mu.RLock()
		status[url] = InstanceHealthStatus{
			URL:          url,
			LastPingTime: health.LastPingTime,
			IsBanned:     now.Before(health.BannedUntil),
			BannedUntil:  health.BannedUntil,
			FailureCount: health.FailureCount,
			LastCheck:    health.LastPingCheck,
		}
		health.mu.RUnlock()
	}

	return status
}

// InstanceHealthStatus represents the health status of an instance for monitoring
type InstanceHealthStatus struct {
	URL          string
	LastPingTime time.Duration
	IsBanned     bool
	BannedUntil  time.Time
	FailureCount int
	LastCheck    time.Time
}
