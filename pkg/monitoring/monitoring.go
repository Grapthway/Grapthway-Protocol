package monitoring

import (
	"bufio"
	"context"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/json-iterator/go"

	"grapthway/pkg/model"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multihash"
)

type PeerStatEntry struct {
	Stats    model.HardwareInfo
	LastSeen time.Time
}

type Monitor struct {
	lastStats        model.SystemStats
	lastTime         time.Time
	lastCPUacctUsage uint64 // For cgroup-aware CPU calculation
	currentInfo      model.HardwareInfo
	mutex            sync.RWMutex
	collectionRate   time.Duration
}

func NewMonitor(rate time.Duration) *Monitor {
	m := &Monitor{
		collectionRate: rate,
	}
	m.collect() // Initial collection
	go m.run()
	return m
}

func (m *Monitor) GetHardwareInfo() model.HardwareInfo {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.currentInfo
}

func (m *Monitor) run() {
	ticker := time.NewTicker(m.collectionRate)
	defer ticker.Stop()
	for range ticker.C {
		m.collect()
	}
}

func (m *Monitor) collect() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	newTime := time.Now()
	duration := newTime.Sub(m.lastTime).Seconds()

	m.currentInfo.CgroupLimits = getCgroupStats()

	// --- CPU Usage Calculation ---
	// Cgroup-aware calculation (preferred method, for containers)
	cgroupV1Usage, errV1 := readUintFromFile("/sys/fs/cgroup/cpu/cpuacct.usage")
	cgroupV2Usage, errV2 := getCgroupV2CPUUsage()

	var cgroupCPUUsage uint64
	var errCgroup error

	if errV1 == nil {
		cgroupCPUUsage = cgroupV1Usage
		errCgroup = nil
	} else if errV2 == nil {
		cgroupCPUUsage = cgroupV2Usage
		errCgroup = nil
	} else {
		errCgroup = errV1 // Report V1 error if both fail
	}

	if errCgroup == nil && m.currentInfo.CgroupLimits.CPUQuotaCores > 0 && duration > 0 {
		cpuUsageDelta := cgroupCPUUsage - m.lastCPUacctUsage
		totalAvailableTime := uint64(duration * m.currentInfo.CgroupLimits.CPUQuotaCores * 1e9) // in nanoseconds

		var cpuUsage float64
		if totalAvailableTime > 0 {
			cpuUsage = (float64(cpuUsageDelta) / float64(totalAvailableTime)) * 100
		}
		if cpuUsage > 100.0 {
			cpuUsage = 100.0 // Cap at 100% to avoid small timing variations causing >100%
		}

		m.currentInfo.Usage.CPUTotalUsagePercent = cpuUsage
		m.currentInfo.Usage.CPUUsedCores = m.currentInfo.CgroupLimits.CPUQuotaCores * (cpuUsage / 100.0)
		m.lastCPUacctUsage = cgroupCPUUsage
	} else {
		// Fallback to host-level stats if not in a cgroup or files are missing
		newHostStats, _ := getSystemStats()
		if !m.lastTime.IsZero() && duration > 0 {
			lastIdle := m.lastStats.CPUIdle + m.lastStats.CPUIowait
			newIdle := newHostStats.CPUIdle + newHostStats.CPUIowait
			lastTotal := m.lastStats.CPUUser + m.lastStats.CPUNice + m.lastStats.CPUSystem + lastIdle + m.lastStats.CPUirq + m.lastStats.CPUSoftirq + m.lastStats.CPUSteal
			newTotal := newHostStats.CPUUser + newHostStats.CPUNice + newHostStats.CPUSystem + newIdle + newHostStats.CPUirq + newHostStats.CPUSoftirq + newHostStats.CPUSteal
			totalDelta := float64(newTotal - lastTotal)
			idleDelta := float64(newIdle - lastIdle)

			var cpuUsage float64
			if totalDelta > 0 {
				cpuUsage = (totalDelta - idleDelta) / totalDelta * 100
			}
			m.currentInfo.Usage.CPUTotalUsagePercent = cpuUsage
			m.currentInfo.Usage.CPUUsedCores = float64(runtime.NumCPU()) * (cpuUsage / 100.0)
		}
		m.lastStats = newHostStats
	}

	// --- Memory Usage Calculation ---
	ramTotal := m.currentInfo.CgroupLimits.MemoryLimitBytes
	ramUsed, err := getCgroupMemoryUsage()
	if err != nil {
		// Fallback to host stats if cgroup memory files are not available
		newSystemStats, _ := getSystemStats()
		ramTotal = newSystemStats.MemTotal
		if newSystemStats.MemAvailable > 0 {
			ramUsed = newSystemStats.MemTotal - newSystemStats.MemAvailable
		} else {
			ramUsed = newSystemStats.MemTotal - newSystemStats.MemFree - newSystemStats.Buffers - newSystemStats.Cached
		}
	}

	m.currentInfo.Usage.RAMTotalBytes = ramTotal
	m.currentInfo.Usage.RAMUsedBytes = ramUsed
	if ramTotal > 0 {
		m.currentInfo.Usage.RAMUsagePercent = (float64(ramUsed) / float64(ramTotal)) * 100
	}

	// --- Network and Disk I/O (from host, generally acceptable) ---
	newSystemStats, _ := getSystemStats()
	if duration > 0 {
		m.currentInfo.Usage.NetSentBps = float64(newSystemStats.NetBytesSent-m.lastStats.NetBytesSent) / duration
		m.currentInfo.Usage.NetRecvBps = float64(newSystemStats.NetBytesRecv-m.lastStats.NetBytesRecv) / duration
		m.currentInfo.Usage.DiskReadBps = float64(newSystemStats.DiskReadBytes-m.lastStats.DiskReadBytes) / duration
		m.currentInfo.Usage.DiskWriteBps = float64(newSystemStats.DiskWriteBytes-m.lastStats.DiskWriteBytes) / duration
	}
	m.lastStats = newSystemStats
	m.lastTime = newTime
}

type NetworkHardwareMonitor struct {
	dht       *dht.IpfsDHT
	peerStats map[string]PeerStatEntry
	mutex     sync.RWMutex
	ctx       context.Context
}

var discoveryKey cid.Cid

func init() {
	hash, err := multihash.Sum([]byte("grapthway-network-discovery-point"), multihash.SHA2_256, -1)
	if err != nil {
		log.Fatalf("FATAL: Failed to generate discovery key hash: %v", err)
	}
	discoveryKey = cid.NewCidV1(cid.Raw, hash)
}

func NewNetworkHardwareMonitor(ctx context.Context) *NetworkHardwareMonitor {
	return &NetworkHardwareMonitor{
		peerStats: make(map[string]PeerStatEntry),
		ctx:       ctx,
	}
}

func (nhm *NetworkHardwareMonitor) Start(dht *dht.IpfsDHT, statsChan <-chan *pubsub.Message) {
	nhm.dht = dht
	go nhm.listenForStats(statsChan)
	go nhm.discoverPeers()
}

func (nhm *NetworkHardwareMonitor) RemovePeer(peerID string) {
	nhm.mutex.Lock()
	defer nhm.mutex.Unlock()
	if _, exists := nhm.peerStats[peerID]; exists {
		log.Printf("MONITOR: Peer %s disconnected. Removing from hardware stats.", peerID)
		delete(nhm.peerStats, peerID)
	}
}

func (nhm *NetworkHardwareMonitor) listenForStats(statsChan <-chan *pubsub.Message) {
	log.Println("MONITOR: Listening to pre-subscribed network hardware stats topic.")
	for {
		select {
		case <-nhm.ctx.Done():
			return
		case msg := <-statsChan:
			var gossipPayload model.HardwareStatsGossip
			if err := json.Unmarshal(msg.Data, &gossipPayload); err != nil {
				log.Printf("MONITOR: Error unmarshalling hardware stats from peer: %v", err)
				continue
			}
			nhm.mergeStats(gossipPayload)
		}
	}
}

func (nhm *NetworkHardwareMonitor) discoverPeers() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-nhm.ctx.Done():
			return
		case <-ticker.C:
			if nhm.dht == nil {
				continue
			}
			log.Println("MONITOR: Announcing presence on DHT for peer discovery...")
			if err := nhm.dht.Provide(nhm.ctx, discoveryKey, true); err != nil {
				log.Printf("MONITOR: Error announcing presence on DHT: %v", err)
			}
		}
	}
}

func (nhm *NetworkHardwareMonitor) mergeStats(gossip model.HardwareStatsGossip) {
	nhm.mutex.Lock()
	defer nhm.mutex.Unlock()

	nhm.peerStats[gossip.SenderStats.NodeID] = PeerStatEntry{
		Stats:    gossip.SenderStats,
		LastSeen: time.Now(),
	}

	for nodeID, stats := range gossip.KnownPeers {
		if _, exists := nhm.peerStats[nodeID]; !exists {
			nhm.peerStats[nodeID] = PeerStatEntry{
				Stats:    stats,
				LastSeen: time.Now(),
			}
		}
	}
}

func (nhm *NetworkHardwareMonitor) GetPeerStats() map[string]model.HardwareInfo {
	nhm.mutex.RLock()
	defer nhm.mutex.RUnlock()

	statsCopy := make(map[string]model.HardwareInfo)
	for id, entry := range nhm.peerStats {
		statsCopy[id] = entry.Stats
	}
	return statsCopy
}

func (nhm *NetworkHardwareMonitor) GetGlobalStats(localStats model.HardwareInfo) model.GlobalHardwareStats {
	nhm.mutex.RLock()
	defer nhm.mutex.RUnlock()

	var global model.GlobalHardwareStats
	var totalCPUUsagePercent float64
	var totalRAMUsagePercent float64

	allStats := make(map[string]model.HardwareInfo)
	allStats[localStats.NodeID] = localStats

	for id, entry := range nhm.peerStats {
		if time.Since(entry.LastSeen) < 3*time.Minute {
			allStats[id] = entry.Stats
		}
	}

	for _, stats := range allStats {
		global.TotalNodes++
		if stats.Role == "server" {
			global.TotalServers++
		} else if stats.Role == "worker" {
			global.TotalWorkers++
		}

		if stats.CgroupLimits.MemoryLimitBytes > 0 {
			global.TotalRAMBytes += stats.CgroupLimits.MemoryLimitBytes
		} else if stats.Usage.RAMTotalBytes > 0 {
			global.TotalRAMBytes += stats.Usage.RAMTotalBytes
		}
		global.UsedRAMBytes += stats.Usage.RAMUsedBytes
		totalRAMUsagePercent += stats.Usage.RAMUsagePercent

		if stats.CgroupLimits.CPUQuotaCores > 0 {
			global.TotalAvailableCores += stats.CgroupLimits.CPUQuotaCores
		}
		global.TotalUsedCores += stats.Usage.CPUUsedCores
		totalCPUUsagePercent += stats.Usage.CPUTotalUsagePercent
	}

	if global.TotalNodes > 0 {
		global.AverageCPUUsage = totalCPUUsagePercent / float64(global.TotalNodes)
		global.AverageRAMUsage = totalRAMUsagePercent / float64(global.TotalNodes)
	}

	return global
}

func getSystemStats() (model.SystemStats, error) {
	var stats model.SystemStats
	parseProcStat(&stats)
	parseProcMeminfo(&stats)
	parseProcNetDev(&stats)
	parseProcDiskstats(&stats)
	return stats, nil
}

func getCgroupStats() model.CgroupStats {
	var stats model.CgroupStats
	// cgroup v1 paths
	if memLimit, err := readUintFromFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		stats.MemoryLimitBytes = memLimit
	}
	quota, errQuota := readIntFromFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	period, errPeriod := readUintFromFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if errQuota == nil && errPeriod == nil && period > 0 {
		if quota == -1 {
			stats.CPUQuotaCores = float64(runtime.NumCPU())
		} else {
			stats.CPUQuotaCores = float64(quota) / float64(period)
		}
	}

	// cgroup v2 paths (fallback)
	if stats.MemoryLimitBytes == 0 {
		if memLimit, err := readUintFromFile("/sys/fs/cgroup/memory.max"); err == nil {
			stats.MemoryLimitBytes = memLimit
		}
	}
	if stats.CPUQuotaCores == 0 {
		if maxStr, err := readStringFromFile("/sys/fs/cgroup/cpu.max"); err == nil {
			parts := strings.Fields(maxStr)
			if len(parts) == 2 && parts[0] != "max" {
				quota, _ := strconv.ParseFloat(parts[0], 64)
				period, _ := strconv.ParseFloat(parts[1], 64)
				if period > 0 {
					stats.CPUQuotaCores = quota / period
				}
			} else {
				stats.CPUQuotaCores = float64(runtime.NumCPU())
			}
		} else {
			// If no cgroup file is found, assume we can use all host CPUs
			stats.CPUQuotaCores = float64(runtime.NumCPU())
		}
	}
	return stats
}

func getCgroupMemoryUsage() (uint64, error) {
	// cgroup v1
	if usage, err := readUintFromFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil {
		return usage, nil
	}
	// cgroup v2
	if usage, err := readUintFromFile("/sys/fs/cgroup/memory.current"); err == nil {
		return usage, nil
	}
	return 0, os.ErrNotExist
}

func getCgroupV2CPUUsage() (uint64, error) {
	data, err := readStringFromFile("/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return 0, err
	}
	scanner := bufio.NewScanner(strings.NewReader(data))
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == "usage_usec" {
			usage, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return 0, err
			}
			return usage * 1000, nil // convert microseconds to nanoseconds
		}
	}
	return 0, os.ErrNotExist
}

func parseProcStat(stats *model.SystemStats) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) >= 8 {
				stats.CPUUser, _ = strconv.ParseUint(fields[1], 10, 64)
				stats.CPUNice, _ = strconv.ParseUint(fields[2], 10, 64)
				stats.CPUSystem, _ = strconv.ParseUint(fields[3], 10, 64)
				stats.CPUIdle, _ = strconv.ParseUint(fields[4], 10, 64)
				stats.CPUIowait, _ = strconv.ParseUint(fields[5], 10, 64)
				stats.CPUirq, _ = strconv.ParseUint(fields[6], 10, 64)
				stats.CPUSoftirq, _ = strconv.ParseUint(fields[7], 10, 64)
				if len(fields) >= 9 {
					stats.CPUSteal, _ = strconv.ParseUint(fields[8], 10, 64)
				}
			}
		}
	}
}

func parseProcMeminfo(stats *model.SystemStats) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		val, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		valBytes := val * 1024
		switch fields[0] {
		case "MemTotal:":
			stats.MemTotal = valBytes
		case "MemFree:":
			stats.MemFree = valBytes
		case "MemAvailable:":
			stats.MemAvailable = valBytes
		case "Buffers:":
			stats.Buffers = valBytes
		case "Cached:":
			stats.Cached = valBytes
		}
	}
}

func parseProcNetDev(stats *model.SystemStats) {
	file, err := os.Open("/proc/net/dev")
	if err != nil {
		return
	}
	defer file.Close()
	var totalRecv, totalSent uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, ":") || strings.Contains(line, "lo:") {
			continue
		}
		fields := strings.Fields(strings.Replace(line, ":", " ", -1))
		if len(fields) < 11 {
			continue
		}
		recv, _ := strconv.ParseUint(fields[1], 10, 64)
		sent, _ := strconv.ParseUint(fields[9], 10, 64)
		totalRecv += recv
		totalSent += sent
	}
	stats.NetBytesRecv = totalRecv
	stats.NetBytesSent = totalSent
}

func parseProcDiskstats(stats *model.SystemStats) {
	file, err := os.Open("/proc/diskstats")
	if err != nil {
		return
	}
	defer file.Close()
	var totalRead, totalWrite uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 10 && !strings.HasPrefix(fields[2], "loop") && !strings.HasPrefix(fields[2], "ram") {
			readSectors, _ := strconv.ParseUint(fields[5], 10, 64)
			writeSectors, _ := strconv.ParseUint(fields[9], 10, 64)
			totalRead += readSectors * 512
			totalWrite += writeSectors * 512
		}
	}
	stats.DiskReadBytes = totalRead
	stats.DiskWriteBytes = totalWrite
}

func readUintFromFile(path string) (uint64, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		// Handle "max" value in memory.max for cgroupv2
		if strings.TrimSpace(string(data)) == "max" {
			return 0, nil // 0 indicates no limit, we will fallback to host memory
		}
		return 0, err
	}
	return val, nil
}

func readIntFromFile(path string) (int64, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
}

func readStringFromFile(path string) (string, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
