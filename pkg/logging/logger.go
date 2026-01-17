package logging

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	json "github.com/json-iterator/go"

	"github.com/gorilla/websocket"
)

// RequestDetails captures the full details of an HTTP request.
type RequestDetails struct {
	Method  string      `json:"method,omitempty"`
	URL     string      `json:"url,omitempty"`
	Headers http.Header `json:"headers,omitempty"`
	Body    string      `json:"body,omitempty"`
}

// ResponseDetails captures the full details of an HTTP response.
type ResponseDetails struct {
	StatusCode int         `json:"status_code,omitempty"`
	Headers    http.Header `json:"headers,omitempty"`
	Body       string      `json:"body,omitempty"`
}

// PipelineStepLog captures the full data flow for a single pipeline step.
type PipelineStepLog struct {
	Service        string            `json:"service"`
	Field          string            `json:"field"`
	Path           string            `json:"path,omitempty"`
	Method         string            `json:"method,omitempty"`
	Operation      string            `json:"operation,omitempty"`
	Concurrent     bool              `json:"concurrent,omitempty"`
	Error          string            `json:"error,omitempty"`
	Assign         map[string]string `json:"assign,omitempty"`
	ArgsMapping    map[string]string `json:"argsMapping,omitempty"`
	Request        RequestDetails    `json:"request"`
	Response       ResponseDetails   `json:"response"`
	CallWorkflow   string            `json:"callWorkflow,omitempty"`
	Conditional    string            `json:"conditional,omitempty"`
	RetryCount     int               `json:"retryCount,omitempty"`
	IsRollbackStep bool              `json:"isRollbackStep,omitempty"`
}

type LogType string

const (
	GatewayLog LogType = "gateway"
	AdminLog   LogType = "admin"
	SchemaLog  LogType = "schema"
	LedgerLog  LogType = "ledger" // New log type for economic events
)

// SchemaChangeDetails captures detailed information about a schema update.
type SchemaChangeDetails struct {
	SchemaBefore       string `json:"schema_before,omitempty"`
	SchemaAfter        string `json:"schema_after,omitempty"`
	IsChanged          bool   `json:"is_changed"`
	Version            int    `json:"version"`
	BlueGreenSwap      bool   `json:"blue_green_swap_successful"`
	TriggeredBy        string `json:"triggered_by"`
	TriggeringRequest  string `json:"triggering_request"`
	FullRequestHeaders string `json:"full_request_headers"`
}

// AdminActivityDetails captures details about an admin action.
type AdminActivityDetails struct {
	AccessedBy string `json:"accessed_by"` // IP Address
	Request    string `json:"request_body"`
	Response   string `json:"response_body"`
}

// TransactionInfo captures details about a ledger transaction.
type TransactionInfo struct {
	TransactionID string  `json:"transactionId"`
	Type          string  `json:"type"`
	From          string  `json:"from"`
	To            string  `json:"to"`
	Amount        float64 `json:"amount"`
}

// LogEntry is the top-level structure for a single gateway transaction.
type LogEntry struct {
	Timestamp          time.Time             `json:"timestamp"`
	TraceID            string                `json:"trace_id"`
	LogType            LogType               `json:"log_type"`
	RequestAddress     string                `json:"request_address,omitempty"`
	ClientRequest      RequestDetails        `json:"client_request"`
	ClientResponse     ResponseDetails       `json:"client_response"`
	DownstreamRequest  RequestDetails        `json:"downstream_request,omitempty"`
	DownstreamResponse ResponseDetails       `json:"downstream_response,omitempty"`
	PreSuccessSteps    []PipelineStepLog     `json:"pre_success_steps,omitempty"`
	PreFailureSteps    []PipelineStepLog     `json:"pre_failure_steps,omitempty"`
	PostSuccessSteps   []PipelineStepLog     `json:"post_success_steps,omitempty"`
	PostFailureSteps   []PipelineStepLog     `json:"post_failure_steps,omitempty"`
	RollbackSuccess    []PipelineStepLog     `json:"rollback_success,omitempty"`
	RollbackFailure    []PipelineStepLog     `json:"rollback_failure,omitempty"`
	Subgraph           string                `json:"subgraph,omitempty"`
	UploadedFiles      []string              `json:"uploaded_files,omitempty"`
	FailureMessage     string                `json:"failure_message,omitempty"`
	User               string                `json:"user,omitempty"`
	Activity           string                `json:"activity,omitempty"`
	SchemaName         string                `json:"schema_name,omitempty"`
	SchemaChangeInfo   *SchemaChangeDetails  `json:"schema_change_info,omitempty"`
	AdminActivityInfo  *AdminActivityDetails `json:"admin_activity_info,omitempty"`
	TransactionInfo    *TransactionInfo      `json:"transaction_info,omitempty"` // New field for ledger events
	WorkflowInstanceID string                `json:"workflowInstanceId,omitempty"`
}

const (
	maxLogLineSize = 5 * 1024 * 1024   // 5 MB
	maxLogFileSize = 100 * 1024 * 1024 // 100 MB
	maxLogBackups  = 10                // Keep up to 10 rotated log files
)

type Logger struct {
	logDir           string
	retentionDays    int
	liveClients      map[*websocket.Conn]bool
	liveClientsMutex sync.RWMutex
	fileMutex        sync.Mutex
	logChannel       chan LogEntry
	stopChan         chan struct{}

	// Fields for optimizing file writes
	currentFile     *os.File
	currentFilePath string
}

func NewLogger(retentionDays int) *Logger {
	baseLogDir := os.Getenv("GRAPTHWAY_LOG_DIR")
	if baseLogDir == "" {
		baseLogDir = "logs"
	}
	log.Printf("Initializing logger in directory: %s", baseLogDir)

	logDirs := []string{baseLogDir,
		filepath.Join(baseLogDir, string(GatewayLog)),
		filepath.Join(baseLogDir, string(AdminLog)),
		filepath.Join(baseLogDir, string(SchemaLog)),
		filepath.Join(baseLogDir, string(LedgerLog)),
	}

	for _, dir := range logDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("FATAL: Could not create log directory %s: %v. Check permissions.", dir, err)
			return nil
		}
	}

	// --- MODIFICATION START ---
	// Dynamically calculate the log channel size based on available CPUs.
	// More CPUs mean more concurrent requests and thus a higher potential burst of log messages.
	// A larger channel acts as a buffer to prevent dropping logs during these bursts.
	numCPU := runtime.GOMAXPROCS(0)
	if numCPU == 0 {
		numCPU = 1
	}
	baseChannelSize := 10000                         // Increase the base size significantly from 1000.
	channelSize := baseChannelSize + (numCPU * 5000) // Add 5000 buffer slots per CPU.

	log.Printf("⚙️  Configuring logger with a channel size of %d for %d CPUs", channelSize, numCPU)
	// --- MODIFICATION END ---

	l := &Logger{
		logDir:        baseLogDir,
		retentionDays: retentionDays,
		liveClients:   make(map[*websocket.Conn]bool),
		logChannel:    make(chan LogEntry, channelSize), // Use the new dynamic size
		stopChan:      make(chan struct{}),
	}

	go l.processLogs()
	return l
}

func (l *Logger) Log(entry LogEntry) {
	select {
	case l.logChannel <- entry:
	default:
		log.Printf("WARN: Log channel is full. Dropping log entry for TraceID: %s", entry.TraceID)
	}
}

func (l *Logger) processLogs() {
	for {
		select {
		case entry := <-l.logChannel:
			l.writeLogToFile(entry)
		case <-l.stopChan:
			if l.currentFile != nil {
				l.currentFile.Close()
			}
			return
		}
	}
}

// rotate performs size-based log rotation.
func (l *Logger) rotate(filePath string) error {
	// 1. Close the current file if it's the one we're rotating.
	if l.currentFile != nil && l.currentFilePath == filePath {
		l.currentFile.Close()
		l.currentFile = nil
		l.currentFilePath = ""
	}

	// 2. Shift old backups.
	for i := maxLogBackups - 1; i > 0; i-- {
		oldPath := fmt.Sprintf("%s.%d", filePath, i)
		if _, err := os.Stat(oldPath); err == nil {
			newPath := fmt.Sprintf("%s.%d", filePath, i+1)
			os.Rename(oldPath, newPath)
		}
	}

	// 3. Rename the current log file to the first backup.
	if _, err := os.Stat(filePath); err == nil {
		os.Rename(filePath, filePath+".1")
	}

	return nil
}

func (l *Logger) writeLogToFile(entry LogEntry) {
	entryJSON, err := json.Marshal(entry)
	if err != nil {
		log.Printf("ERROR: Could not marshal log entry for TraceID %s: %v", entry.TraceID, err)
		return
	}

	l.fileMutex.Lock()
	defer l.fileMutex.Unlock()

	logSubDir := filepath.Join(l.logDir, string(entry.LogType))
	dateStr := entry.Timestamp.Format("2006-01-02")
	logFile := filepath.Join(logSubDir, dateStr+".log")

	// If the target log file is different from the currently open file, switch files.
	if l.currentFilePath != logFile {
		if l.currentFile != nil {
			l.currentFile.Close()
		}
		var err error
		l.currentFile, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("ERROR: Could not open log file %s: %v", logFile, err)
			l.currentFile = nil
			return
		}
		l.currentFilePath = logFile
	}

	// Perform rotation check if the file is open
	if l.currentFile != nil {
		stat, err := l.currentFile.Stat()
		if err == nil && stat.Size() > maxLogFileSize {
			if err := l.rotate(l.currentFilePath); err != nil {
				log.Printf("ERROR: Failed to rotate log file %s: %v", l.currentFilePath, err)
				return
			}
			// After rotation, open the new primary log file
			l.currentFile, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Printf("ERROR: Could not open new log file after rotation %s: %v", logFile, err)
				l.currentFile = nil
				return
			}
		}
	}

	if l.currentFile != nil {
		if _, err := l.currentFile.Write(append(entryJSON, '\n')); err != nil {
			log.Printf("ERROR: Could not write to log file %s: %v", l.currentFilePath, err)
		}
	}

	l.broadcastLog(entryJSON)
}

func (l *Logger) broadcastLog(entryJSON []byte) {
	l.liveClientsMutex.RLock()
	defer l.liveClientsMutex.RUnlock()

	for client := range l.liveClients {
		if err := client.WriteMessage(websocket.TextMessage, entryJSON); err != nil {
			go func(c *websocket.Conn) {
				l.liveClientsMutex.Lock()
				defer l.liveClientsMutex.Unlock()
				c.Close()
				delete(l.liveClients, c)
			}(client)
		}
	}
}

func (l *Logger) AddLiveClient(client *websocket.Conn) {
	l.liveClientsMutex.Lock()
	defer l.liveClientsMutex.Unlock()
	l.liveClients[client] = true
}

func (l *Logger) RemoveLiveClient(client *websocket.Conn) {
	l.liveClientsMutex.Lock()
	defer l.liveClientsMutex.Unlock()
	delete(l.liveClients, client)
}

func (l *Logger) SetRetention(days int) {
	l.retentionDays = days
}

func (l *Logger) StartCleanupRoutine() {
	ticker := time.NewTicker(24 * time.Hour)
	go func() {
		for range ticker.C {
			l.cleanupOldLogs()
		}
	}()
}

func (l *Logger) cleanupOldLogs() {
	cutoff := time.Now().AddDate(0, 0, -l.retentionDays)
	filepath.Walk(l.logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".log") {
			return nil
		}
		dateStr := strings.TrimSuffix(info.Name(), filepath.Ext(info.Name()))
		fileDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			return nil
		}
		if fileDate.Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
}

func (l *Logger) GetLogs(logType LogType, start, end string, max int) ([]LogEntry, error) {
	var startTime, endTime time.Time
	var err error
	if start != "" {
		startTime, err = time.Parse(time.RFC3339, start)
		if err != nil {
			return nil, err
		}
	}
	if end != "" {
		endTime, err = time.Parse(time.RFC3339, end)
		if err != nil {
			return nil, err
		}
	} else {
		endTime = time.Now()
	}

	logSubDir := filepath.Join(l.logDir, string(logType))

	// Find all log files for the relevant date range
	var relevantFiles []string
	cutoffDate := time.Now().AddDate(0, 0, -l.retentionDays-1)
	if !startTime.IsZero() {
		cutoffDate = startTime.AddDate(0, 0, -1)
	}

	for d := time.Now(); d.After(cutoffDate); d = d.AddDate(0, 0, -1) {
		dateStr := d.Format("2006-01-02")
		globPattern := filepath.Join(logSubDir, dateStr+".log*")
		matches, err := filepath.Glob(globPattern)
		if err == nil {
			relevantFiles = append(relevantFiles, matches...)
		}
	}

	// Sort files to read the newest ones first (e.g., .log, then .log.1, .log.2)
	sort.Slice(relevantFiles, func(i, j int) bool {
		return relevantFiles[i] > relevantFiles[j]
	})

	var logs []LogEntry
	for _, file := range relevantFiles {
		if max > 0 && len(logs) >= max {
			break
		}
		if err := l.readLogFile(file, startTime, endTime, &logs, max); err != nil {
			log.Printf("WARN: Could not read log file '%s': %v", file, err)
			continue
		}
	}

	sort.Slice(logs, func(i, j int) bool { return logs[j].Timestamp.Before(logs[i].Timestamp) })
	if max > 0 && len(logs) > max {
		logs = logs[:max]
	}
	return logs, nil
}

func (l *Logger) readLogFile(path string, start, end time.Time, logs *[]LogEntry, max int) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, maxLogLineSize)

	// Read file from bottom to top to get recent logs first
	var lines [][]byte
	for scanner.Scan() {
		lines = append(lines, scanner.Bytes())
	}

	for i := len(lines) - 1; i >= 0; i-- {
		if max > 0 && len(*logs) >= max {
			return nil
		}

		line := lines[i]
		if len(line) == 0 {
			continue
		}

		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		if !start.IsZero() && entry.Timestamp.Before(start) {
			// Since we are reading backwards in time, we can stop early
			return nil
		}
		if !end.IsZero() && entry.Timestamp.After(end) {
			continue
		}

		*logs = append(*logs, entry)
	}

	return scanner.Err()
}

// maskSensitive masks a string, showing the first and last `keep` characters.
func maskSensitive(data string, keep int) string {
	if len(data) <= keep*2 {
		return strings.Repeat("*", 8)
	}
	return data[:keep] + "****************" + data[len(data)-keep:]
}

// Sanitize removes or masks sensitive information from a log entry before it's written.
func (le *LogEntry) Sanitize() {
	// Sanitize wallet creation response in the response body.
	if le.LogType == AdminLog && strings.Contains(le.ClientRequest.URL, "/wallet/create") && le.ClientResponse.Body != "" {
		var responseData map[string]interface{}
		if err := json.Unmarshal([]byte(le.ClientResponse.Body), &responseData); err == nil {
			if pk, ok := responseData["privateKey"].(string); ok && pk != "" {
				responseData["privateKey"] = maskSensitive(pk, 4)
			}
			if pub, ok := responseData["publicKey"].(string); ok && pub != "" {
				responseData["publicKey"] = maskSensitive(pub, 8)
			}
			sanitizedBody, _ := json.Marshal(responseData)
			le.ClientResponse.Body = string(sanitizedBody)
		}
	}

	// Sanitize sensitive request headers.
	headersToSanitize := []string{"Authorization", "X-Grapthway-Admin-Signature", "X-Grapthway-User-Signature"}
	for _, h := range headersToSanitize {
		if val := le.ClientRequest.Headers.Get(h); val != "" {
			le.ClientRequest.Headers.Set(h, maskSensitive(val, 8))
		}
	}

	// Sanitize private keys from request bodies (e.g., login from dashboard).
	if le.ClientRequest.Body != "" {
		// Use a regex to be more robust against different JSON formatting.
		re := regexp.MustCompile(`("privateKey"\s*:\s*)"([^"]+)"`)
		le.ClientRequest.Body = re.ReplaceAllString(le.ClientRequest.Body, `$1"`+maskSensitive("...", 4)+`"`)
	}
}

func (l *Logger) Stop() {
	close(l.stopChan)
}
