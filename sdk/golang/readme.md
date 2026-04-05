# Grapthway Go Client SDK Documentation

Complete Go SDK for interacting with the Grapthway Protocol Network with **multi-node support, automatic failover, and latency-based routing**.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Multi-Node Configuration](#multi-node-configuration)
- [Account Management](#account-management)
- [Configuration & Services](#configuration--services)
- [Wallet Operations](#wallet-operations)
- [Token Operations](#token-operations)
- [Staking](#staking)
- [Network Information](#network-information)
- [Advanced Features](#advanced-features)
- [Middleware](#middleware)
- [API Reference](#api-reference)
- [Complete Example](#complete-example)

---

## Installation

```bash
go get github.com/your-org/grapthway
```

**Required Dependencies:**
```bash
go get github.com/ethereum/go-ethereum/crypto
go get github.com/ethereum/go-ethereum/crypto/secp256k1
```

---

## Quick Start

### Basic Setup (Single Node)

```go
package main

import (
    "log"
    "your-module-path/grapthway"
)

func main() {
    // Single node (backward compatible)
    client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
        NodeURL:    "http://localhost:5000",
        PrivateKey: "your_private_key_hex",
        Label:      "My Account",
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

### Multi-Node Setup (Recommended)

```go
// Multiple nodes with automatic failover
client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
    NodeURLs: []string{
        "https://node1.grapthway.com",
        "https://node2.grapthway.com",
        "https://node3.grapthway.com",
    },
    PrivateKey: "your_private_key_hex",
    Label:      "Production Account",
})
if err != nil {
    log.Fatal(err)
}
```

### Environment Variables (Recommended)

**Using single node:**
```bash
export GRAPTHWAY_NODE_URL=https://node1.grapthway.com
```

**Using multiple nodes:**
```bash
export GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
export GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
export GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
```

Then initialize without config:
```go
// Automatically loads all GRAPTHWAY_NODE_URL_* from environment
client, err := grapthway.NewGrapthwayClient(nil)
if err != nil {
    log.Fatal(err)
}
```

---

## Multi-Node Configuration

### How It Works

The SDK includes an intelligent **NodePoolManager** that:

1. ✅ **Pings all nodes** on initialization to measure latency
2. ✅ **Sorts nodes by latency** (fastest first)
3. ✅ **Automatically fails over** to backup nodes if primary fails
4. ✅ **Health checks every 30 seconds** to re-evaluate node performance
5. ✅ **Marks nodes unhealthy** after 3 consecutive failures
6. ✅ **Exponential backoff** for retries (1s, 2s, 4s...)
7. ✅ **Automatic recovery** when nodes come back online

### Node Pool Features

```go
// Check node health status
stats := client.GetNodeStats()
fmt.Printf("Total nodes: %d\n", stats.TotalNodes)
fmt.Printf("Healthy nodes: %d\n", stats.HealthyNodes)

for _, node := range stats.Nodes {
    status := "UP"
    if !node.IsHealthy {
        status = "DOWN"
    }
    latency := "TIMEOUT"
    if node.Latency != nil {
        latency = fmt.Sprintf("%dms", *node.Latency)
    }
    fmt.Printf("%s: %s (%s)\n", node.URL, latency, status)
}

// Force health check (useful for debugging)
stats, err := client.CheckNodeHealth()
if err != nil {
    log.Fatal(err)
}
```

### Console Output Example

When you initialize the client, you'll see:

```
✅ Loaded 3 Grapthway node(s) from environment
🔍 Checking latency for all nodes...
  ✅ #1: https://node1.grapthway.com - 45ms
  ✅ #2: https://node2.grapthway.com - 67ms
  ✅ #3: https://node3.grapthway.com - 89ms
```

If a node fails:
```
⚠️ Node https://node1.grapthway.com marked as unhealthy after 3 failures
🔄 Retry attempt 2/3 using https://node2.grapthway.com
```

### Retry Behavior

```go
// All requests automatically retry up to 3 times
_, err := client.Transfer("0x...", 10.0)
// If node1 fails, automatically tries node2, then node3

// Custom retry count (advanced)
result, err := client.request("/endpoint", &grapthway.RequestOptions{
    MaxRetries: 5, // Override default of 3
})
```

---

## Account Management

The SDK includes a powerful account management system with thread-safe operations.

### Adding Accounts

```go
// Get account manager
accountManager := client.GetAccountManager()

// Add a new account
account, err := accountManager.AddAccount(
    "your_private_key_hex",
    "Development Account", // Optional label
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Created account: %s\n", account.Address)
```

### Managing Multiple Accounts

```go
accountManager := client.GetAccountManager()

// Add multiple accounts
account1, _ := accountManager.AddAccount("private_key_1", "Trading Account")
account2, _ := accountManager.AddAccount("private_key_2", "Service Account")

// Select an account
err := accountManager.SelectAccount(account1.ID)
if err != nil {
    log.Fatal(err)
}

// Get current account
currentAccount, err := accountManager.GetCurrentAccount()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Current account: %s\n", currentAccount.Label)

// Update account label
err = accountManager.UpdateAccountLabel(account1.ID, "Production Trading")
if err != nil {
    log.Fatal(err)
}

// Remove an account
err = accountManager.RemoveAccount(account2.ID)
if err != nil {
    log.Fatal(err)
}

// Clear all accounts (use with caution!)
accountManager.ClearAccounts()
```

### Account Structure

```go
type Account struct {
    ID           string    // Unique account identifier
    PrivateKey   string    // Hex-encoded private key
    Address      string    // Ethereum-compatible address
    PublicKeyHex string    // Hex-encoded public key
    Label        string    // Human-readable label
    CreatedAt    time.Time // Creation timestamp
}
```

---

## Configuration & Services

### Publishing Service Configuration

```go
config := map[string]interface{}{
    "service": "my-graphql-api",
    "url":     "https://api.myservice.com",
    "type":    "graphql",
    "subgraph": "main",
    "schema": `
        type Query {
            hello: String
        }
    `,
    // Optional: Middleware pipelines
    "pipelines": map[string]interface{}{
        "Query.hello": map[string]interface{}{
            "pre": []map[string]interface{}{
                {
                    "service": "auth-service",
                    "field":   "validateToken",
                    "argsMapping": map[string]string{
                        "token": "$headers.authorization",
                    },
                },
            },
        },
    },
}

result, err := client.PublishConfig(config)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Service published: %v\n", result["service"])
```

### Service Heartbeat

Keep your service registered and healthy:

```go
import "time"

// Send heartbeat every 15 seconds
ticker := time.NewTicker(15 * time.Second)
defer ticker.Stop()

for range ticker.C {
    result, err := client.Heartbeat(map[string]interface{}{
        "service":   "my-graphql-api",
        "url":       "https://api.myservice.com",
        "type":      "graphql",
        "subgraph":  "main",
        "configCid": "QmXxxx...", // From publish response
    })
    
    if err != nil {
        log.Printf("Heartbeat failed: %v", err)
        continue
    }
    
    fmt.Println("Heartbeat sent successfully")
}
```

### Fetching Network Configuration

```go
// Get all registered services
services, err := client.GetServices()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Services: %+v\n", services)

// Get all pipelines
pipelines, err := client.GetPipelines()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Pipelines: %+v\n", pipelines)

// Get schema information
schema, err := client.GetSchema()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Schema: %+v\n", schema)
```

---

## Wallet Operations

### Balance & Nonce

```go
accountManager := client.GetAccountManager()
account, _ := accountManager.GetCurrentAccount()

// Get balance
balance, err := client.GetBalance(account.Address)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Balance: %.2f GCU\n", balance)

// Get nonce (for transaction ordering)
nonce, err := client.GetNonce(account.Address)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Next nonce: %d\n", nonce+1)
```

### Transferring GCU

```go
// Simple transfer
result, err := client.Transfer(
    "0xRecipientAddress",
    25.5, // Amount in GCU
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Transaction ID: %v\n", result["transactionId"])
fmt.Printf("Assigned Nonce: %v\n", result["assignedNonce"])
```

### Transaction History

```go
// Get recent transactions
history, err := client.GetTransactionHistory(account.Address, &grapthway.TransactionHistoryOptions{
    Page:  1,
    Limit: 20,
})
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total transactions: %v\n", history["total"])
fmt.Printf("Transactions: %+v\n", history["transactions"])

// With time filtering (last 24 hours)
yesterday := time.Now().Add(-24 * time.Hour).Format(time.RFC3339)
now := time.Now().Format(time.RFC3339)

recentHistory, err := client.GetTransactionHistory(account.Address, &grapthway.TransactionHistoryOptions{
    Page:  1,
    Limit: 10,
    Start: yesterday,
    End:   now,
})
```

### Allowances (Delegated Spending)

```go
// Set allowance for a spender
result, err := client.SetAllowance(
    "0xSpenderAddress",
    100.0, // Allow spending up to 100 GCU
)
if err != nil {
    log.Fatal(err)
}

// Check allowance
allowance, err := client.GetAllowance(
    account.Address,
    "0xSpenderAddress",
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Allowance: %v GCU\n", allowance["allowance"])

// Remove allowance
result, err = client.RemoveAllowance("0xSpenderAddress")
if err != nil {
    log.Fatal(err)
}
```

---

## Token Operations

### Creating a Token

```go
tokenMetadata := map[string]interface{}{
    "name":          "My Token",
    "symbol":        "MTK",
    "decimals":      18,
    "totalSupply":   1000000, // 1 million tokens
    "tokenType":     "FUNGIBLE", // or "RWA", "NFT"
    "burnConfig": map[string]interface{}{
        "enabled":        true,
        "manualBurn":     true,
        "burnRatePerTx":  0, // No automatic burn
    },
    "mintConfig": map[string]interface{}{
        "enabled":        true,
        "manualMint":     true,
        "mintRatePerTx":  0, // No automatic mint
    },
    "metadata": `{
        "description": "My custom token",
        "website": "https://mytoken.com"
    }`,
}

result, err := client.CreateToken(tokenMetadata)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Token created: %+v\n", result["tokenMetadata"])
```

### Transferring Tokens

```go
result, err := client.TransferToken(
    "0xTokenAddress",
    "0xRecipientAddress",
    100.5, // Amount in token units
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Token transfer result: %+v\n", result)
```

### Token Information

```go
// Get token details
tokenInfo, err := client.GetTokenInfo("0xTokenAddress")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Token: %v %v\n", tokenInfo["name"], tokenInfo["symbol"])
fmt.Printf("Total Supply: %v\n", tokenInfo["totalSupply"])

// Get token balance
balance, err := client.GetTokenBalance(
    "0xTokenAddress",
    account.Address,
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Balance: %v\n", balance["balance"])

// Get all owned tokens
ownedTokens, err := client.GetOwnedTokens(account.Address)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("You own %v different tokens\n", ownedTokens["count"])
```

### Token Allowances

```go
// Approve spender for tokens
result, err := client.SetTokenAllowance(
    "0xTokenAddress",
    "0xSpenderAddress",
    50.0, // Allow spending 50 tokens
)
if err != nil {
    log.Fatal(err)
}

// Check token allowance
allowance, err := client.GetTokenAllowance(
    "0xTokenAddress",
    account.Address,
    "0xSpenderAddress",
)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Token allowance: %v\n", allowance["allowance"])

// Revoke allowance
result, err = client.DeleteTokenAllowance(
    "0xTokenAddress",
    "0xSpenderAddress",
)
if err != nil {
    log.Fatal(err)
}
```

### Minting & Burning (Creator Only)

```go
// Mint new tokens
result, err := client.MintToken(
    "0xTokenAddress",
    "0xRecipientAddress",
    1000.0, // Mint 1000 tokens
)
if err != nil {
    log.Fatal(err)
}

// Burn your tokens
result, err = client.BurnToken(
    "0xTokenAddress",
    500.0, // Burn 500 tokens
)
if err != nil {
    log.Fatal(err)
}
```

### Token Configuration

```go
// Update mint configuration
result, err := client.SetTokenMintConfig("0xTokenAddress", map[string]interface{}{
    "enabled":     true,
    "rate":        1.5,  // 1.5% mint per transaction
    "manualMint":  true,
})
if err != nil {
    log.Fatal(err)
}

// Update burn configuration
result, err = client.SetTokenBurnConfig("0xTokenAddress", map[string]interface{}{
    "enabled":     true,
    "rate":        0.5,  // 0.5% burn per transaction
    "manualBurn":  true,
})
if err != nil {
    log.Fatal(err)
}

// Lock configuration (permanent!)
result, err = client.LockTokenConfig("0xTokenAddress")
if err != nil {
    log.Fatal(err)
}
```

### Token History

```go
// Get all token transactions for an address
history, err := client.GetTokenHistory(account.Address, &grapthway.TokenHistoryOptions{
    Page:  1,
    Limit: 20,
})
if err != nil {
    log.Fatal(err)
}

// Filter by specific token
tokenHistory, err := client.GetTokenHistory(account.Address, &grapthway.TokenHistoryOptions{
    TokenAddress: "0xTokenAddress",
    Page:         1,
    Limit:        10,
})
```

---

## Staking

### Checking Staking Status

```go
stakingInfo, err := client.GetStakingStatus(account.Address)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total Staked: %v GCU\n", stakingInfo["totalStake"])
fmt.Printf("Allocations: %+v\n", stakingInfo["allocations"])
fmt.Printf("Unbonding: %+v\n", stakingInfo["unbonding"])
```

### Depositing & Withdrawing Stake

```go
// Deposit to staking pool
result, err := client.StakeDeposit(100.0) // Deposit 100 GCU
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Stake deposited: %+v\n", result)

// Withdraw from staking pool (starts unbonding period)
result, err = client.StakeWithdraw(50.0) // Withdraw 50 GCU
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Stake withdrawal initiated: %+v\n", result)
```

### Assigning Stake to Nodes

```go
// Assign stake to support a node
result, err := client.StakeAssign(
    "12D3KooWXxxx...", // Node Peer ID
    25.0,              // Assign 25 GCU
)
if err != nil {
    log.Fatal(err)
}

// Unassign stake (node must be offline)
result, err = client.StakeUnassign("12D3KooWXxxx...")
if err != nil {
    log.Fatal(err)
}
```

---

## Network Information

### Gateway Status

```go
status, err := client.GetGatewayStatus()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Uptime: %v\n", status["uptime"])
fmt.Printf("Node ID: %v\n", status["node_id"])
fmt.Printf("Total Services: %v\n", status["total_services"])
fmt.Printf("Connected Peers: %v\n", len(status["peers"].([]interface{})))
```

### Hardware Stats

```go
stats, err := client.GetHardwareStats()
if err != nil {
    log.Fatal(err)
}

// Local node stats
local := stats["local"].(map[string]interface{})
usage := local["usage"].(map[string]interface{})
fmt.Printf("CPU Usage: %.2f%%\n", usage["cpu_total_usage_percent"])
fmt.Printf("RAM Usage: %.2f%%\n", usage["ram_usage_percent"])

// Global network stats
global := stats["global"].(map[string]interface{})
fmt.Printf("Total Nodes: %v\n", global["total_nodes"])
fmt.Printf("Network CPU: %.2f%%\n", global["average_cpu_usage_percent"])
```

---

## Advanced Features

### Workflow Monitoring

```go
// Get workflow instances
workflows, err := client.GetWorkflowMonitoring(map[string]string{
    "workflowName": "data-processing",
    "status":       "RUNNING", // RUNNING, COMPLETED, FAILED
})
if err != nil {
    log.Fatal(err)
}

workflowList := workflows["workflows"].([]interface{})
for _, wf := range workflowList {
    workflow := wf.(map[string]interface{})
    fmt.Printf("Workflow: %v\n", workflow["workflowName"])
    fmt.Printf("Status: %v\n", workflow["status"])
    fmt.Printf("Step: %v\n", workflow["currentStep"])
}
```

### Logging

```go
// Get gateway logs (your requests)
gatewayLogs, err := client.GetLogs("gateway", &grapthway.LogOptions{
    Start: time.Now().Add(-1 * time.Hour).Format(time.RFC3339), // Last hour
    Max:   100,
})
if err != nil {
    log.Fatal(err)
}

// Get ledger logs (your transactions)
ledgerLogs, err := client.GetLogs("ledger", &grapthway.LogOptions{
    Max: 50,
})
if err != nil {
    log.Fatal(err)
}
```

### Admin Operations (Node Operators Only)

```go
// Create a new wallet
wallet, err := client.CreateWallet()
if err != nil {
    log.Fatal(err)
}
fmt.Printf("New wallet: %v\n", wallet["address"])
fmt.Printf("Private key: %v\n", wallet["privateKey"]) // SAVE THIS!

// Get all logs
allLogs, err := client.GetAdminLogs("gateway", &grapthway.LogOptions{
    Max: 1000,
})
if err != nil {
    log.Fatal(err)
}

// Get all workflows
allWorkflows, err := client.GetAdminWorkflowMonitoring(map[string]string{})
if err != nil {
    log.Fatal(err)
}
```

---

## Middleware

### HTTP Middleware for Service Verification

Verify that requests to your service come from **authorized Grapthway nodes only**.

#### Basic Setup (Auto-detect from environment)

```go
package main

import (
    "log"
    "net/http"
    "your-module-path/grapthway"
)

func main() {
    // Create middleware (automatically reads GRAPTHWAY_NODE_URL_* from env)
    grapthwayAuth := grapthway.GrapthwayMiddleware(nil)

    // Apply to protected routes
    http.Handle("/api/protected", grapthwayAuth(http.HandlerFunc(protectedHandler)))

    log.Println("Server starting on :3000")
    log.Fatal(http.ListenAndServe(":3000", nil))
}

func protectedHandler(w http.ResponseWriter, r *http.Request) {
    // Only verified Grapthway nodes can access this
    nodeAddress := r.Header.Get("X-Grapthway-Verified-Address")
    log.Printf("Request from verified node: %s", nodeAddress)
    
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(`{"message": "Authenticated!"}`))
}
```

#### Explicit Node Whitelist

```go
// Manually specify allowed nodes
grapthwayAuth := grapthway.GrapthwayMiddleware(&grapthway.MiddlewareConfig{
    NodeURLs: []string{
        "https://node1.grapthway.com",
        "https://node2.grapthway.com",
        "https://node3.grapthway.com",
    },
})

http.Handle("/api/protected", grapthwayAuth(http.HandlerFunc(protectedHandler)))
```

#### Console Output

When middleware initializes:
```
🔒 Grapthway middleware initialized with 3 allowed node(s)
 ✅ node1.grapthway.com
 ✅ node2.grapthway.com
 ✅ node3.grapthway.com
```

When requests arrive:
```
✅ Request signature verified from Grapthway node: 0x123...
```

When unauthorized requests are blocked:
```
⚠️ Request from unauthorized node. Origin: https://evil.com, IP: 1.2.3.4
```

#### Accessing Node Information

```go
func protectedHandler(w http.ResponseWriter, r *http.Request) {
    // Node info attached to request headers
    verified := r.Header.Get("X-Grapthway-Verified")
    nodeAddress := r.Header.Get("X-Grapthway-Verified-Address")
    
    if verified == "true" {
        log.Printf("Verified request from node: %s", nodeAddress)
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(`{"message": "Request processed", "nodeAddress": "` + nodeAddress + `"}`))
}
```

#### Environment Setup for Middleware

**Development (.env):**
```bash
export GRAPTHWAY_NODE_URL_1=http://localhost:5000
export GRAPTHWAY_NODE_URL_2=http://localhost:5001
```

**Production (.env.production):**
```bash
export GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
export GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
export GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
```

---

## API Reference

### Client Initialization

```go
func NewGrapthwayClient(config *ClientConfig) (*GrapthwayClient, error)

type ClientConfig struct {
    // Single node (backward compatible)
    NodeURL    string
    
    // Multiple nodes (recommended)
    NodeURLs   []string
    
    // Account configuration
    PrivateKey string
    Label      string
}
```

**Examples:**
```go
// From environment (auto-loads GRAPTHWAY_NODE_URL_*)
client, err := grapthway.NewGrapthwayClient(nil)

// Single node
client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
    NodeURL: "http://localhost:5000",
})

// Multiple nodes with failover
client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
    NodeURLs: []string{
        "https://node1.grapthway.com",
        "https://node2.grapthway.com",
    },
})
```

### Node Pool Types

```go
type NodeStats struct {
    URL       string
    Latency   *int64    // Milliseconds, nil if timeout
    IsHealthy bool
    Failures  int
    LastCheck time.Time
}

type PoolStats struct {
    Nodes        []NodeStats
    TotalNodes   int
    HealthyNodes int
}
```

### Node Pool Methods

```go
// Get node statistics
func (gc *GrapthwayClient) GetNodeStats() *PoolStats

// Force health check
func (gc *GrapthwayClient) CheckNodeHealth() (*PoolStats, error)
```

### Account Manager Methods

```go
func (am *AccountManager) AddAccount(privateKeyHex, label string) (*Account, error)
func (am *AccountManager) RemoveAccount(accountID string) error
func (am *AccountManager) SelectAccount(accountID string) error
func (am *AccountManager) GetCurrentAccount() (*Account, error)
func (am *AccountManager) UpdateAccountLabel(accountID, label string) error
func (am *AccountManager) ClearAccounts()
```

### Configuration Methods

```go
func (gc *GrapthwayClient) PublishConfig(config map[string]interface{}) (map[string]interface{}, error)
func (gc *GrapthwayClient) Heartbeat(payload map[string]interface{}) (map[string]interface{}, error)
```

### Wallet Methods

```go
func (gc *GrapthwayClient) GetBalance(address string) (float64, error)
func (gc *GrapthwayClient) GetNonce(address string) (int64, error)
func (gc *GrapthwayClient) Transfer(to string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetTransactionHistory(address string, options *TransactionHistoryOptions) (map[string]interface{}, error)
func (gc *GrapthwayClient) SetAllowance(spender string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) RemoveAllowance(spender string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetAllowance(owner, spender string) (map[string]interface{}, error)
```

### Token Methods

```go
func (gc *GrapthwayClient) CreateToken(tokenMetadata map[string]interface{}) (map[string]interface{}, error)
func (gc *GrapthwayClient) TransferToken(tokenAddress, to string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetTokenInfo(tokenAddress string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetTokenBalance(tokenAddress, owner string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetOwnedTokens(owner string) (map[string]interface{}, error)
func (gc *GrapthwayClient) SetTokenAllowance(tokenAddress, spender string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) DeleteTokenAllowance(tokenAddress, spender string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetTokenAllowance(tokenAddress, owner, spender string) (map[string]interface{}, error)
func (gc *GrapthwayClient) MintToken(tokenAddress, to string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) BurnToken(tokenAddress string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) SetTokenMintConfig(tokenAddress string, config map[string]interface{}) (map[string]interface{}, error)
func (gc *GrapthwayClient) SetTokenBurnConfig(tokenAddress string, config map[string]interface{}) (map[string]interface{}, error)
func (gc *GrapthwayClient) LockTokenConfig(tokenAddress string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetTokenHistory(address string, options *TokenHistoryOptions) (map[string]interface{}, error)
```

### Staking Methods

```go
func (gc *GrapthwayClient) GetStakingStatus(ownerAddress string) (map[string]interface{}, error)
func (gc *GrapthwayClient) StakeDeposit(amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) StakeWithdraw(amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) StakeAssign(nodePeerID string, amount float64) (map[string]interface{}, error)
func (gc *GrapthwayClient) StakeUnassign(nodePeerID string) (map[string]interface{}, error)
```

### Network Methods

```go
func (gc *GrapthwayClient) GetGatewayStatus() (map[string]interface{}, error)
func (gc *GrapthwayClient) GetHardwareStats() (map[string]interface{}, error)
func (gc *GrapthwayClient) GetServices() (map[string]interface{}, error)
func (gc *GrapthwayClient) GetPipelines() (map[string]interface{}, error)
func (gc *GrapthwayClient) GetSchema() (map[string]interface{}, error)
```

### Workflow & Logging

```go
func (gc *GrapthwayClient) GetWorkflowMonitoring(filters map[string]string) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetLogs(logType string, options *LogOptions) (map[string]interface{}, error)
```

### Admin Methods

```go
func (gc *GrapthwayClient) CreateWallet() (map[string]interface{}, error)
func (gc *GrapthwayClient) GetAdminLogs(logType string, options *LogOptions) (map[string]interface{}, error)
func (gc *GrapthwayClient) GetAdminWorkflowMonitoring(filters map[string]string) (map[string]interface{}, error)
```

### Middleware

```go
func GrapthwayMiddleware(config *MiddlewareConfig) func(http.Handler) http.Handler

type MiddlewareConfig struct {
    NodeURLs []string // Allowed node URLs
}
```

---

## Complete Example

Here's a production-ready example with multi-node setup:

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "time"
    
    "your-module-path/grapthway"
)

// ========================================
// 1. SERVICE SETUP
// ========================================

func main() {
    // Start HTTP service in goroutine
    go startHTTPService()
    
    // Run client operations
    if err := runClientOperations(); err != nil {
        log.Fatal(err)
    }
    
    // Keep main running
    select {}
}

func startHTTPService() {
    // Create Grapthway middleware with whitelist
    grapthwayAuth := grapthway.GrapthwayMiddleware(&grapthway.MiddlewareConfig{
        NodeURLs: []string{
            "https://node1.grapthway.com",
            "https://node2.grapthway.com",
            "https://node3.grapthway.com",
        },
    })

    // Protected endpoint (only accessible by whitelisted Grapthway nodes)
    http.Handle("/api/process", grapthwayAuth(http.HandlerFunc(processHandler)))

    log.Println("Service listening on port 3000")
    if err := http.ListenAndServe(":3000", nil); err != nil {
        log.Fatal(err)
    }
}

func processHandler(w http.ResponseWriter, r *http.Request) {
    nodeAddress := r.Header.Get("X-Grapthway-Verified-Address")
    log.Printf("Processing request from node: %s", nodeAddress)
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "result":      "processed",
        "nodeAddress": nodeAddress,
    })
}

// ========================================
// 2. CLIENT OPERATIONS
// ========================================

func runClientOperations() error {
    // Initialize client with multiple nodes
    client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
        NodeURLs: []string{
            "https://node1.grapthway.com",
            "https://node2.grapthway.com",
            "https://node3.grapthway.com",
        },
    })
    if err != nil {
        return fmt.Errorf("failed to create client: %w", err)
    }

    // Add accounts
    accountManager := client.GetAccountManager()
    
    tradingAccount, err := accountManager.AddAccount(
        os.Getenv("PRIVATE_KEY_1"),
        "Trading Account",
    )
    if err != nil {
        return fmt.Errorf("failed to add trading account: %w", err)
    }
    
    serviceAccount, err := accountManager.AddAccount(
        os.Getenv("PRIVATE_KEY_2"),
        "Service Account",
    )
    if err != nil {
        return fmt.Errorf("failed to add service account: %w", err)
    }

    // Select trading account
    if err := accountManager.SelectAccount(tradingAccount.ID); err != nil {
        return err
    }

    // Check node health
    log.Println("Checking node health...")
    nodeStats, err := client.CheckNodeHealth()
    if err != nil {
        return fmt.Errorf("failed to check node health: %w", err)
    }
    log.Printf("Using %d/%d healthy nodes", nodeStats.HealthyNodes, nodeStats.TotalNodes)

    // Check balance
    balance, err := client.GetBalance(tradingAccount.Address)
    if err != nil {
        return fmt.Errorf("failed to get balance: %w", err)
    }
    log.Printf("Balance: %.2f GCU", balance)

    // Publish service configuration
    log.Println("Publishing service configuration...")
    _, err = client.PublishConfig(map[string]interface{}{
        "service":   "my-api",
        "url":       "https://api.example.com",
        "type":      "rest",
        "subgraph":  "main",
        "path":      "/api",
        "configCid": "QmXxx...",
    })
    if err != nil {
        return fmt.Errorf("failed to publish config: %w", err)
    }

    // Start heartbeat in goroutine
    go startHeartbeat(client)

    // Transfer GCU with automatic failover
    if balance > 10 {
        log.Println("Transferring 5 GCU to service account...")
        tx, err := client.Transfer(serviceAccount.Address, 5.0)
        if err != nil {
            return fmt.Errorf("failed to transfer: %w", err)
        }
        log.Printf("Transferred 5 GCU: %v", tx["transactionId"])
    }

    // Create a token
    log.Println("Creating token...")
    token, err := client.CreateToken(map[string]interface{}{
        "name":         "MyToken",
        "symbol":       "MTK",
        "decimals":     18,
        "totalSupply":  1000000,
        "tokenType":    "FUNGIBLE",
        "burnConfig": map[string]interface{}{
            "enabled":    true,
            "manualBurn": true,
        },
        "mintConfig": map[string]interface{}{
            "enabled":    true,
            "manualMint": true,
        },
        "metadata": `{"description": "Production token"}`,
    })
    if err != nil {
        return fmt.Errorf("failed to create token: %w", err)
    }
    
    tokenMeta := token["tokenMetadata"].(map[string]interface{})
    log.Printf("Token created: %v", tokenMeta["symbol"])

    // Stake GCU
    log.Println("Depositing stake...")
    _, err = client.StakeDeposit(50.0)
    if err != nil {
        return fmt.Errorf("failed to stake: %w", err)
    }
    
    staking, err := client.GetStakingStatus(tradingAccount.Address)
    if err != nil {
        return fmt.Errorf("failed to get staking status: %w", err)
    }
    log.Printf("Total staked: %v GCU", staking["totalStake"])

    // Monitor node stats periodically
    go monitorNodeStats(client)

    return nil
}

func startHeartbeat(client *grapthway.GrapthwayClient) {
    ticker := time.NewTicker(15 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        _, err := client.Heartbeat(map[string]interface{}{
            "service":   "my-api",
            "url":       "https://api.example.com",
            "type":      "rest",
            "subgraph":  "main",
            "path":      "/api",
            "configCid": "QmXxx...",
        })
        
        if err != nil {
            log.Printf("✗ Heartbeat failed: %v", err)
        } else {
            log.Println("✓ Heartbeat sent")
        }
    }
}

func monitorNodeStats(client *grapthway.GrapthwayClient) {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        stats := client.GetNodeStats()
        log.Printf("Node Status: %d/%d healthy", stats.HealthyNodes, stats.TotalNodes)
        
        for _, node := range stats.Nodes {
            status := "✅"
            if !node.IsHealthy {
                status = "❌"
            }
            
            latency := "DOWN"
            if node.Latency != nil {
                latency = fmt.Sprintf("%dms", *node.Latency)
            }
            
            log.Printf("  %s %s: %s", status, node.URL, latency)
        }
    }
}
```

---

## Error Handling

All methods return errors. Multi-node setup provides automatic retries:

```go
// Automatically retries on different nodes if one fails
result, err := client.Transfer("0xRecipient", 100.0)
if err != nil {
    // Only throws after all retries exhausted
    if strings.Contains(err.Error(), "insufficient balance") {
        log.Println("Not enough GCU")
    } else if strings.Contains(err.Error(), "all 3 request attempts failed") {
        log.Printf("All nodes are down: %v", err)
    } else {
        log.Printf("Transaction failed: %v", err)
    }
}
```

---

## Best Practices

### 1. Node Configuration
- ✅ **Use multiple nodes** in production (minimum 3 for redundancy)
- ✅ **Set GRAPTHWAY_NODE_URL_1, _2, _3** in environment variables
- ✅ **Mix geographic regions** for better availability
- ✅ **Monitor node health** with `GetNodeStats()` periodically

### 2. Account Security
- ✅ **Never commit private keys** - use environment variables
- ✅ **Use different accounts** for different purposes (trading, services, etc.)
- ✅ **Implement account rotation** for high-security applications

### 3. Error Handling
- ✅ **Check all errors** from client methods
- ✅ **Log node failures** for monitoring
- ✅ **Set up alerts** when all nodes are unhealthy
- ✅ **Use defer** for cleanup operations

### 4. Service Integration
- ✅ **Use middleware whitelist** to only accept requests from your nodes
- ✅ **Send heartbeats every 15 seconds** for registered services
- ✅ **Monitor middleware logs** for unauthorized access attempts

### 5. Performance
- ✅ **Initial health check** runs on client initialization
- ✅ **Automatic re-checks** every 30 seconds
- ✅ **Request retries** use exponential backoff (1s, 2s, 4s)
- ✅ **Node stats available** without network calls

### 6. Concurrency
- ✅ **Client is thread-safe** - safe to use from multiple goroutines
- ✅ **AccountManager is thread-safe** - uses sync.RWMutex
- ✅ **NodePoolManager is thread-safe** - uses sync.RWMutex
- ✅ **Use goroutines** for heartbeats and monitoring

### 7. Environment Setup

**Development:**
```bash
export GRAPTHWAY_NODE_URL_1=http://localhost:5000
export GRAPTHWAY_NODE_URL_2=http://localhost:5001
export SERVICE_PRIVATE_KEY=your_dev_key
```

**Production:**
```bash
export GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
export GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
export GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
export SERVICE_PRIVATE_KEY=your_prod_key
```

---

## Troubleshooting

### Problem: "no Grapthway node URLs found"
**Solution:** Set environment variables:
```bash
export GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
export GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
```

### Problem: "all 3 request attempts failed"
**Solution:** Check node health:
```go
stats, err := client.CheckNodeHealth()
if err != nil {
    log.Fatal(err)
}
for _, node := range stats.Nodes {
    log.Printf("Node %s: healthy=%v", node.URL, node.IsHealthy)
}
```

All nodes might be down or unreachable. Verify:
- Network connectivity
- Node URLs are correct
- Nodes are running
- Firewall rules allow connections

### Problem: Middleware blocking legitimate requests
**Solution:** Check whitelist:
```go
middleware := grapthway.GrapthwayMiddleware(&grapthway.MiddlewareConfig{
    NodeURLs: []string{"https://your-actual-node.com"},
})
```

Ensure your node URLs match the environment variables.

### Problem: High latency on all nodes
**Solution:** 
- Check your internet connection
- Verify DNS resolution
- Consider using geographically closer nodes
- Check node logs for performance issues

### Problem: "no account selected"
**Solution:** Add and select an account:
```go
accountManager := client.GetAccountManager()
account, err := accountManager.AddAccount("private_key", "My Account")
if err != nil {
    log.Fatal(err)
}
// Account is automatically selected as the first one
```

---

## Migration Guide

### From Single Node to Multi-Node

**Before:**
```go
client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
    NodeURL: "http://localhost:5000",
})
```

**After:**
```go
client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
    NodeURLs: []string{
        "http://localhost:5000",
        "http://localhost:5001",
        "http://localhost:5002",
    },
})
```

**Environment Variables:**

**Before:**
```bash
export GRAPTHWAY_NODE_URL=http://localhost:5000
```

**After:**
```bash
export GRAPTHWAY_NODE_URL_1=http://localhost:5000
export GRAPTHWAY_NODE_URL_2=http://localhost:5001
export GRAPTHWAY_NODE_URL_3=http://localhost:5002
```

### Backward Compatibility

All existing code continues to work:
- ✅ Single `NodeURL` parameter still supported
- ✅ `GRAPTHWAY_NODE_URL` environment variable still works
- ✅ No breaking changes to existing methods
- ✅ Same function signatures

---

## Testing

### Unit Testing

```go
package main

import (
    "testing"
    "your-module-path/grapthway"
)

func TestClientInitialization(t *testing.T) {
    client, err := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
        NodeURLs: []string{
            "http://localhost:5000",
            "http://localhost:5001",
        },
    })
    
    if err != nil {
        t.Fatalf("Failed to create client: %v", err)
    }
    
    stats := client.GetNodeStats()
    if stats.TotalNodes != 2 {
        t.Errorf("Expected 2 nodes, got %d", stats.TotalNodes)
    }
}

func TestAccountManagement(t *testing.T) {
    client, _ := grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
        NodeURL: "http://localhost:5000",
    })
    
    accountManager := client.GetAccountManager()
    
    account, err := accountManager.AddAccount(
        "your_test_private_key",
        "Test Account",
    )
    
    if err != nil {
        t.Fatalf("Failed to add account: %v", err)
    }
    
    if account.Label != "Test Account" {
        t.Errorf("Expected label 'Test Account', got '%s'", account.Label)
    }
}
```

### Integration Testing

```go
func TestTransfer(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    client, err := grapthway.NewGrapthwayClient(nil)
    if err != nil {
        t.Fatal(err)
    }
    
    accountManager := client.GetAccountManager()
    account, _ := accountManager.AddAccount(
        os.Getenv("TEST_PRIVATE_KEY"),
        "Test",
    )
    
    // Check balance first
    balance, err := client.GetBalance(account.Address)
    if err != nil {
        t.Fatal(err)
    }
    
    if balance < 1.0 {
        t.Skip("Insufficient balance for test")
    }
    
    // Perform transfer
    result, err := client.Transfer(
        "0xRecipientAddress",
        0.1,
    )
    
    if err != nil {
        t.Fatalf("Transfer failed: %v", err)
    }
    
    if result["transactionId"] == nil {
        t.Error("No transaction ID returned")
    }
}
```

---

## Performance Considerations

### Connection Pooling

The client uses a single `http.Client` with a 30-second timeout:

```go
httpClient := &http.Client{
    Timeout: 30 * time.Second,
}
```

For high-throughput applications, consider customizing:

```go
import (
    "net/http"
    "time"
)

// Custom transport with connection pooling
transport := &http.Transport{
    MaxIdleConns:        100,
    MaxIdleConnsPerHost: 100,
    IdleConnTimeout:     90 * time.Second,
}

// Note: Currently the client creates its own http.Client
// For custom transport, you may need to modify the client
```

### Concurrent Requests

The client is thread-safe and can handle concurrent requests:

```go
import "sync"

func parallelTransfers(client *grapthway.GrapthwayClient, recipients []string) {
    var wg sync.WaitGroup
    
    for _, recipient := range recipients {
        wg.Add(1)
        go func(to string) {
            defer wg.Done()
            
            result, err := client.Transfer(to, 1.0)
            if err != nil {
                log.Printf("Transfer to %s failed: %v", to, err)
                return
            }
            
            log.Printf("Transferred to %s: %v", to, result["transactionId"])
        }(recipient)
    }
    
    wg.Wait()
}
```

### Memory Management

The SDK uses minimal memory:
- Node pool keeps lightweight node metadata
- Account manager stores only essential account data
- No caching of API responses (stateless)

For long-running applications:
```go
// Periodically check for memory leaks
import "runtime"

func logMemoryStats() {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    log.Printf("Alloc = %v MB", m.Alloc / 1024 / 1024)
    log.Printf("TotalAlloc = %v MB", m.TotalAlloc / 1024 / 1024)
    log.Printf("Sys = %v MB", m.Sys / 1024 / 1024)
    log.Printf("NumGC = %v", m.NumGC)
}
```

---

## Security Best Practices

### Private Key Management

```go
import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    "encoding/hex"
    "io"
)

// Encrypt private key before storing
func encryptPrivateKey(privateKey, passphrase string) (string, error) {
    key := []byte(passphrase) // Use proper key derivation in production
    
    block, err := aes.NewCipher(key)
    if err != nil {
        return "", err
    }
    
    gcm, err := cipher.NewGCM(block)
    if err != nil {
        return "", err
    }
    
    nonce := make([]byte, gcm.NonceSize())
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        return "", err
    }
    
    ciphertext := gcm.Seal(nonce, nonce, []byte(privateKey), nil)
    return hex.EncodeToString(ciphertext), nil
}

// Always use environment variables or secure vaults
func getPrivateKey() string {
    // From environment
    if key := os.Getenv("GRAPTHWAY_PRIVATE_KEY"); key != "" {
        return key
    }
    
    // From secure vault (e.g., HashiCorp Vault, AWS Secrets Manager)
    // return getFromVault("grapthway-private-key")
    
    log.Fatal("Private key not found")
    return ""
}
```

### Request Signing

The SDK automatically signs all requests. Verify signatures in your middleware:

```go
// Middleware automatically verifies:
// 1. Public key matches address
// 2. Request origin is from whitelisted nodes
// 3. Signature is valid for request body
func protectedHandler(w http.ResponseWriter, r *http.Request) {
    // Request is already verified by middleware
    verified := r.Header.Get("X-Grapthway-Verified")
    if verified != "true" {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }
    
    // Process request safely
}
```

---

## Support

- **Documentation:** [https://docs.grapthway.com](https://docs.grapthway.com)
- **GitHub:** [https://github.com/grapthway/client-sdk-go](https://github.com/grapthway/client-sdk-go)
- **Discord:** [https://discord.gg/grapthway](https://discord.gg/grapthway)
- **Email:** support@grapthway.com

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup

```bash
# Clone repository
git clone https://github.com/grapthway/client-sdk-go.git
cd client-sdk-go

# Install dependencies
go mod download

# Run tests
go test ./...

# Run with race detector
go test -race ./...

# Build
go build ./...
```

---

## License

MIT License - see LICENSE file for details

---

## Changelog

### v1.0.0 (2024-01-15)
- ✅ Multi-node support with automatic failover
- ✅ Latency-based node routing
- ✅ Thread-safe account management
- ✅ Comprehensive wallet operations
- ✅ Full token lifecycle support
- ✅ Staking operations
- ✅ HTTP middleware for service verification
- ✅ Automatic request retries with exponential backoff
- ✅ Node health monitoring

### v0.9.0 (2023-12-01)
- Initial beta release
- Single node support
- Basic wallet operations

---

## FAQ

**Q: Can I use this SDK in a web application?**
A: This is a Go SDK designed for backend services. For web applications, use the JavaScript SDK.

**Q: How many nodes should I configure?**
A: We recommend at least 3 nodes in production for redundancy. Use 5+ for mission-critical applications.

**Q: What happens if all nodes go down?**
A: The client will attempt to use the least-failed node and return an error after all retries are exhausted.

**Q: Can I add nodes after initialization?**
A: Currently, nodes are configured at initialization. To change nodes, create a new client instance.

**Q: Is the SDK compatible with Go modules?**
A: Yes, the SDK fully supports Go modules. Use `go get` to install.

**Q: How do I handle rate limiting?**
A: The SDK includes automatic retry with exponential backoff. For custom rate limiting, implement your own logic around client calls.

**Q: Can I use this with Docker?**
A: Yes! Pass environment variables to your Docker container:
```dockerfile
ENV GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
ENV GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
ENV GRAPTHWAY_PRIVATE_KEY=your_private_key
```

---

**Built with ❤️ by the Grapthway Team**