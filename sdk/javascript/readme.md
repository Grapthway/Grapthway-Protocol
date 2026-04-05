# Grapthway Client SDK Documentation

Complete JavaScript/Node.js SDK for interacting with the Grapthway Protocol Network with **multi-node support, automatic failover, and latency-based routing**.

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

---

## Installation

```bash
npm install grapthway-client elliptic zustand
```

**Required Dependencies:**
```json
{
  "elliptic": "^6.5.4",
  "zustand": "^4.4.0",
  "node-fetch": "^2.6.7"
}
```

---

## Quick Start

### Basic Setup (Single Node)

```javascript
import GrapthwayClient from 'grapthway-client';

// Single node (backward compatible)
const client = new GrapthwayClient({
  nodeUrl: 'http://localhost:5000',
  privateKey: 'your_private_key_hex'
});
```

### Multi-Node Setup (Recommended)

```javascript
// Multiple nodes with automatic failover
const client = new GrapthwayClient({
  nodeUrls: [
    'https://node1.grapthway.com',
    'https://node2.grapthway.com',
    'https://node3.grapthway.com'
  ],
  privateKey: 'your_private_key_hex'
});
```

### Environment Variables (Recommended)

**Using single node:**
```bash
GRAPTHWAY_NODE_URL=https://node1.grapthway.com
```

**Using multiple nodes:**
```bash
GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
```

Then initialize without config:
```javascript
const client = new GrapthwayClient();
// Automatically loads all GRAPTHWAY_NODE_URL_* from environment
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

```javascript
// Check node health status
const stats = client.getNodeStats();
console.log('Total nodes:', stats.totalNodes);
console.log('Healthy nodes:', stats.healthyNodes);

stats.nodes.forEach(node => {
  console.log(`${node.url}: ${node.latency}ms (${node.isHealthy ? 'UP' : 'DOWN'})`);
});

// Force health check (useful for debugging)
await client.checkNodeHealth();
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

```javascript
// All requests automatically retry up to 3 times
await client.transfer('0x...', 10);
// If node1 fails, automatically tries node2, then node3

// Custom retry count (advanced)
await client._request('/endpoint', { 
  maxRetries: 5 // Override default of 3
});
```

---

## Account Management

The SDK includes a powerful Zustand-based account management system with persistence.

### Adding Accounts

```javascript
import { useGrapthwayAccounts } from 'grapthway-client';

// Get account store
const accountStore = useGrapthwayAccounts.getState();

// Add a new account
const newAccount = accountStore.addAccount(
  'your_private_key_hex',
  'Development Account' // Optional label
);

console.log('Created account:', newAccount.address);
```

### Managing Multiple Accounts

```javascript
// List all accounts
const accounts = accountStore.accounts;
console.log('Total accounts:', accounts.length);

// Select an account
accountStore.selectAccount(accountId);

// Get current account
const currentAccount = accountStore.getCurrentAccount();

// Update account label
accountStore.updateAccountLabel(accountId, 'Production Account');

// Remove an account
accountStore.removeAccount(accountId);

// Clear all accounts
accountStore.clearAccounts();
```

### Using Account Store in React

```javascript
import { useGrapthwayAccounts } from 'grapthway-client';

function AccountSelector() {
  const { accounts, selectedAccount, selectAccount } = useGrapthwayAccounts();
  
  return (
    <select 
      value={selectedAccount?.id} 
      onChange={(e) => selectAccount(e.target.value)}
    >
      {accounts.map(account => (
        <option key={account.id} value={account.id}>
          {account.label} - {account.address.substring(0, 10)}...
        </option>
      ))}
    </select>
  );
}
```

---

## Configuration & Services

### Publishing Service Configuration

```javascript
const config = {
  service: 'my-graphql-api',
  url: 'https://api.myservice.com',
  type: 'graphql',
  subgraph: 'main',
  schema: `
    type Query {
      hello: String
    }
  `,
  // Optional: Middleware pipelines
  pipelines: {
    'Query.hello': {
      pre: [
        {
          service: 'auth-service',
          field: 'validateToken',
          argsMapping: {
            token: '$headers.authorization'
          }
        }
      ]
    }
  }
};

await client.publishConfig(config);
```

### Service Heartbeat

Keep your service registered and healthy:

```javascript
// Send heartbeat every 15 seconds
setInterval(async () => {
  try {
    await client.heartbeat({
      service: 'my-graphql-api',
      url: 'https://api.myservice.com',
      type: 'graphql',
      subgraph: 'main',
      configCid: 'QmXxxx...' // From publish response
    });
    console.log('Heartbeat sent');
  } catch (error) {
    console.error('Heartbeat failed:', error.message);
  }
}, 15000);
```

### Fetching Network Configuration

```javascript
// Get all registered services
const services = await client.getServices();
console.log('Services:', services);

// Get all pipelines
const pipelines = await client.getPipelines();
console.log('Pipelines:', pipelines);

// Get schema information
const schema = await client.getSchema();
console.log('Schema:', schema);
```

---

## Wallet Operations

### Balance & Nonce

```javascript
const address = client._getCurrentIdentity().address;

// Get balance
const balance = await client.getBalance(address);
console.log('Balance:', balance, 'GCU');

// Get nonce (for transaction ordering)
const nonce = await client.getNonce(address);
console.log('Next nonce:', nonce + 1);
```

### Transferring GCU

```javascript
// Simple transfer
const result = await client.transfer(
  '0xRecipientAddress',
  25.5 // Amount in GCU
);

console.log('Transaction ID:', result.transactionId);
console.log('Assigned Nonce:', result.assignedNonce);
```

### Transaction History

```javascript
// Get recent transactions
const history = await client.getTransactionHistory(address, {
  page: 1,
  limit: 20
});

console.log('Total transactions:', history.total);
console.log('Transactions:', history.transactions);

// With time filtering
const recentHistory = await client.getTransactionHistory(address, {
  page: 1,
  limit: 10,
  start: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(), // Last 24h
  end: new Date().toISOString()
});
```

### Allowances (Delegated Spending)

```javascript
// Set allowance for a spender
await client.setAllowance(
  '0xSpenderAddress',
  100 // Allow spending up to 100 GCU
);

// Check allowance
const allowance = await client.getAllowance(
  address,
  '0xSpenderAddress'
);
console.log('Allowance:', allowance.allowance, 'GCU');

// Remove allowance
await client.removeAllowance('0xSpenderAddress');
```

---

## Token Operations

### Creating a Token

```javascript
const tokenMetadata = {
  name: 'My Token',
  ticker: 'MTK',
  decimals: 6,
  initialSupply: 1000000, // 1 million tokens
  tokenType: 'FUNGIBLE', // or 'RWA', 'NFT'
  burnConfig: {
    enabled: true,
    manualBurn: true,
    burnRatePerTx: 0 // No automatic burn
  },
  mintConfig: {
    enabled: true,
    manualMint: true,
    mintRatePerTx: 0 // No automatic mint
  },
  metadata: JSON.stringify({
    description: 'My custom token',
    website: 'https://mytoken.com'
  })
};

const result = await client.createToken(tokenMetadata);
console.log('Token created:', result.tokenMetadata);
```

### Transferring Tokens

```javascript
await client.transferToken(
  '0xTokenAddress',
  '0xRecipientAddress',
  100.5 // Amount in token units
);
```

### Token Information

```javascript
// Get token details
const tokenInfo = await client.getTokenInfo('0xTokenAddress');
console.log('Token:', tokenInfo.name, tokenInfo.ticker);
console.log('Total Supply:', tokenInfo.totalSupply);

// Get token balance
const balance = await client.getTokenBalance(
  '0xTokenAddress',
  address
);
console.log('Balance:', balance.balance);

// Get all owned tokens
const ownedTokens = await client.getOwnedTokens(address);
console.log('You own', ownedTokens.count, 'different tokens');
```

### Token Allowances

```javascript
// Approve spender for tokens
await client.setTokenAllowance(
  '0xTokenAddress',
  '0xSpenderAddress',
  50 // Allow spending 50 tokens
);

// Check token allowance
const allowance = await client.getTokenAllowance(
  '0xTokenAddress',
  address,
  '0xSpenderAddress'
);

// Revoke allowance
await client.deleteTokenAllowance(
  '0xTokenAddress',
  '0xSpenderAddress'
);
```

### Minting & Burning (Creator Only)

```javascript
// Mint new tokens
await client.mintToken(
  '0xTokenAddress',
  '0xRecipientAddress',
  1000 // Mint 1000 tokens
);

// Burn your tokens
await client.burnToken(
  '0xTokenAddress',
  500 // Burn 500 tokens
);
```

### Token Configuration

```javascript
// Update mint configuration
await client.setTokenMintConfig('0xTokenAddress', {
  enabled: true,
  rate: 1.5, // 1.5% mint per transaction
  manualMint: true
});

// Update burn configuration
await client.setTokenBurnConfig('0xTokenAddress', {
  enabled: true,
  rate: 0.5, // 0.5% burn per transaction
  manualBurn: true
});

// Lock configuration (permanent!)
await client.lockTokenConfig('0xTokenAddress');
```

### Token History

```javascript
// Get all token transactions for an address
const history = await client.getTokenHistory(address, {
  page: 1,
  limit: 20
});

// Filter by specific token
const tokenHistory = await client.getTokenHistory(address, {
  tokenAddress: '0xTokenAddress',
  page: 1,
  limit: 10
});
```

---

## Staking

### Checking Staking Status

```javascript
const stakingInfo = await client.getStakingStatus(address);

console.log('Total Staked:', stakingInfo.totalStake, 'GCU');
console.log('Allocations:', stakingInfo.allocations);
console.log('Unbonding:', stakingInfo.unbonding);
```

### Depositing & Withdrawing Stake

```javascript
// Deposit to staking pool
await client.stakeDeposit(100); // Deposit 100 GCU

// Withdraw from staking pool (starts unbonding period)
await client.stakeWithdraw(50); // Withdraw 50 GCU
```

### Assigning Stake to Nodes

```javascript
// Assign stake to support a node
await client.stakeAssign(
  '12D3KooWXxxx...', // Node Peer ID
  25 // Assign 25 GCU
);

// Unassign stake (node must be offline)
await client.stakeUnassign('12D3KooWXxxx...');
```

---

## Network Information

### Gateway Status

```javascript
const status = await client.getGatewayStatus();

console.log('Uptime:', status.uptime);
console.log('Node ID:', status.node_id);
console.log('Total Services:', status.total_services);
console.log('Connected Peers:', status.peers.length);
```

### Hardware Stats

```javascript
const stats = await client.getHardwareStats();

// Local node stats
console.log('CPU Usage:', stats.local.usage.cpu_total_usage_percent, '%');
console.log('RAM Usage:', stats.local.usage.ram_usage_percent, '%');

// Global network stats
console.log('Total Nodes:', stats.global.total_nodes);
console.log('Network CPU:', stats.global.average_cpu_usage_percent, '%');
```

---

## Advanced Features

### Workflow Monitoring

```javascript
// Get workflow instances
const workflows = await client.getWorkflowMonitoring({
  workflowName: 'data-processing',
  status: 'RUNNING' // RUNNING, COMPLETED, FAILED
});

workflows.forEach(workflow => {
  console.log('Workflow:', workflow.workflowName);
  console.log('Status:', workflow.status);
  console.log('Step:', workflow.currentStep);
});
```

### Logging

```javascript
// Get gateway logs (your requests)
const gatewayLogs = await client.getLogs('gateway', {
  start: new Date(Date.now() - 3600000).toISOString(), // Last hour
  max: 100
});

// Get ledger logs (your transactions)
const ledgerLogs = await client.getLogs('ledger', {
  max: 50
});
```

### Admin Operations (Node Operators Only)

```javascript
// Create a new wallet
const wallet = await client.createWallet();
console.log('New wallet:', wallet.address);
console.log('Private key:', wallet.privateKey); // SAVE THIS!

// Get all logs
const allLogs = await client.getAdminLogs('gateway', { max: 1000 });

// Get all workflows
const allWorkflows = await client.getAdminWorkflowMonitoring();
```

---

## Middleware

### Express Middleware for Service Verification

Verify that requests to your service come from **authorized Grapthway nodes only**.

#### Basic Setup (Auto-detect from environment)

```javascript
import express from 'express';
import { createGrapthwayMiddleware } from 'grapthway-client';

const app = express();

// CRITICAL: Add raw body parser for signature verification
app.use(express.json({
  verify: (req, res, buf) => {
    req.rawBody = buf.toString('utf8');
  }
}));

// Create middleware (automatically reads GRAPTHWAY_NODE_URL_* from env)
const grapthwayAuth = createGrapthwayMiddleware();

// Apply to protected routes
app.post('/api/protected', grapthwayAuth, (req, res) => {
  // Only verified Grapthway nodes can access this
  console.log('Request from node:', req.grapthwayNode.address);
  res.json({ message: 'Authenticated!' });
});

app.listen(3000);
```

#### Explicit Node Whitelist

```javascript
// Manually specify allowed nodes
const grapthwayAuth = createGrapthwayMiddleware({
  nodeUrls: [
    'https://node1.grapthway.com',
    'https://node2.grapthway.com',
    'https://node3.grapthway.com'
  ]
});

app.post('/api/protected', grapthwayAuth, (req, res) => {
  // Only requests from whitelisted nodes are allowed
  res.json({ success: true });
});
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

```javascript
app.post('/api/protected', grapthwayAuth, (req, res) => {
  // Node info attached to request
  console.log('Node Address:', req.grapthwayNode.address);
  console.log('Node Public Key:', req.grapthwayNode.publicKey);
  console.log('Verified:', req.grapthwayNode.verified);
  
  res.json({ 
    message: 'Request processed',
    nodeAddress: req.grapthwayNode.address 
  });
});
```

#### Environment Setup for Middleware

**Development (.env):**
```bash
GRAPTHWAY_NODE_URL_1=http://localhost:5000
GRAPTHWAY_NODE_URL_2=http://localhost:5001
```

**Production (.env.production):**
```bash
GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
```

#### Legacy Middleware (Backward Compatible)

```javascript
import { grapthwayVerificationMiddleware } from 'grapthway-client';

// Uses default environment variables
app.post('/api/protected', grapthwayVerificationMiddleware, handler);
```

---

## API Reference

### Client Initialization

```typescript
new GrapthwayClient(config?: {
  // Single node (backward compatible)
  nodeUrl?: string;
  
  // Multiple nodes (recommended)
  nodeUrls?: string[];
  
  // Account configuration
  privateKey?: string;
  label?: string;
})
```

**Examples:**
```javascript
// From environment (auto-loads GRAPTHWAY_NODE_URL_*)
const client = new GrapthwayClient();

// Single node
const client = new GrapthwayClient({
  nodeUrl: 'http://localhost:5000'
});

// Multiple nodes with failover
const client = new GrapthwayClient({
  nodeUrls: [
    'https://node1.grapthway.com',
    'https://node2.grapthway.com'
  ]
});
```

### Node Pool Methods

```typescript
// Get node statistics
getNodeStats(): {
  nodes: Array<{
    url: string;
    latency: number | null;
    isHealthy: boolean;
    failures: number;
    lastCheck: number;
  }>;
  totalNodes: number;
  healthyNodes: number;
}

// Force health check
checkNodeHealth(): Promise<NodeStats>;
```

### Middleware Factory

```typescript
createGrapthwayMiddleware(options?: {
  nodeUrls?: string[]; // Allowed node URLs (auto-loads from env if not provided)
}): ExpressMiddleware;
```

### Account Management (Zustand Store)

```typescript
useGrapthwayAccounts: {
  // State
  accounts: Account[];
  selectedAccount: Account | null;
  
  // Actions
  addAccount(privateKey: string, label?: string): Account;
  removeAccount(accountId: string): void;
  selectAccount(accountId: string): void;
  updateAccountLabel(accountId: string, label: string): void;
  getCurrentAccount(): Account | null;
  clearAccounts(): void;
}
```

### Configuration Methods

```typescript
publishConfig(config: ServiceConfig): Promise<{ success: boolean; service: string }>;
heartbeat(payload: HeartbeatPayload): Promise<{ success: boolean; service: string }>;
```

### Wallet Methods

```typescript
getBalance(address: string): Promise<number>;
getNonce(address: string): Promise<number>;
transfer(to: string, amount: number): Promise<TransactionResult>;
getTransactionHistory(address: string, options?: HistoryOptions): Promise<TransactionHistory>;
setAllowance(spender: string, amount: number): Promise<TransactionResult>;
removeAllowance(spender: string): Promise<TransactionResult>;
getAllowance(owner: string, spender: string): Promise<{ allowance: number }>;
```

### Token Methods

```typescript
createToken(tokenMetadata: TokenMetadata): Promise<TransactionResult>;
transferToken(tokenAddress: string, to: string, amount: number): Promise<TransactionResult>;
getTokenInfo(tokenAddress: string): Promise<TokenInfo>;
getTokenBalance(tokenAddress: string, owner: string): Promise<{ balance: number }>;
getOwnedTokens(owner: string): Promise<{ tokens: OwnedToken[]; count: number }>;
setTokenAllowance(tokenAddress: string, spender: string, amount: number): Promise<TransactionResult>;
deleteTokenAllowance(tokenAddress: string, spender: string): Promise<TransactionResult>;
getTokenAllowance(tokenAddress: string, owner: string, spender: string): Promise<{ allowance: number }>;
mintToken(tokenAddress: string, to: string, amount: number): Promise<TransactionResult>;
burnToken(tokenAddress: string, amount: number): Promise<TransactionResult>;
setTokenMintConfig(tokenAddress: string, config: MintConfig): Promise<TransactionResult>;
setTokenBurnConfig(tokenAddress: string, config: BurnConfig): Promise<TransactionResult>;
lockTokenConfig(tokenAddress: string): Promise<TransactionResult>;
getTokenHistory(address: string, options?: TokenHistoryOptions): Promise<TokenHistoryResult>;
```

### Staking Methods

```typescript
getStakingStatus(ownerAddress: string): Promise<StakingInfo>;
stakeDeposit(amount: number): Promise<TransactionResult>;
stakeWithdraw(amount: number): Promise<TransactionResult>;
stakeAssign(nodePeerId: string, amount: number): Promise<TransactionResult>;
stakeUnassign(nodePeerId: string): Promise<TransactionResult>;
```

### Network Methods

```typescript
getGatewayStatus(): Promise<GatewayStatus>;
getHardwareStats(): Promise<HardwareStats>;
getServices(): Promise<ServicesMap>;
getPipelines(): Promise<PipelinesMap>;
getSchema(): Promise<SchemaMap>;
```

### Workflow & Logging

```typescript
getWorkflowMonitoring(filters?: WorkflowFilters): Promise<WorkflowInstance[]>;
getLogs(logType: 'gateway' | 'ledger', options?: LogOptions): Promise<LogEntry[]>;
```

### Admin Methods

```typescript
createWallet(): Promise<WalletInfo>;
getAdminLogs(logType: LogType, options?: LogOptions): Promise<LogEntry[]>;
getAdminWorkflowMonitoring(filters?: WorkflowFilters): Promise<WorkflowInstance[]>;
```

---

## Complete Example

Here's a production-ready example with multi-node setup:

```javascript
import GrapthwayClient, { 
  useGrapthwayAccounts, 
  createGrapthwayMiddleware 
} from 'grapthway-client';
import express from 'express';

// ========================================
// 1. SERVICE SETUP
// ========================================

const app = express();

// Raw body parser (required for signature verification)
app.use(express.json({
  verify: (req, res, buf) => {
    req.rawBody = buf.toString('utf8');
  }
}));

// Create Grapthway middleware with whitelist
const grapthwayAuth = createGrapthwayMiddleware({
  nodeUrls: [
    'https://node1.grapthway.com',
    'https://node2.grapthway.com',
    'https://node3.grapthway.com'
  ]
});

// Protected endpoint (only accessible by whitelisted Grapthway nodes)
app.post('/api/process', grapthwayAuth, (req, res) => {
  console.log('Processing request from node:', req.grapthwayNode.address);
  res.json({ result: 'processed', data: req.body });
});

app.listen(3000, () => {
  console.log('Service listening on port 3000');
});

// ========================================
// 2. CLIENT OPERATIONS
// ========================================

async function main() {
  // Initialize client with multiple nodes
  const client = new GrapthwayClient({
    nodeUrls: [
      'https://node1.grapthway.com',
      'https://node2.grapthway.com',
      'https://node3.grapthway.com'
    ]
  });

  // Add accounts
  const accountStore = useGrapthwayAccounts.getState();
  accountStore.addAccount(process.env.PRIVATE_KEY_1, 'Trading Account');
  accountStore.addAccount(process.env.PRIVATE_KEY_2, 'Service Account');

  // Select trading account
  const tradingAccount = accountStore.accounts[0];
  accountStore.selectAccount(tradingAccount.id);

  // Check node health
  console.log('Checking node health...');
  const nodeStats = await client.checkNodeHealth();
  console.log(`Using ${nodeStats.healthyNodes}/${nodeStats.totalNodes} healthy nodes`);

  // Check balance
  const balance = await client.getBalance(tradingAccount.address);
  console.log('Balance:', balance, 'GCU');

  // Publish service configuration
  await client.publishConfig({
    service: 'my-api',
    url: 'https://api.example.com',
    type: 'rest',
    subgraph: 'main',
    path: '/api',
    configCid: 'QmXxx...'
  });

  // Start heartbeat (keeps service registered)
  setInterval(async () => {
    try {
      await client.heartbeat({
        service: 'my-api',
        url: 'https://api.example.com',
        type: 'rest',
        subgraph: 'main',
        path: '/api',
        configCid: 'QmXxx...'
      });
      console.log('✅ Heartbeat sent');
    } catch (error) {
      console.error('❌ Heartbeat failed:', error.message);
    }
  }, 15000);

  // Transfer GCU with automatic failover
  if (balance > 10) {
    const serviceAccount = accountStore.accounts[1];
    const tx = await client.transfer(serviceAccount.address, 5);
    console.log('Transferred 5 GCU:', tx.transactionId);
  }

  // Create a token
  const token = await client.createToken({
    name: 'MyToken',
    ticker: 'MTK',
    decimals: 6,
    initialSupply: 1000000,
    tokenType: 'FUNGIBLE',
    burnConfig: { enabled: true, manualBurn: true },
    mintConfig: { enabled: true, manualMint: true },
    metadata: JSON.stringify({ description: 'Production token' })
  });

  console.log('Token created:', token.tokenMetadata?.ticker);

  // Stake GCU
  await client.stakeDeposit(50);
  const staking = await client.getStakingStatus(tradingAccount.address);
  console.log('Total staked:', staking.totalStake, 'GCU');

  // Monitor node stats periodically
  setInterval(async () => {
    const stats = client.getNodeStats();
    console.log(`Node Status: ${stats.healthyNodes}/${stats.totalNodes} healthy`);
    stats.nodes.forEach(node => {
      const status = node.isHealthy ? '✅' : '❌';
      const latency = node.latency ? `${node.latency}ms` : 'DOWN';
      console.log(`  ${status} ${node.url}: ${latency}`);
    });
  }, 60000); // Every minute
}

main().catch(console.error);
```

---

## Error Handling

All methods can throw errors. Multi-node setup provides automatic retries:

```javascript
try {
  // Automatically retries on different nodes if one fails
  await client.transfer('0xRecipient', 100);
} catch (error) {
  // Only throws after all retries exhausted
  if (error.message.includes('insufficient balance')) {
    console.error('Not enough GCU');
  } else if (error.message.includes('All 3 request attempts failed')) {
    console.error('All nodes are down:', error.message);
  } else {
    console.error('Transaction failed:', error.message);
  }
}
```

---

## Best Practices

### 1. Node Configuration
- ✅ **Use multiple nodes** in production (minimum 3 for redundancy)
- ✅ **Set GRAPTHWAY_NODE_URL_1, _2, _3** in environment variables
- ✅ **Mix geographic regions** for better availability
- ✅ **Monitor node health** with `getNodeStats()` periodically

### 2. Account Security
- ✅ **Never commit private keys** - use environment variables
- ✅ **Use different accounts** for different purposes (trading, services, etc.)
- ✅ **Implement account rotation** for high-security applications

### 3. Error Handling
- ✅ **Wrap all client calls** in try-catch blocks
- ✅ **Log node failures** for monitoring
- ✅ **Set up alerts** when all nodes are unhealthy

### 4. Service Integration
- ✅ **Use middleware whitelist** to only accept requests from your nodes
- ✅ **Send heartbeats every 15 seconds** for registered services
- ✅ **Monitor middleware logs** for unauthorized access attempts

### 5. Performance
- ✅ **Initial health check** runs on client initialization
- ✅ **Automatic re-checks** every 30 seconds
- ✅ **Request retries** use exponential backoff (1s, 2s, 4s)
- ✅ **Node stats available** without network calls

### 6. Environment Setup

**Development:**
```bash
GRAPTHWAY_NODE_URL_1=http://localhost:5000
GRAPTHWAY_NODE_URL_2=http://localhost:5001
SERVICE_PRIVATE_KEY=your_dev_key
```

**Production:**
```bash
GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
GRAPTHWAY_NODE_URL_3=https://node3.grapthway.com
SERVICE_PRIVATE_KEY=your_prod_key
```

---

## Troubleshooting

### Problem: "No Grapthway node URLs found"
**Solution:** Set environment variables:
```bash
export GRAPTHWAY_NODE_URL_1=https://node1.grapthway.com
export GRAPTHWAY_NODE_URL_2=https://node2.grapthway.com
```

### Problem: "All 3 request attempts failed"
**Solution:** Check node health:
```javascript
const stats = await client.checkNodeHealth();
console.log(stats);
```

All nodes might be down or unreachable. Verify:
- Network connectivity
- Node URLs are correct
- Nodes are running

### Problem: Middleware blocking legitimate requests
**Solution:** Check whitelist:
```javascript
const middleware = createGrapthwayMiddleware({
  nodeUrls: ['https://your-actual-node.com']
});
```

Ensure your node URLs match the environment variables.

### Problem: High latency on all nodes
**Solution:** 
- Check your internet connection
- Verify DNS resolution
- Consider using geographically closer nodes
- Check node logs for performance issues

---

## Migration Guide

### From Single Node to Multi-Node

**Before:**
```javascript
const client = new GrapthwayClient({
  nodeUrl: 'http://localhost:5000'
});
```

**After:**
```javascript
const client = new GrapthwayClient({
  nodeUrls: [
    'http://localhost:5000',
    'http://localhost:5001',
    'http://localhost:5002'
  ]
});
```

**Environment Variables:**

**Before:**
```bash
GRAPTHWAY_NODE_URL=http://localhost:5000
```

**After:**
```bash
GRAPTHWAY_NODE_URL_1=http://localhost:5000
GRAPTHWAY_NODE_URL_2=http://localhost:5001
GRAPTHWAY_NODE_URL_3=http://localhost:5002
```

### Backward Compatibility

All existing code continues to work:
- ✅ Single `nodeUrl` parameter still supported
- ✅ `GRAPTHWAY_NODE_URL` environment variable still works
- ✅ Legacy middleware still available
- ✅ No breaking changes to existing methods

---

## Support

- Documentation: [https://docs.grapthway.com](https://docs.grapthway.com)
- GitHub: [https://github.com/grapthway/client-sdk](https://github.com/grapthway/client-sdk)
- Discord: [https://discord.gg/grapthway](https://discord.gg/grapthway)

---

## License

MIT License - see LICENSE file for details