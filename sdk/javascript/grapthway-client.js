import elliptic from 'elliptic';
import { createHash } from 'crypto';
import fetch from 'cross-fetch';
import { create } from 'zustand';
import { persist } from 'zustand/middleware';

const EC = elliptic.ec;
const ec = new EC('secp256k1');

// ========================================
// NODE POOL MANAGER
// ========================================

class NodePoolManager {
    constructor(nodeUrls = []) {
        this.nodes = nodeUrls.map(url => ({
            url: url.replace(/\/$/, ''),
            latency: Infinity,
            failures: 0,
            lastCheck: 0,
            isHealthy: true
        }));
        
        this.maxFailures = 3;
        this.healthCheckInterval = 30000; // 30 seconds
        this.pingTimeout = 5000; // 5 seconds
        this.lastHealthCheck = 0;
    }

    // Get all node URLs from environment variables
    static fromEnv() {
        const urls = [];
        let index = 1;
        
        // Try GRAPTHWAY_NODE_URL first (backward compatibility)
        if (process.env.GRAPTHWAY_NODE_URL) {
            urls.push(process.env.GRAPTHWAY_NODE_URL);
        }
        
        // Then try GRAPTHWAY_NODE_URL_1, GRAPTHWAY_NODE_URL_2, etc.
        while (process.env[`GRAPTHWAY_NODE_URL_${index}`]) {
            urls.push(process.env[`GRAPTHWAY_NODE_URL_${index}`]);
            index++;
        }
        
        if (urls.length === 0) {
            throw new Error('No Grapthway node URLs found. Set GRAPTHWAY_NODE_URL or GRAPTHWAY_NODE_URL_1, GRAPTHWAY_NODE_URL_2, etc.');
        }
        
        console.log(`✅ Loaded ${urls.length} Grapthway node(s) from environment`);
        return new NodePoolManager(urls);
    }

    // Ping a single node
    async pingNode(node) {
        const startTime = Date.now();
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.pingTimeout);
        
        try {
            const response = await fetch(`${node.url}/public/gateway-status`, {
                method: 'GET',
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            
            const latency = Date.now() - startTime;
            node.latency = latency;
            node.failures = 0;
            node.isHealthy = true;
            node.lastCheck = Date.now();
            
            return latency;
        } catch (error) {
            clearTimeout(timeoutId);
            node.failures++;
            node.latency = Infinity;
            
            if (node.failures >= this.maxFailures) {
                node.isHealthy = false;
                console.warn(`⚠️ Node ${node.url} marked as unhealthy after ${node.failures} failures`);
            }
            
            return Infinity;
        }
    }

    // Ping all nodes and sort by latency
    async updateLatencies() {
        console.log('🔍 Checking latency for all nodes...');
        
        const pingPromises = this.nodes.map(node => this.pingNode(node));
        await Promise.all(pingPromises);
        
        // Sort nodes by latency (healthy nodes first)
        this.nodes.sort((a, b) => {
            if (a.isHealthy && !b.isHealthy) return -1;
            if (!a.isHealthy && b.isHealthy) return 1;
            return a.latency - b.latency;
        });
        
        this.lastHealthCheck = Date.now();
        
        // Log results
        this.nodes.forEach((node, index) => {
            const status = node.isHealthy ? '✅' : '❌';
            const latency = node.latency === Infinity ? 'TIMEOUT' : `${node.latency}ms`;
            console.log(`  ${status} #${index + 1}: ${node.url} - ${latency}`);
        });
    }

    // Get the best available node
    async getBestNode() {
        // Perform health check if needed
        const timeSinceLastCheck = Date.now() - this.lastHealthCheck;
        if (timeSinceLastCheck > this.healthCheckInterval) {
            await this.updateLatencies();
        }
        
        // Return first healthy node (already sorted by latency)
        const healthyNode = this.nodes.find(n => n.isHealthy);
        
        if (!healthyNode) {
            // All nodes are down, try the least-failed one
            console.warn('⚠️ All nodes unhealthy, attempting least-failed node');
            return this.nodes[0];
        }
        
        return healthyNode;
    }

    // Mark a node as failed (for request-level failure handling)
    markNodeFailed(nodeUrl) {
        const node = this.nodes.find(n => n.url === nodeUrl);
        if (node) {
            node.failures++;
            if (node.failures >= this.maxFailures) {
                node.isHealthy = false;
                console.warn(`⚠️ Node ${node.url} marked unhealthy after request failure`);
            }
        }
    }

    // Get all node URLs (for middleware whitelist)
    getAllNodeUrls() {
        return this.nodes.map(n => n.url);
    }

    // Get all node addresses (extract domain for verification)
    getAllNodeDomains() {
        return this.nodes.map(n => {
            try {
                const url = new URL(n.url);
                return url.hostname;
            } catch {
                return n.url;
            }
        });
    }
}

// ========================================
// UTILITY FUNCTIONS
// ========================================

const normalizeConfig = (data) => {
    if (data === null || typeof data !== 'object') {
        return data;
    }

    if (Array.isArray(data)) {
        return data.map(normalizeConfig);
    }

    const normalized = {};
    for (const [key, value] of Object.entries(data)) {
        if (value === null || value === undefined) continue;
        if (typeof value === 'string' && value === '') continue;
        if (Array.isArray(value) && value.length === 0) continue;
        if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0) continue;
        normalized[key] = normalizeConfig(value);
    }

    return normalized;
};

const marshalCanonical = (data) => {
    const normalized = normalizeConfig(data);
    
    if (normalized === null || typeof normalized !== 'object') {
        return JSON.stringify(normalized);
    }

    if (Array.isArray(normalized)) {
        return `[${normalized.map(marshalCanonical).join(',')}]`;
    }

    const keys = Object.keys(normalized).sort();
    const pairs = keys.map(key => {
        const value = normalized[key];
        const valStr = marshalCanonical(value);
        return `"${key}":${valStr}`;
    });

    return `{${pairs.join(',')}}`;
};

// ========================================
// ACCOUNT MANAGEMENT STORE (ZUSTAND)
// ========================================

export const useGrapthwayAccounts = create(
    persist(
        (set, get) => ({
            accounts: [],
            selectedAccount: null,

            // Add a new account
            addAccount: (privateKey, label = '') => {
                const keyPair = ec.keyFromPrivate(privateKey, 'hex');
                const publicKeyHex = keyPair.getPublic('hex');
                const pubKeyBytes = Buffer.from(publicKeyHex.substring(2), 'hex');
                const pubKeyHash = createHash('sha256').update(pubKeyBytes).digest();
                const address = `0x${pubKeyHash.toString('hex').slice(-40)}`;

                const newAccount = {
                    id: Date.now().toString(),
                    privateKey,
                    address,
                    publicKeyHex,
                    label: label || `Account ${address.substring(0, 8)}...`,
                    createdAt: new Date().toISOString()
                };

                set(state => ({
                    accounts: [...state.accounts, newAccount],
                    selectedAccount: state.selectedAccount || newAccount
                }));

                return newAccount;
            },

            // Remove an account
            removeAccount: (accountId) => {
                set(state => {
                    const newAccounts = state.accounts.filter(a => a.id !== accountId);
                    const newSelected = state.selectedAccount?.id === accountId 
                        ? (newAccounts[0] || null)
                        : state.selectedAccount;
                    
                    return {
                        accounts: newAccounts,
                        selectedAccount: newSelected
                    };
                });
            },

            // Select an account
            selectAccount: (accountId) => {
                set(state => ({
                    selectedAccount: state.accounts.find(a => a.id === accountId) || state.selectedAccount
                }));
            },

            // Update account label
            updateAccountLabel: (accountId, label) => {
                set(state => ({
                    accounts: state.accounts.map(a => 
                        a.id === accountId ? { ...a, label } : a
                    )
                }));
            },

            // Get current account
            getCurrentAccount: () => get().selectedAccount,

            // Clear all accounts
            clearAccounts: () => set({ accounts: [], selectedAccount: null })
        }),
        {
            name: 'grapthway-accounts-storage'
        }
    )
);

// ========================================
// GRAPTHWAY CLIENT CLASS
// ========================================

export class GrapthwayClient {
    constructor(config = {}) {
        // Initialize node pool
        if (config.nodeUrls && Array.isArray(config.nodeUrls)) {
            // Explicit node URLs provided
            this.nodePool = new NodePoolManager(config.nodeUrls);
        } else if (config.nodeUrl) {
            // Single node URL provided (backward compatibility)
            this.nodePool = new NodePoolManager([config.nodeUrl]);
        } else {
            // Load from environment
            this.nodePool = NodePoolManager.fromEnv();
        }

        // Perform initial health check
        this.nodePool.updateLatencies().catch(err => {
            console.warn('⚠️ Initial health check failed:', err.message);
        });

        // If privateKey provided, set it as current account
        if (config.privateKey) {
            const accountStore = useGrapthwayAccounts.getState();
            const existingAccount = accountStore.accounts.find(
                a => a.privateKey === config.privateKey
            );

            if (!existingAccount) {
                accountStore.addAccount(config.privateKey, config.label);
            } else {
                accountStore.selectAccount(existingAccount.id);
            }
        }
    }

    // Get current identity
    _getCurrentIdentity() {
        const account = useGrapthwayAccounts.getState().getCurrentAccount();
        if (!account) {
            throw new Error('No account selected. Add an account using addAccount() first.');
        }

        const keyPair = ec.keyFromPrivate(account.privateKey, 'hex');
        
        return {
            keyPair,
            address: account.address,
            publicKeyHex: account.publicKeyHex
        };
    }

    // Sign data
    _signData(data) {
        const { keyPair } = this._getCurrentIdentity();
        const hash = createHash('sha256').update(data, 'utf8').digest();
        return keyPair.sign(hash).toDER('hex');
    }

    // Create auth headers
    _createAuthHeaders(body = null, isAdmin = false) {
        const { address, publicKeyHex } = this._getCurrentIdentity();
        
        const headers = {
            'Content-Type': 'application/json',
            'X-Grapthway-Developer-ID': address,
            'X-Grapthway-User-Address': address,
            'X-Grapthway-User-PublicKey': publicKeyHex
        };

        // Sign the body or URI
        const dataToSign = body ? body : '';
        const signature = this._signData(dataToSign);
        headers['X-Grapthway-User-Signature'] = signature;

        // Add admin headers if needed
        if (isAdmin) {
            const adminSignature = this._signData(address);
            headers['X-Grapthway-Admin-Address'] = address;
            headers['X-Grapthway-Admin-Signature'] = adminSignature;
        }

        return headers;
    }

    // Make authenticated request with automatic failover
    async _request(endpoint, options = {}) {
        const { method = 'GET', body, authType = 'user', pathPrefix, maxRetries = 3 } = options;
        
        let prefix = '/api/v1';
        if (pathPrefix) {
            prefix = pathPrefix;
        } else if (authType === 'admin') {
            prefix = '/admin';
        } else if (authType === 'public') {
            prefix = '/public';
        }

        const bodyString = body ? JSON.stringify(body) : null;
        const headers = authType === 'public' 
            ? { 'Content-Type': 'application/json' }
            : this._createAuthHeaders(bodyString, authType === 'admin');

        let lastError;
        let attemptCount = 0;

        while (attemptCount < maxRetries) {
            attemptCount++;
            
            try {
                // Get the best available node
                const node = await this.nodePool.getBestNode();
                const url = `${node.url}${prefix}${endpoint}`;
                
                if (attemptCount > 1) {
                    console.log(`🔄 Retry attempt ${attemptCount}/${maxRetries} using ${node.url}`);
                }

                const fetchOptions = {
                    method,
                    headers,
                    timeout: 30000 // 30 second timeout
                };

                if (bodyString) {
                    fetchOptions.body = bodyString;
                }

                const response = await fetch(url, fetchOptions);
                
                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`HTTP ${response.status}: ${errorText}`);
                }

                const text = await response.text();
                return text ? JSON.parse(text) : {};

            } catch (error) {
                lastError = error;
                
                // Mark the node as failed
                const currentNode = await this.nodePool.getBestNode();
                this.nodePool.markNodeFailed(currentNode.url);
                
                console.warn(`⚠️ Request failed: ${error.message}`);
                
                // If this was the last retry, throw the error
                if (attemptCount >= maxRetries) {
                    throw new Error(`All ${maxRetries} request attempts failed. Last error: ${error.message}`);
                }
                
                // Wait before retry (exponential backoff)
                await new Promise(resolve => setTimeout(resolve, Math.pow(2, attemptCount) * 1000));
            }
        }

        throw lastError;
    }

    // Get node pool statistics
    getNodeStats() {
        return {
            nodes: this.nodePool.nodes.map(n => ({
                url: n.url,
                latency: n.latency === Infinity ? null : n.latency,
                isHealthy: n.isHealthy,
                failures: n.failures,
                lastCheck: n.lastCheck
            })),
            totalNodes: this.nodePool.nodes.length,
            healthyNodes: this.nodePool.nodes.filter(n => n.isHealthy).length
        };
    }

    // Force health check
    async checkNodeHealth() {
        await this.nodePool.updateLatencies();
        return this.getNodeStats();
    }

    /**
     * Pick the best node URL based on current latency
     * @param {boolean} forceCheck - Force a fresh health check before returning
     * @returns {Promise<string>} Best node URL
     */
    async getBestNodeUrl(forceCheck = false) {
        if (forceCheck) {
            await this.nodePool.updateLatencies();
        }
        
        const bestNode = await this.nodePool.getBestNode();
        return bestNode.url;
    }

    /**
     * Pick the best node with full details
     * @param {boolean} forceCheck - Force a fresh health check before returning
     * @returns {Promise<Object>} Best node object with url, latency, and health status
     */
    async getBestNodeDetails(forceCheck = false) {
    if (forceCheck) {
        await this.nodePool.updateLatencies();
    }
    
    const bestNode = await this.nodePool.getBestNode();
        return {
            url: bestNode.url,
            latency: bestNode.latency === Infinity ? null : bestNode.latency,
            isHealthy: bestNode.isHealthy,
            failures: bestNode.failures,
            lastCheck: bestNode.lastCheck
        };
    }

    // ========================================
    // CONFIGURATION MANAGEMENT
    // ========================================

    /**
     * Publish a service configuration
     * @param {Object} config - Service configuration
     * @returns {Promise<Object>}
     */
    async publishConfig(config) {
        const { address, publicKeyHex } = this._getCurrentIdentity();
        
        const configWithPubKey = {
            ...config,
            developerPubKey: publicKeyHex
        };

        const canonicalJson = marshalCanonical(configWithPubKey);
        const configSignature = this._signData(canonicalJson);
        const userSignature = this._signData(canonicalJson);

        const headers = {
            'Content-Type': 'application/json',
            'X-Grapthway-Developer-ID': address,
            'X-Grapthway-User-Address': address,
            'X-Grapthway-User-PublicKey': publicKeyHex,
            'X-Grapthway-User-Signature': userSignature,
            'X-Grapthway-Config-Signature': configSignature
        };

        const node = await this.nodePool.getBestNode();
        const response = await fetch(`${node.url}/api/v1/publish-config`, {
            method: 'POST',
            headers,
            body: canonicalJson
        });

        if (!response.ok) {
            throw new Error(`Failed to publish config: ${response.status} ${await response.text()}`);
        }

        return { success: true, service: config.service };
    }

    /**
     * Send heartbeat for service health
     * @param {Object} payload - Heartbeat data
     * @returns {Promise<Object>}
     */
    async heartbeat(payload) {
        const { address, publicKeyHex } = this._getCurrentIdentity();
        
        const payloadWithPubKey = {
            ...payload,
            developerPubKey: publicKeyHex
        };

        const canonicalJson = marshalCanonical(payloadWithPubKey);
        const configSignature = this._signData(canonicalJson);
        const userSignature = this._signData(canonicalJson);

        const headers = {
            'Content-Type': 'application/json',
            'X-Grapthway-Developer-ID': address,
            'X-Grapthway-User-Address': address,
            'X-Grapthway-User-PublicKey': publicKeyHex,
            'X-Grapthway-User-Signature': userSignature,
            'X-Grapthway-Config-Signature': configSignature
        };

        const node = await this.nodePool.getBestNode();
        const response = await fetch(`${node.url}/api/v1/health`, {
            method: 'POST',
            headers,
            body: canonicalJson
        });

        if (!response.ok) {
            throw new Error(`Heartbeat failed: ${response.status} ${await response.text()}`);
        }

        return { success: true, service: payload.service };
    }

    // ========================================
    // WALLET OPERATIONS
    // ========================================

    /**
     * Get wallet balance
     * @param {string} address - Wallet address
     * @returns {Promise<number>}
     */
    async getBalance(address) {
        const data = await this._request(`/wallet/balance?address=${address}`, {
            authType: 'public'
        });
        return data.balance;
    }

    /**
     * Get wallet nonce
     * @param {string} address - Wallet address
     * @returns {Promise<number>}
     */
    async getNonce(address) {
        const data = await this._request(`/wallet/nonce?address=${address}`, {
            authType: 'public'
        });
        return data.nonce;
    }

    /**
     * Transfer GCU
     * @param {string} to - Recipient address
     * @param {number} amount - Amount in GCU
     * @returns {Promise<Object>}
     */
    async transfer(to, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/wallet/transfer', {
            method: 'POST',
            body: { to, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Get transaction history
     * @param {string} address - Wallet address
     * @param {Object} options - Query options
     * @returns {Promise<Object>}
     */
    async getTransactionHistory(address, options = {}) {
        const { page = 1, limit = 10, start, end } = options;
        let query = `address=${address}&page=${page}&limit=${limit}`;
        
        if (start) query += `&start=${start}`;
        if (end) query += `&end=${end}`;

        return await this._request(`/wallet/history?${query}`, {
            authType: 'public'
        });
    }

    /**
     * Set allowance for spender
     * @param {string} spender - Spender address
     * @param {number} amount - Allowance amount
     * @returns {Promise<Object>}
     */
    async setAllowance(spender, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/wallet/allowance/set', {
            method: 'POST',
            body: { spender, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Remove allowance
     * @param {string} spender - Spender address
     * @returns {Promise<Object>}
     */
    async removeAllowance(spender) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/wallet/allowance/remove', {
            method: 'POST',
            body: { spender, nonce: nonce + 1 }
        });
    }

    /**
     * Get allowance
     * @param {string} owner - Owner address
     * @param {string} spender - Spender address
     * @returns {Promise<Object>}
     */
    async getAllowance(owner, spender) {
        return await this._request(`/wallet/allowance?owner=${owner}&spender=${spender}`, {
            authType: 'public'
        });
    }

    // ========================================
    // STAKING OPERATIONS
    // ========================================

    /**
     * Get staking status
     * @param {string} ownerAddress - Owner address
     * @returns {Promise<Object>}
     */
    async getStakingStatus(ownerAddress) {
        return await this._request(`/staking/status?owner_address=${ownerAddress}`, {
            authType: 'public'
        });
    }

    /**
     * Deposit stake
     * @param {number} amount - Amount to stake
     * @returns {Promise<Object>}
     */
    async stakeDeposit(amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/staking/deposit', {
            method: 'POST',
            body: { amount, nonce: nonce + 1 }
        });
    }

    /**
     * Withdraw stake
     * @param {number} amount - Amount to withdraw
     * @returns {Promise<Object>}
     */
    async stakeWithdraw(amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/staking/withdraw', {
            method: 'POST',
            body: { amount, nonce: nonce + 1 }
        });
    }

    /**
     * Assign stake to node
     * @param {string} nodePeerId - Node peer ID
     * @param {number} amount - Amount to assign
     * @returns {Promise<Object>}
     */
    async stakeAssign(nodePeerId, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/staking/assign', {
            method: 'POST',
            body: { nodePeerId, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Unassign stake from node
     * @param {string} nodePeerId - Node peer ID
     * @returns {Promise<Object>}
     */
    async stakeUnassign(nodePeerId) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/staking/unassign', {
            method: 'POST',
            body: { nodePeerId, nonce: nonce + 1 }
        });
    }

    // ========================================
    // TOKEN OPERATIONS
    // ========================================

    /**
     * Create a new token
     * @param {Object} tokenMetadata - Token configuration
     * @returns {Promise<Object>}
     */
    async createToken(tokenMetadata) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/create', {
            method: 'POST',
            body: { tokenMetadata, nonce: nonce + 1 }
        });
    }

    /**
     * Transfer tokens
     * @param {string} tokenAddress - Token contract address
     * @param {string} to - Recipient address
     * @param {number} amount - Amount to transfer
     * @returns {Promise<Object>}
     */
    async transferToken(tokenAddress, to, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/transfer', {
            method: 'POST',
            body: { tokenAddress, to, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Get token info
     * @param {string} tokenAddress - Token address
     * @returns {Promise<Object>}
     */
    async getTokenInfo(tokenAddress) {
        return await this._request(`/token/info?address=${tokenAddress}`, {
            authType: 'public'
        });
    }

    /**
     * Get token balance
     * @param {string} tokenAddress - Token address
     * @param {string} owner - Owner address
     * @returns {Promise<Object>}
     */
    async getTokenBalance(tokenAddress, owner) {
        return await this._request(`/token/balance?tokenAddress=${tokenAddress}&owner=${owner}`, {
            authType: 'public'
        });
    }

    /**
     * Get owned tokens
     * @param {string} owner - Owner address
     * @returns {Promise<Object>}
     */
    async getOwnedTokens(owner) {
        return await this._request(`/token/owned?owner=${owner}`, {
            authType: 'user'
        });
    }

    /**
     * Set token allowance
     * @param {string} tokenAddress - Token address
     * @param {string} spender - Spender address
     * @param {number} amount - Allowance amount
     * @returns {Promise<Object>}
     */
    async setTokenAllowance(tokenAddress, spender, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/allowance/set', {
            method: 'POST',
            body: { tokenAddress, spender, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Delete token allowance
     * @param {string} tokenAddress - Token address
     * @param {string} spender - Spender address
     * @returns {Promise<Object>}
     */
    async deleteTokenAllowance(tokenAddress, spender) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/allowance/delete', {
            method: 'POST',
            body: { tokenAddress, spender, nonce: nonce + 1 }
        });
    }

    /**
     * Get token allowance
     * @param {string} tokenAddress - Token address
     * @param {string} owner - Owner address
     * @param {string} spender - Spender address
     * @returns {Promise<Object>}
     */
    async getTokenAllowance(tokenAddress, owner, spender) {
        return await this._request(
            `/token/allowance?tokenAddress=${tokenAddress}&owner=${owner}&spender=${spender}`,
            { authType: 'public' }
        );
    }

    /**
     * Mint tokens (creator only)
     * @param {string} tokenAddress - Token address
     * @param {string} to - Recipient address
     * @param {number} amount - Amount to mint
     * @returns {Promise<Object>}
     */
    async mintToken(tokenAddress, to, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/mint', {
            method: 'POST',
            body: { tokenAddress, to, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Burn tokens
     * @param {string} tokenAddress - Token address
     * @param {number} amount - Amount to burn
     * @returns {Promise<Object>}
     */
    async burnToken(tokenAddress, amount) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/burn', {
            method: 'POST',
            body: { tokenAddress, amount, nonce: nonce + 1 }
        });
    }

    /**
     * Set token mint configuration
     * @param {string} tokenAddress - Token address
     * @param {Object} config - Mint configuration
     * @returns {Promise<Object>}
     */
    async setTokenMintConfig(tokenAddress, config) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/mintable/set', {
            method: 'POST',
            body: { tokenAddress, ...config, nonce: nonce + 1 }
        });
    }

    /**
     * Set token burn configuration
     * @param {string} tokenAddress - Token address
     * @param {Object} config - Burn configuration
     * @returns {Promise<Object>}
     */
    async setTokenBurnConfig(tokenAddress, config) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/burnable/set', {
            method: 'POST',
            body: { tokenAddress, ...config, nonce: nonce + 1 }
        });
    }

    /**
     * Lock token configuration
     * @param {string} tokenAddress - Token address
     * @returns {Promise<Object>}
     */
    async lockTokenConfig(tokenAddress) {
        const { address } = this._getCurrentIdentity();
        const nonce = await this.getNonce(address);
        
        return await this._request('/token/config/lock', {
            method: 'POST',
            body: { tokenAddress, nonce: nonce + 1 }
        });
    }

    /**
     * Get token history
     * @param {string} address - Address to query
     * @param {Object} options - Query options
     * @returns {Promise<Object>}
     */
    async getTokenHistory(address, options = {}) {
        const { tokenAddress = '', page = 1, limit = 10 } = options;
        let query = `address=${address}&page=${page}&limit=${limit}`;
        
        if (tokenAddress) query += `&tokenAddress=${tokenAddress}`;

        return await this._request(`/token/history?${query}`, {
            authType: 'public'
        });
    }

    // ========================================
    // NODE & NETWORK INFO
    // ========================================

    /**
     * Get gateway status
     * @returns {Promise<Object>}
     */
    async getGatewayStatus() {
        return await this._request('/gateway-status', {
            authType: 'public'
        });
    }

    /**
     * Get hardware stats
     * @returns {Promise<Object>}
     */
    async getHardwareStats() {
        return await this._request('/hardware-stats', {
            authType: 'public'
        });
    }

    /**
     * Get all services
     * @returns {Promise<Object>}
     */
    async getServices() {
        return await this._request('/services', {
            authType: 'public'
        });
    }

    /**
     * Get all pipelines
     * @returns {Promise<Object>}
     */
    async getPipelines() {
        return await this._request('/pipelines', {
            authType: 'public'
        });
    }

    /**
     * Get schema
     * @returns {Promise<Object>}
     */
    async getSchema() {
        return await this._request('/schema', {
            authType: 'public'
        });
    }

    // ========================================
    // WORKFLOW OPERATIONS
    // ========================================

    /**
     * Get workflow monitoring data
     * @param {Object} filters - Query filters
     * @returns {Promise<Array>}
     */
    async getWorkflowMonitoring(filters = {}) {
        const params = new URLSearchParams(filters);
        return await this._request(`/workflows/monitoring?${params}`, {
            authType: 'user'
        });
    }

    // ========================================
    // LOGGING
    // ========================================

    /**
     * Get logs (user scope)
     * @param {string} logType - Type of logs (gateway, ledger)
     * @param {Object} options - Query options
     * @returns {Promise<Array>}
     */
    async getLogs(logType, options = {}) {
        const { start, end, max } = options;
        let query = '';
        
        if (start) query += `start=${start}&`;
        if (end) query += `end=${end}&`;
        if (max) query += `max=${max}`;

        return await this._request(`/logs/${logType}?${query}`, {
            authType: 'user'
        });
    }

    // ========================================
    // ADMIN OPERATIONS
    // ========================================

    /**
     * Create a new wallet (admin only)
     * @returns {Promise<Object>}
     */
    async createWallet() {
        return await this._request('/wallet/create', {
            method: 'POST',
            authType: 'admin',
            body: {}
        });
    }

    /**
     * Get admin logs (admin only)
     * @param {string} logType - Type of logs
     * @param {Object} options - Query options
     * @returns {Promise<Array>}
     */
    async getAdminLogs(logType, options = {}) {
        const { start, end, max } = options;
        let query = '';
        
        if (start) query += `start=${start}&`;
        if (end) query += `end=${end}&`;
        if (max) query += `max=${max}`;

        return await this._request(`/logs/${logType}?${query}`, {
            authType: 'admin'
        });
    }

    /**
     * Get all workflow monitoring (admin only)
     * @param {Object} filters - Query filters
     * @returns {Promise<Array>}
     */
    async getAdminWorkflowMonitoring(filters = {}) {
        const params = new URLSearchParams(filters);
        return await this._request(`/workflows/monitoring?${params}`, {
            authType: 'admin'
        });
    }
}

// ========================================
// MIDDLEWARE FOR EXPRESS
// ========================================

/**
 * Create Grapthway verification middleware with node whitelist
 * @param {Object} options - Configuration options
 * @returns {Function} Express middleware
 */
export function createGrapthwayMiddleware(options = {}) {
    // Load allowed nodes from environment or config
    let allowedNodes = [];
    
    if (options.nodeUrls && Array.isArray(options.nodeUrls)) {
        allowedNodes = options.nodeUrls;
    } else {
        // Load from environment
        if (process.env.GRAPTHWAY_NODE_URL) {
            allowedNodes.push(process.env.GRAPTHWAY_NODE_URL);
        }
        
        let index = 1;
        while (process.env[`GRAPTHWAY_NODE_URL_${index}`]) {
            allowedNodes.push(process.env[`GRAPTHWAY_NODE_URL_${index}`]);
            index++;
        }
    }

    // Extract domains/hosts from URLs for comparison
    const allowedDomains = allowedNodes.map(url => {
        try {
            const parsed = new URL(url);
            return parsed.hostname;
        } catch {
            return url;
        }
    });

    console.log(`🔒 Grapthway middleware initialized with ${allowedDomains.length} allowed node(s)`);
    allowedDomains.forEach(domain => console.log(`   ✅ ${domain}`));

    return (req, res, next) => {
        const signatureHex = req.headers['x-grapthway-signature'];
        const nodeAddress = req.headers['x-grapthway-node-address'];
        const nodePublicKeyHex = req.headers['x-grapthway-node-public-key'];

        if (!signatureHex || !nodeAddress || !nodePublicKeyHex) {
            console.warn('⚠️ Request missing Grapthway signature headers');
            return res.status(403).json({ 
                error: 'Forbidden: Missing Grapthway signature headers.' 
            });
        }

        try {
            // 1. Verify that the address corresponds to the public key
            const pubKeyBytes = Buffer.from(nodePublicKeyHex.substring(2), 'hex');
            const pubKeyHash = createHash('sha256').update(pubKeyBytes).digest();
            const derivedAddress = `0x${pubKeyHash.toString('hex').slice(-40)}`;

            if (derivedAddress !== nodeAddress) {
                console.error('❌ Verification failed: Node address does not match public key');
                return res.status(403).json({ 
                    error: 'Forbidden: Invalid Grapthway node identity.' 
                });
            }

            // 2. Check if request origin is from allowed nodes
            const origin = req.headers['origin'] || req.headers['referer'] || '';
            const isFromAllowedNode = allowedDomains.some(domain => {
                return origin.includes(domain) || req.ip.includes(domain);
            });

            // Also check X-Forwarded-For header
            const forwardedFor = req.headers['x-forwarded-for'];
            const isForwardedFromAllowed = forwardedFor && allowedDomains.some(domain => 
                forwardedFor.includes(domain)
            );

            if (!isFromAllowedNode && !isForwardedFromAllowed && allowedDomains.length > 0) {
                console.warn(`⚠️ Request from unauthorized node. Origin: ${origin}, IP: ${req.ip}`);
                return res.status(403).json({ 
                    error: 'Forbidden: Request not from authorized Grapthway node.' 
                });
            }

            // 3. Verify the signature against the raw request body
            if (!req.rawBody) {
                console.error('❌ Verification failed: Raw request body not found');
                return res.status(500).json({ 
                    error: 'Internal Server Error: Missing raw body for verification.' 
                });
            }

            const requestHash = createHash('sha256').update(req.rawBody).digest();
            const key = ec.keyFromPublic(nodePublicKeyHex, 'hex');
            
            if (key.verify(requestHash, signatureHex)) {
                console.log(`✅ Request signature verified from Grapthway node: ${nodeAddress}`);
                
                // Attach node info to request for use in handlers
                req.grapthwayNode = {
                    address: nodeAddress,
                    publicKey: nodePublicKeyHex,
                    verified: true
                };
                
                next();
            } else {
                console.error('❌ Verification failed: Invalid signature');
                return res.status(403).json({ 
                    error: 'Forbidden: Invalid Grapthway signature.' 
                });
            }
        } catch (error) {
            console.error('❌ Error during Grapthway signature verification:', error);
            return res.status(403).json({ 
                error: 'Forbidden: Signature verification failed.',
                details: error.message 
            });
        }
    };
}

/**
 * Legacy middleware (for backward compatibility)
 * Uses default node URLs from environment
 */
export const grapthwayVerificationMiddleware = createGrapthwayMiddleware();

// Default export
export default GrapthwayClient;