// example/main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"

	"your-module-path/grapthway"
)

func main() {
	// Example 1: Create client from environment variables
	client, err := grapthway.NewGrapthwayClient(nil)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Example 2: Create client with explicit configuration
	client, err = grapthway.NewGrapthwayClient(&grapthway.ClientConfig{
		NodeURLs: []string{
			"https://grapthway-node-1.example.com",
			"https://grapthway-node-2.example.com",
		},
		PrivateKey: "your-private-key-hex",
		Label:      "My Account",
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Example 3: Add multiple accounts
	accountManager := client.GetAccountManager()
	account1, err := accountManager.AddAccount("private-key-1", "Account 1")
	if err != nil {
		log.Fatalf("Failed to add account: %v", err)
	}
	fmt.Printf("Added account: %s\n", account1.Address)

	// Example 4: Check node health
	stats, err := client.CheckNodeHealth()
	if err != nil {
		log.Fatalf("Failed to check node health: %v", err)
	}
	fmt.Printf("Healthy nodes: %d/%d\n", stats.HealthyNodes, stats.TotalNodes)

	// Example 5: Get balance
	balance, err := client.GetBalance(account1.Address)
	if err != nil {
		log.Fatalf("Failed to get balance: %v", err)
	}
	fmt.Printf("Balance: %.2f GCU\n", balance)

	// Example 6: Transfer GCU
	result, err := client.Transfer("0x1234567890abcdef", 100.0)
	if err != nil {
		log.Fatalf("Failed to transfer: %v", err)
	}
	fmt.Printf("Transfer result: %v\n", result)

	// Example 7: Create token
	tokenResult, err := client.CreateToken(map[string]interface{}{
		"name":        "My Token",
		"symbol":      "MTK",
		"totalSupply": 1000000,
		"decimals":    18,
	})
	if err != nil {
		log.Fatalf("Failed to create token: %v", err)
	}
	fmt.Printf("Token created: %v\n", tokenResult)

	// Example 8: Publish service configuration
	config := map[string]interface{}{
		"service": "my-service",
		"version": "1.0.0",
		"endpoints": []string{
			"https://api.example.com",
		},
	}
	publishResult, err := client.PublishConfig(config)
	if err != nil {
		log.Fatalf("Failed to publish config: %v", err)
	}
	fmt.Printf("Config published: %v\n", publishResult)

	// Example 9: Send heartbeat
	heartbeatResult, err := client.Heartbeat(map[string]interface{}{
		"service": "my-service",
		"status":  "healthy",
	})
	if err != nil {
		log.Fatalf("Failed to send heartbeat: %v", err)
	}
	fmt.Printf("Heartbeat sent: %v\n", heartbeatResult)

	// Example 10: Get transaction history
	history, err := client.GetTransactionHistory(account1.Address, &grapthway.TransactionHistoryOptions{
		Page:  1,
		Limit: 10,
	})
	if err != nil {
		log.Fatalf("Failed to get history: %v", err)
	}
	historyJSON, _ := json.MarshalIndent(history, "", "  ")
	fmt.Printf("Transaction history:\n%s\n", historyJSON)

	// Example 11: Staking operations
	stakeResult, err := client.StakeDeposit(1000.0)
	if err != nil {
		log.Fatalf("Failed to stake: %v", err)
	}
	fmt.Printf("Stake deposited: %v\n", stakeResult)

	// Example 12: Get gateway status
	status, err := client.GetGatewayStatus()
	if err != nil {
		log.Fatalf("Failed to get status: %v", err)
	}
	statusJSON, _ := json.MarshalIndent(status, "", "  ")
	fmt.Printf("Gateway status:\n%s\n", statusJSON)
}
