// middleware.go
package grapthway

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

type MiddlewareConfig struct {
	NodeURLs []string
}

// GrapthwayMiddleware creates HTTP middleware that verifies requests from Grapthway nodes
func GrapthwayMiddleware(config *MiddlewareConfig) func(http.Handler) http.Handler {
	// Load allowed nodes from environment or config
	var allowedNodes []string
	if config != nil && len(config.NodeURLs) > 0 {
		allowedNodes = config.NodeURLs
	} else {
		// Load from environment
		if nodeURL := os.Getenv("GRAPTHWAY_NODE_URL"); nodeURL != "" {
			allowedNodes = append(allowedNodes, nodeURL)
		}

		index := 1
		for {
			envKey := fmt.Sprintf("GRAPTHWAY_NODE_URL_%d", index)
			if nodeURL := os.Getenv(envKey); nodeURL != "" {
				allowedNodes = append(allowedNodes, nodeURL)
				index++
			} else {
				break
			}
		}
	}

	// Extract domains/hosts from URLs for comparison
	allowedDomains := make([]string, len(allowedNodes))
	for i, nodeURL := range allowedNodes {
		if parsedURL, err := url.Parse(nodeURL); err == nil {
			allowedDomains[i] = parsedURL.Hostname()
		} else {
			allowedDomains[i] = nodeURL
		}
	}

	fmt.Printf("🔒 Grapthway middleware initialized with %d allowed node(s)\n", len(allowedDomains))
	for _, domain := range allowedDomains {
		fmt.Printf("   ✅ %s\n", domain)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			signatureHex := r.Header.Get("X-Grapthway-Signature")
			nodeAddress := r.Header.Get("X-Grapthway-Node-Address")
			nodePublicKeyHex := r.Header.Get("X-Grapthway-Node-Public-Key")

			if signatureHex == "" || nodeAddress == "" || nodePublicKeyHex == "" {
				fmt.Println("⚠️  Request missing Grapthway signature headers")
				http.Error(w, `{"error": "Forbidden: Missing Grapthway signature headers."}`, http.StatusForbidden)
				return
			}

			// 1. Verify that the address corresponds to the public key
			pubKeyBytes, err := hex.DecodeString(strings.TrimPrefix(nodePublicKeyHex, "0x"))
			if err != nil {
				fmt.Printf("❌ Error decoding public key: %v\n", err)
				http.Error(w, `{"error": "Forbidden: Invalid public key format."}`, http.StatusForbidden)
				return
			}

			// Verify address matches public key
			pubKeyHash := sha256.Sum256(pubKeyBytes[1:]) // Skip 0x04 prefix
			derivedAddress := "0x" + hex.EncodeToString(pubKeyHash[len(pubKeyHash)-20:])

			if !strings.EqualFold(derivedAddress, nodeAddress) {
				fmt.Println("❌ Verification failed: Node address does not match public key")
				http.Error(w, `{"error": "Forbidden: Invalid Grapthway node identity."}`, http.StatusForbidden)
				return
			}

			// 2. Check if request origin is from allowed nodes
			origin := r.Header.Get("Origin")
			if origin == "" {
				origin = r.Header.Get("Referer")
			}

			isFromAllowedNode := false
			for _, domain := range allowedDomains {
				if strings.Contains(origin, domain) || strings.Contains(r.RemoteAddr, domain) {
					isFromAllowedNode = true
					break
				}
			}

			// Also check X-Forwarded-For header
			if !isFromAllowedNode {
				forwardedFor := r.Header.Get("X-Forwarded-For")
				if forwardedFor != "" {
					for _, domain := range allowedDomains {
						if strings.Contains(forwardedFor, domain) {
							isFromAllowedNode = true
							break
						}
					}
				}
			}

			if !isFromAllowedNode && len(allowedDomains) > 0 {
				fmt.Printf("⚠️  Request from unauthorized node. Origin: %s, IP: %s\n", origin, r.RemoteAddr)
				http.Error(w, `{"error": "Forbidden: Request not from authorized Grapthway node."}`, http.StatusForbidden)
				return
			}

			// 3. Read and verify the signature against the raw request body
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				fmt.Printf("❌ Error reading request body: %v\n", err)
				http.Error(w, `{"error": "Internal Server Error: Failed to read body."}`, http.StatusInternalServerError)
				return
			}

			// Restore body for downstream handlers
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			// Calculate hash of the body (Grapthway always signs the body, even if empty)
			requestHash := sha256.Sum256(bodyBytes)

			// Parse public key
			publicKey, err := crypto.UnmarshalPubkey(pubKeyBytes)
			if err != nil {
				fmt.Printf("❌ Error unmarshaling public key: %v\n", err)
				http.Error(w, `{"error": "Forbidden: Invalid public key."}`, http.StatusForbidden)
				return
			}

			// Decode signature
			signatureBytes, err := hex.DecodeString(signatureHex)
			if err != nil {
				fmt.Printf("❌ Error decoding signature: %v\n", err)
				http.Error(w, `{"error": "Forbidden: Invalid signature format."}`, http.StatusForbidden)
				return
			}

			// *** CRITICAL FIX: Grapthway node uses ecdsa.SignASN1 (DER/ASN.1 format) ***
			// We must verify using ecdsa.VerifyASN1 to match
			verified := ecdsa.VerifyASN1(publicKey, requestHash[:], signatureBytes)

			if !verified {
				fmt.Println("❌ Verification failed: Invalid signature")
				fmt.Printf("   Body length: %d bytes\n", len(bodyBytes))
				fmt.Printf("   Body hash: %x\n", requestHash)
				fmt.Printf("   Signature: %x\n", signatureBytes)
				fmt.Printf("   Public key: %x\n", crypto.FromECDSAPub(publicKey))
				http.Error(w, `{"error": "Forbidden: Invalid Grapthway signature."}`, http.StatusForbidden)
				return
			}

			fmt.Printf("✅ Request signature verified from Grapthway node: %s (%s %s)\n",
				nodeAddress, r.Method, r.RequestURI)

			// Attach node info to context for use in handlers
			r.Header.Set("X-Grapthway-Verified", "true")
			r.Header.Set("X-Grapthway-Verified-Address", nodeAddress)

			next.ServeHTTP(w, r)
		})
	}
}

// HexToPublicKey converts a hex-encoded string back to an ECDSA public key.
func HexToPublicKey(hexKey string) (*ecdsa.PublicKey, error) {
	bytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	x, y := elliptic.Unmarshal(secp256k1.S256(), bytes)
	if x == nil {
		return nil, fmt.Errorf("invalid public key")
	}
	return &ecdsa.PublicKey{Curve: secp256k1.S256(), X: x, Y: y}, nil
}

// PublicKeyToAddress converts an ECDSA public key into a human-readable address.
func PublicKeyToAddress(pub *ecdsa.PublicKey) string {
	pubBytes := elliptic.Marshal(secp256k1.S256(), pub.X, pub.Y)
	hash := sha256.Sum256(pubBytes[1:])
	return fmt.Sprintf("0x%s", hex.EncodeToString(hash[len(hash)-20:]))
}

// VerifySignature checks if a given signature is valid for the data hash and public key.
func VerifySignature(hash []byte, signature []byte, pubKey *ecdsa.PublicKey) bool {
	return ecdsa.VerifyASN1(pubKey, hash, signature)
}

func GetAuthenticatedUser(ctx context.Context) (string, bool) {
	// Standard context values are retrieved using the key they were set with.
	if val := ctx.Value("authenticatedUser"); val != nil {
		if address, ok := val.(string); ok {
			return address, true
		}
	}
	return "", false
}

func UserAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			next.ServeHTTP(w, r)
			return
		}

		userAddress := r.Header.Get("X-Grapthway-User-Address")
		publicKeyHex := r.Header.Get("X-Grapthway-User-PublicKey")
		signatureHex := r.Header.Get("X-Grapthway-User-Signature")

		if userAddress == "" || publicKeyHex == "" || signatureHex == "" {
			next.ServeHTTP(w, r)
			return
		}

		pubKey, err := HexToPublicKey(publicKeyHex)
		if err != nil {
			http.Error(w, "Invalid user public key", http.StatusBadRequest)
			return
		}
		if PublicKeyToAddress(pubKey) != userAddress {
			http.Error(w, "User address does not match public key", http.StatusForbidden)
			return
		}

		var dataToVerify []byte
		if r.Method == "GET" {
			dataToVerify = []byte(r.RequestURI)
		} else {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Cannot read request body for signature verification", http.StatusInternalServerError)
				return
			}
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			dataToVerify = bodyBytes
		}

		signature, err := hex.DecodeString(signatureHex)
		if err != nil {
			http.Error(w, "Invalid signature format", http.StatusBadRequest)
			return
		}

		hash := sha256.Sum256(dataToVerify)
		if !VerifySignature(hash[:], signature, pubKey) {
			log.Printf("[AUTH-FAIL] User signature verification failed for address %s and data: %s", userAddress, string(dataToVerify))
			http.Error(w, "Invalid user signature", http.StatusForbidden)
			return
		}

		ctx := context.WithValue(r.Context(), "authenticatedUser", userAddress)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
