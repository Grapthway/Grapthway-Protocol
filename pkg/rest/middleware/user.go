package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"grapthway/pkg/crypto"
	"io"
	"log"
	"net/http"
)

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

		pubKey, err := crypto.HexToPublicKey(publicKeyHex)
		if err != nil {
			http.Error(w, "Invalid user public key", http.StatusBadRequest)
			return
		}
		if crypto.PublicKeyToAddress(pubKey) != userAddress {
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
		if !crypto.VerifySignature(hash[:], signature, pubKey) {
			log.Printf("[AUTH-FAIL] User signature verification failed for address %s and data: %s", userAddress, string(dataToVerify))
			http.Error(w, "Invalid user signature", http.StatusForbidden)
			return
		}

		ctx := context.WithValue(r.Context(), "authenticatedUser", userAddress)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
