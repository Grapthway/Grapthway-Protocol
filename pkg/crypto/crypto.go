package crypto

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
	json "github.com/json-iterator/go"
	libp2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
)

// Identity represents a participant's cryptographic identity in the Grapthway network.
type Identity struct {
	PrivateKey *ecdsa.PrivateKey
	PublicKey  *ecdsa.PublicKey
	Address    string
}

// NewIdentity creates a new, unique cryptographic identity.
func NewIdentity() (*Identity, error) {
	privateKey, err := ecdsa.GenerateKey(secp256k1.S256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	publicKey := &privateKey.PublicKey
	address := PublicKeyToAddress(publicKey)

	return &Identity{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
		Address:    address,
	}, nil
}

// IdentityFromPrivateKeyHex reconstructs an Identity from a hex-encoded private key.
// crypto.go

func IdentityFromPrivateKeyHex(hexKey string) (*Identity, error) {
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid hex string for private key: %w", err)
	}

	privateKey := new(ecdsa.PrivateKey)
	privateKey.D = new(big.Int).SetBytes(keyBytes)
	privateKey.PublicKey.Curve = secp256k1.S256()

	// Correct way to derive the public key
	privateKey.PublicKey.X, privateKey.PublicKey.Y = privateKey.PublicKey.Curve.ScalarBaseMult(privateKey.D.Bytes())

	if privateKey.PublicKey.X == nil {
		return nil, fmt.Errorf("invalid private key")
	}

	publicKey := &privateKey.PublicKey
	address := PublicKeyToAddress(publicKey)

	return &Identity{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
		Address:    address,
	}, nil
}

// --- CHANGE START: Added method to convert to libp2p key type ---
// ToLibp2pPrivateKey converts the ECDSA private key to a libp2p compatible private key.
// This is crucial for ensuring the P2P network identity and the wallet identity are the same.
func (id *Identity) ToLibp2pPrivateKey() (libp2p_crypto.PrivKey, error) {
	privBytes := id.PrivateKey.D.Bytes()
	// Pad the key to 32 bytes if it's smaller (which is common).
	paddedPrivBytes := make([]byte, 32)
	copy(paddedPrivBytes[32-len(privBytes):], privBytes)

	return libp2p_crypto.UnmarshalSecp256k1PrivateKey(paddedPrivBytes)
}

// --- CHANGE END ---

// FromECDSAPub serializes a public key to the uncompressed format.
func FromECDSAPub(pub *ecdsa.PublicKey) []byte {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil
	}
	return elliptic.Marshal(secp256k1.S256(), pub.X, pub.Y)
}

// Sign generates a digital signature for the given data hash.
func (id *Identity) Sign(hash []byte) ([]byte, error) {
	return ecdsa.SignASN1(rand.Reader, id.PrivateKey, hash)
}

// VerifySignature checks if a given signature is valid for the data hash and public key.
func VerifySignature(hash []byte, signature []byte, pubKey *ecdsa.PublicKey) bool {
	return ecdsa.VerifyASN1(pubKey, hash, signature)
}

// PublicKeyToAddress converts an ECDSA public key into a human-readable address.
func PublicKeyToAddress(pub *ecdsa.PublicKey) string {
	pubBytes := elliptic.Marshal(secp256k1.S256(), pub.X, pub.Y)
	hash := sha256.Sum256(pubBytes[1:])
	return fmt.Sprintf("0x%s", hex.EncodeToString(hash[len(hash)-20:]))
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

// PublicKeyToHex converts an ecdsa.PublicKey to a hex string.
func PublicKeyToHex(pubKey *ecdsa.PublicKey) string {
	if pubKey == nil || pubKey.X == nil || pubKey.Y == nil {
		return ""
	}
	return hex.EncodeToString(elliptic.Marshal(secp256k1.S256(), pubKey.X, pubKey.Y))
}

func normalizeData(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		normalized := make(map[string]interface{})
		for key, val := range v {
			if val == nil {
				continue
			}
			if strVal, ok := val.(string); ok && strVal == "" {
				continue
			}
			if sliceVal, ok := val.([]interface{}); ok && len(sliceVal) == 0 {
				continue
			}
			if mapVal, ok := val.(map[string]interface{}); ok && len(mapVal) == 0 {
				continue
			}
			normalized[key] = normalizeData(val)
		}
		return normalized

	case []interface{}:
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			normalized[i] = normalizeData(item)
		}
		return normalized

	default:
		return data
	}
}

func marshalCanonicalRecursive(data interface{}) ([]byte, error) {
	normalized := normalizeData(data)

	if normalized == nil {
		return []byte("null"), nil
	}

	switch v := normalized.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var buf bytes.Buffer
		buf.WriteString("{")
		isFirst := true
		for _, k := range keys {
			val := v[k]
			if !isFirst {
				buf.WriteString(",")
			}
			isFirst = false

			keyBytes, err := json.Marshal(k)
			if err != nil {
				return nil, err
			}
			buf.Write(keyBytes)
			buf.WriteString(":")

			valBytes, err := marshalCanonicalRecursive(val)
			if err != nil {
				return nil, err
			}
			buf.Write(valBytes)
		}
		buf.WriteString("}")
		return buf.Bytes(), nil

	case []interface{}:
		var buf bytes.Buffer
		buf.WriteString("[")
		for i, item := range v {
			if i > 0 {
				buf.WriteString(",")
			}
			itemBytes, err := marshalCanonicalRecursive(item)
			if err != nil {
				return nil, err
			}
			buf.Write(itemBytes)
		}
		buf.WriteString("]")
		return buf.Bytes(), nil

	default:
		return json.Marshal(normalized)
	}
}

// MarshalCanonicalJSON creates a deterministic, sorted JSON string from any struct or map.
func MarshalCanonicalJSON(data interface{}) ([]byte, error) {
	tempBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var genericData interface{}
	if err := json.Unmarshal(tempBytes, &genericData); err != nil {
		return nil, err
	}

	return marshalCanonicalRecursive(genericData)
}
