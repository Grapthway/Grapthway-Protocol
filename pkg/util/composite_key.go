package util

import (
	"fmt"
	"strings"
)

// GetCompositeKey creates the unique namespaced key for a service.
func GetCompositeKey(developerAddress, serviceName string) string {
	return fmt.Sprintf("%s:%s", developerAddress, serviceName)
}

// ParseCompositeKey extracts the developer address and service name from a unique key.
func ParseCompositeKey(compositeKey string) (developerAddress, serviceName string, err error) {
	parts := strings.SplitN(compositeKey, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid composite key format: %s", compositeKey)
	}
	return parts[0], parts[1], nil
}
