package util

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/graphql-go/graphql"
	json "github.com/json-iterator/go"
)

// ValueToString converts any Go interface into a GraphQL-compliant string representation.
// This function is now the single source of truth for value formatting.
func ValueToString(value interface{}, argType graphql.Input) string {
	if value == nil {
		return "null"
	}

	// Check for enums first, as they are represented as strings but shouldn't be quoted.
	if argType != nil {
		if _, ok := graphql.GetNamed(argType).(*graphql.Enum); ok {
			if s, ok := value.(string); ok {
				return s
			}
		}
	}

	switch v := value.(type) {
	case string:
		// Use JSON marshaling for strings to handle all special characters and escaping.
		jsonBytes, _ := json.Marshal(v)
		return string(jsonBytes)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	// case float32:
	// 	// Use 'g' format which is compact and prints whole numbers without decimals.
	// 	return strconv.FormatFloat(float64(v), 'g', -1, 32)
	// case float64:
	// 	return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		// Format as integer if whole number
		if v == float32(int(v)) {
			return strconv.Itoa(int(v))
		}
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		// Format as integer if whole number
		if v == float64(int(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case json.Number:
		// Handle json.Number by attempting to format as a compact float.
		f, err := v.Float64()
		if err != nil {
			return v.String() // Fallback to raw string representation.
		}
		return strconv.FormatFloat(f, 'g', -1, 64)
	case []interface{}:
		var items []string
		var itemType graphql.Input

		// Determine the item type if argType is a list
		if argType != nil {
			if listType, ok := argType.(*graphql.List); ok {
				itemType = listType.OfType
			} else if nonNullType, ok := argType.(*graphql.NonNull); ok {
				if listType, ok := nonNullType.OfType.(*graphql.List); ok {
					itemType = listType.OfType
				}
			}
		}

		for _, item := range v {
			items = append(items, ValueToString(item, itemType))
		}
		return fmt.Sprintf("[%s]", strings.Join(items, ", "))
	case map[string]interface{}:
		var fields []string
		var inputObject *graphql.InputObject

		// Determine the input object type
		if argType != nil {
			namedType := graphql.GetNamed(argType)
			if inputObj, ok := namedType.(*graphql.InputObject); ok {
				inputObject = inputObj
			}
		}

		// Sort keys for consistent output
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		// Properly format nested objects
		for key, val := range v {
			var fieldType graphql.Input
			if inputObject != nil {
				if field, ok := inputObject.Fields()[key]; ok {
					fieldType = field.Type
				}
			}
			fields = append(fields, fmt.Sprintf("%s: %s", key, ValueToString(val, fieldType)))
		}
		return fmt.Sprintf("{%s}", strings.Join(fields, ", "))
	default:
		// For any other type, try to convert to a basic type first
		if stringer, ok := value.(fmt.Stringer); ok {
			return ValueToString(stringer.String(), argType)
		}

		// Check if it's a slice of a different type
		if slice, ok := convertToInterfaceSlice(value); ok {
			return ValueToString(slice, argType)
		}

		// Check if it's a map of a different type
		if m, ok := convertToStringInterfaceMap(value); ok {
			return ValueToString(m, argType)
		}

		// Final fallback: JSON marshal but then parse as string if it's a simple value
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}

		// If it's a simple JSON value (not object/array), treat as string
		jsonStr := string(jsonBytes)
		if !strings.HasPrefix(jsonStr, "{") && !strings.HasPrefix(jsonStr, "[") {
			return jsonStr
		}

		// For complex JSON, try to parse back and format properly
		var parsed interface{}
		if err := json.Unmarshal(jsonBytes, &parsed); err == nil {
			return ValueToString(parsed, argType)
		}

		return jsonStr
	}
}

// convertToInterfaceSlice attempts to convert various slice types to []interface{}
func convertToInterfaceSlice(value interface{}) ([]interface{}, bool) {
	switch v := value.(type) {
	case []string:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	case []int:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	case []float64:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	case []bool:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	case []map[string]interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = item
		}
		return result, true
	}
	return nil, false
}

// convertToStringInterfaceMap attempts to convert various map types to map[string]interface{}
func convertToStringInterfaceMap(value interface{}) (map[string]interface{}, bool) {
	switch v := value.(type) {
	case map[string]string:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = val
		}
		return result, true
	case map[string]int:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = val
		}
		return result, true
	case map[string]float64:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = val
		}
		return result, true
	case map[string]bool:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = val
		}
		return result, true
	}
	return nil, false
}
