package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// EncodedCursor is a serialized keyset cursor for pagination.
type EncodedCursor struct {
	Values []any `json:"v"`
	ID     int64 `json:"id"`
}

// EncodeCursor serializes a cursor to a base64 string.
func EncodeCursor(values []any, id int64) (string, error) {
	c := EncodedCursor{Values: values, ID: id}
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(data), nil
}

// DecodeCursor deserializes a cursor from a base64 string.
func DecodeCursor(encoded string) (*EncodedCursor, error) {
	data, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	var c EncodedCursor
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("unmarshal cursor: %w", err)
	}
	return &c, nil
}
