package query

import (
	"encoding/base64"
	"testing"
)

func TestEncodeCursor_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []any
		id     int64
	}{
		{"empty values", []any{}, 123},
		{"single int", []any{42}, 1},
		{"single string", []any{"hello"}, 2},
		{"multiple values", []any{100, "world", 3.14}, 3},
		{"nil value", []any{nil}, 4},
		{"mixed with nil", []any{1, nil, "test"}, 5},
		{"large id", []any{1}, 9223372036854775807}, // max int64
		{"negative id", []any{1}, -1},
		{"zero id", []any{1}, 0},
		{"bool values", []any{true, false}, 6},
		{"float values", []any{1.5, -2.7, 0.0}, 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeCursor(tt.values, tt.id)
			if err != nil {
				t.Fatalf("EncodeCursor() error = %v", err)
			}

			if encoded == "" {
				t.Error("EncodeCursor() returned empty string")
			}

			decoded, err := DecodeCursor(encoded)
			if err != nil {
				t.Fatalf("DecodeCursor() error = %v", err)
			}

			if decoded.ID != tt.id {
				t.Errorf("ID = %d, want %d", decoded.ID, tt.id)
			}

			if len(decoded.Values) != len(tt.values) {
				t.Errorf("len(Values) = %d, want %d", len(decoded.Values), len(tt.values))
			}
		})
	}
}

func TestEncodeCursor_Base64Format(t *testing.T) {
	encoded, err := EncodeCursor([]any{1, 2, 3}, 100)
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}

	// Should be valid base64 URL encoding
	_, err = base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		t.Errorf("encoded string is not valid base64 URL: %v", err)
	}
}

func TestDecodeCursor_InvalidBase64(t *testing.T) {
	_, err := DecodeCursor("not-valid-base64!!!")
	if err == nil {
		t.Error("DecodeCursor() expected error for invalid base64")
	}
}

func TestDecodeCursor_InvalidJSON(t *testing.T) {
	// Valid base64 but invalid JSON
	invalidJSON := base64.URLEncoding.EncodeToString([]byte("not json"))
	_, err := DecodeCursor(invalidJSON)
	if err == nil {
		t.Error("DecodeCursor() expected error for invalid JSON")
	}
}

func TestDecodeCursor_EmptyString(t *testing.T) {
	_, err := DecodeCursor("")
	if err == nil {
		t.Error("DecodeCursor() expected error for empty string")
	}
}

func TestEncodedCursor_JSONFields(t *testing.T) {
	// Verify the JSON field names are correct
	encoded, _ := EncodeCursor([]any{1, 2}, 42)
	data, _ := base64.URLEncoding.DecodeString(encoded)
	json := string(data)

	if !contains(json, `"v"`) {
		t.Errorf("JSON should contain 'v' field, got: %s", json)
	}
	if !contains(json, `"id"`) {
		t.Errorf("JSON should contain 'id' field, got: %s", json)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
