package main

import (
	"encoding/base64"
	"testing"
)

func TestParseKEK(t *testing.T) {
	// Generate a 32-byte valid key
	validKey := make([]byte, 32)
	for i := 0; i < 32; i++ {
		validKey[i] = byte(i)
	}
	validKeyBase64 := base64.StdEncoding.EncodeToString(validKey)
	validKeyString := string(validKey)

	// Short key
	shortKey := make([]byte, 16)
	shortKeyBase64 := base64.StdEncoding.EncodeToString(shortKey)
	shortKeyString := string(shortKey)

	tests := []struct {
		name      string
		masterKey string
		expectErr bool
	}{
		{
			name:      "Valid Base64 32-byte key",
			masterKey: validKeyBase64,
			expectErr: false,
		},
		{
			name:      "Valid String 32-byte key",
			masterKey: validKeyString,
			expectErr: false,
		},
		{
			name:      "Invalid Short Base64 key",
			masterKey: shortKeyBase64,
			expectErr: true,
		},
		{
			name:      "Invalid Short String key",
			masterKey: shortKeyString,
			expectErr: true,
		},
		{
			name:      "Empty key",
			masterKey: "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseKEK(tt.masterKey)
			if (err != nil) != tt.expectErr {
				t.Errorf("parseKEK() error = %v, expectErr %v", err, tt.expectErr)
			}
		})
	}
}
