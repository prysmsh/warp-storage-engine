package kms

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalProvider_RequiresMasterKey(t *testing.T) {
	ctx := context.Background()
	_, err := NewLocalProvider(ctx, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "master_key")

	_, err = NewLocalProvider(ctx, map[string]interface{}{"master_key": ""})
	assert.Error(t, err)

	_, err = NewLocalProvider(ctx, map[string]interface{}{"other": "x"})
	assert.Error(t, err)
}

func TestNewLocalProvider_InvalidKeySize(t *testing.T) {
	ctx := context.Background()
	// 8 bytes - invalid
	_, err := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString([]byte("12345678")),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "16, 24, or 32")
}

func TestNewLocalProvider_ValidHexKey(t *testing.T) {
	ctx := context.Background()
	key32 := make([]byte, 32)
	for i := range key32 {
		key32[i] = byte(i)
	}
	p, err := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString(key32),
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, string(ProviderTypeLocal), p.Name())
}

func TestLocalProvider_GenerateDataKey_Decrypt_Validate_GetKeyInfo(t *testing.T) {
	ctx := context.Background()
	key32 := make([]byte, 32)
	for i := range key32 {
		key32[i] = byte(i + 1)
	}
	p, err := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString(key32),
	})
	require.NoError(t, err)

	// GenerateDataKey
	dk, err := p.GenerateDataKey(ctx, "key-1", "AES_256")
	require.NoError(t, err)
	require.NotNil(t, dk)
	assert.Len(t, dk.Plaintext, 32)
	assert.NotEmpty(t, dk.CiphertextBlob)
	assert.Equal(t, "key-1", dk.KeyID)
	assert.Equal(t, string(ProviderTypeLocal), dk.Provider)

	// Decrypt
	plain, err := p.Decrypt(ctx, dk.CiphertextBlob)
	require.NoError(t, err)
	assert.Equal(t, dk.Plaintext, plain)

	// ValidateKey
	err = p.ValidateKey(ctx, "key-1")
	assert.NoError(t, err)

	// GetKeyInfo
	info, err := p.GetKeyInfo(ctx, "key-1")
	require.NoError(t, err)
	assert.True(t, info.Enabled)
	assert.True(t, info.SupportsEncrypt)
	assert.True(t, info.SupportsDecrypt)
	assert.Contains(t, info.Description, "256 bits")
}

func TestLocalProvider_GenerateDataKey_AES128(t *testing.T) {
	ctx := context.Background()
	key16 := make([]byte, 16)
	for i := range key16 {
		key16[i] = byte(i)
	}
	p, err := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString(key16),
	})
	require.NoError(t, err)

	dk, err := p.GenerateDataKey(ctx, "aes128", "AES_128")
	require.NoError(t, err)
	assert.Len(t, dk.Plaintext, 16)
}

func TestLocalProvider_Decrypt_CiphertextTooShort(t *testing.T) {
	ctx := context.Background()
	key32 := make([]byte, 32)
	p, _ := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString(key32),
	})
	_, err := p.Decrypt(ctx, []byte("short"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

func TestLocalProvider_GenerateDataKey_CacheHit(t *testing.T) {
	ctx := context.Background()
	key32 := make([]byte, 32)
	p, _ := NewLocalProvider(ctx, map[string]interface{}{
		"master_key": hex.EncodeToString(key32),
	})

	dk1, err := p.GenerateDataKey(ctx, "cached-key", "AES_256")
	require.NoError(t, err)
	dk2, err := p.GenerateDataKey(ctx, "cached-key", "AES_256")
	require.NoError(t, err)
	// Cached - same plaintext
	assert.Equal(t, dk1.Plaintext, dk2.Plaintext)
	assert.Equal(t, dk1.CiphertextBlob, dk2.CiphertextBlob)
}
