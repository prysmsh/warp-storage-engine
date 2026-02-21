package kms

import (
	"testing"

	"github.com/einyx/foundation-storage-engine/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigFromAppConfig_Nil(t *testing.T) {
	cfg, err := ConfigFromAppConfig(nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.False(t, cfg.Enabled)
}

func TestConfigFromAppConfig_KeySpec(t *testing.T) {
	// AES_256 / default
	cfg, err := ConfigFromAppConfig(&config.KMSKeyConfig{
		Enabled: true, KeySpec: "AES_256", DefaultKeyID: "key-1", Region: "us-east-1",
	})
	require.NoError(t, err)
	require.True(t, cfg.Enabled)

	// AES_128
	cfg2, err := ConfigFromAppConfig(&config.KMSKeyConfig{
		Enabled: true, KeySpec: "AES_128", DefaultKeyID: "key-2", Region: "us-east-1",
	})
	require.NoError(t, err)
	require.True(t, cfg2.Enabled)

	// Invalid TTL uses default
	cfg3, err := ConfigFromAppConfig(&config.KMSKeyConfig{
		Enabled: true, DataKeyCacheTTL: "invalid", DefaultKeyID: "k", Region: "us-east-1",
	})
	require.NoError(t, err)
	require.NotNil(t, cfg3.DataKeyCacheTTL)
}

func TestBucketConfigFromAppConfig(t *testing.T) {
	assert.Nil(t, BucketConfigFromAppConfig(nil))

	bc := &config.BucketConfig{KMSKeyID: ""}
	assert.Nil(t, BucketConfigFromAppConfig(bc))

	bc2 := &config.BucketConfig{
		KMSKeyID:             "arn:key",
		KMSEncryptionContext: map[string]string{"a": "b"},
	}
	out := BucketConfigFromAppConfig(bc2)
	require.NotNil(t, out)
	assert.Equal(t, "arn:key", out.KeyID)
	assert.Equal(t, map[string]string{"a": "b"}, out.EncryptionContext)
	assert.True(t, out.OverrideDefault)
}
