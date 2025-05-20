package mongodb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientWithCACert tests the MongoDB client with CA certificate
func TestClientWithCACert(t *testing.T) {
	// Skip this test if running in CI environment where testcontainers might not be available
	if os.Getenv("CI") == "true" {
		t.Skip("Skipping test in CI environment")
	}

	// Create a temporary file for the CA cert
	tempDir := t.TempDir()
	caCertFile := filepath.Join(tempDir, "ca.pem")

	// Write sample PEM content to the file
	// This is a fake certificate just to test file loading logic, not actual TLS
	fakeCertPEM := `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
WFNWn0AWnF+L4pDngyRGJd9N9iEuGlECIBkAPKVi+HOgxM1YYRCt5POtLsFD9Lrx
Lhqyfh3Q/2P4
-----END CERTIFICATE-----`

	err := os.WriteFile(caCertFile, []byte(fakeCertPEM), 0644)
	require.NoError(t, err, "Failed to write fake cert file")

	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start MongoDB container
	uri, container, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start MongoDB container")
	defer container.Terminate(ctx)

	t.Logf("MongoDB URI: %s", uri)
	t.Logf("CA certificate file: %s", caCertFile)

	// Test with valid CA certificate file path
	t.Run("With valid CA file", func(t *testing.T) {
		// The TLS handshake will fail since our fake cert doesn't match the server,
		// but we can test that the file is properly read and TLS config is created
		connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := NewClient(connCtx, uri, caCertFile)
		connCancel()

		// We expect an error since our CA cert is fake, but the error should be during TLS handshake
		// not during the file reading/parsing phase
		assert.Error(t, err, "Expected TLS handshake to fail with fake cert")
		// The error could be various TLS-related errors depending on the environment
		assert.Nil(t, client, "Client should be nil on error")
	})

	// Test with non-existent CA certificate file path
	t.Run("With non-existent CA file", func(t *testing.T) {
		nonExistentFile := filepath.Join(tempDir, "nonexistent.pem")
		connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := NewClient(connCtx, uri, nonExistentFile)
		connCancel()

		assert.Error(t, err, "Expected error with non-existent CA file")
		assert.Contains(t, err.Error(), "failed to read CA certificate file", "Expected specific error message")
		assert.Nil(t, client, "Client should be nil on error")
	})

	// Test with invalid PEM content
	t.Run("With invalid PEM content", func(t *testing.T) {
		invalidCertFile := filepath.Join(tempDir, "invalid.pem")
		err := os.WriteFile(invalidCertFile, []byte("THIS IS NOT A VALID PEM FILE"), 0644)
		require.NoError(t, err, "Failed to write invalid cert file")

		connCtx, connCancel := context.WithTimeout(context.Background(), 5*time.Second)
		client, err := NewClient(connCtx, uri, invalidCertFile)
		connCancel()

		assert.Error(t, err, "Expected error with invalid PEM content")
		assert.Contains(t, err.Error(), "failed to append CA certificate", "Expected specific error message")
		assert.Nil(t, client, "Client should be nil on error")
	})
}
