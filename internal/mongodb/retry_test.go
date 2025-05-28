package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestRetryWithBackoff(t *testing.T) {
	tests := []struct {
		name           string
		attempts       int
		operation      string
		setupFn        func() func() error // Returns the function to be retried
		expectError    bool
		expectAttempts int
	}{
		{
			name:      "Success on first attempt",
			attempts:  3,
			operation: "test operation",
			setupFn: func() func() error {
				attemptCount := 0
				return func() error {
					attemptCount++
					if attemptCount == 1 {
						return nil
					}
					return errors.New("should not reach here")
				}
			},
			expectError:    false,
			expectAttempts: 1,
		},
		{
			name:      "Success after retry",
			attempts:  3,
			operation: "test operation",
			setupFn: func() func() error {
				attemptCount := 0
				return func() error {
					attemptCount++
					if attemptCount < 3 {
						return mongo.CommandError{Code: 189} // Primary stepped down (retryable)
					}
					return nil
				}
			},
			expectError:    false,
			expectAttempts: 3,
		},
		{
			name:      "Failure after max attempts",
			attempts:  3,
			operation: "test operation",
			setupFn: func() func() error {
				attemptCount := 0
				return func() error {
					attemptCount++
					return mongo.CommandError{Code: 189} // Always fail with retryable error
				}
			},
			expectError:    true,
			expectAttempts: 3,
		},
		{
			name:      "Non-retryable error",
			attempts:  3,
			operation: "test operation",
			setupFn: func() func() error {
				attemptCount := 0
				return func() error {
					attemptCount++
					return mongo.CommandError{Code: 11000} // Duplicate key (non-retryable)
				}
			},
			expectError:    true,
			expectAttempts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			attemptCount := 0

			fn := tt.setupFn()
			wrappedFn := func() error {
				attemptCount++
				return fn()
			}

			err := RetryWithBackoff(ctx, tt.attempts, tt.operation, wrappedFn)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.expectAttempts, attemptCount, "Unexpected number of attempts")
		})
	}
}

func TestRetryWithBackoffContextCancellation(t *testing.T) {
	t.Run("Context canceled before first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		attemptCount := 0
		err := RetryWithBackoff(ctx, 3, "test operation", func() error {
			attemptCount++
			return nil
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
		assert.Equal(t, 0, attemptCount, "Should not attempt when context is already canceled")
	})

	t.Run("Context canceled during retry", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		attemptCount := 0
		err := RetryWithBackoff(ctx, 3, "test operation", func() error {
			attemptCount++
			if attemptCount == 1 {
				cancel() // Cancel after first attempt
			}
			return mongo.CommandError{Code: 189} // Retryable error
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
		assert.Equal(t, 1, attemptCount, "Should have attempted once before cancellation")
	})
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "Nil error",
			err:       nil,
			retryable: false,
		},
		{
			name:      "Timeout error",
			err:       context.DeadlineExceeded,
			retryable: true,
		},
		{
			name:      "WriteConflict error",
			err:       mongo.CommandError{Code: 112},
			retryable: true,
		},
		{
			name:      "DuplicateKey error",
			err:       mongo.CommandError{Code: 11000},
			retryable: false,
		},
		{
			name:      "Host unreachable",
			err:       mongo.CommandError{Code: 6},
			retryable: true,
		},
		{
			name:      "Primary stepped down",
			err:       mongo.CommandError{Code: 189},
			retryable: true,
		},
		{
			name:      "Unknown command error",
			err:       mongo.CommandError{Code: 99999},
			retryable: false,
		},
		{
			name:      "Generic error",
			err:       errors.New("some error"),
			retryable: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			assert.Equal(t, tt.retryable, result)
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		minDuration time.Duration
		maxDuration time.Duration
	}{
		{
			name:        "First attempt",
			attempt:     1,
			minDuration: 100 * time.Millisecond,
			maxDuration: 225 * time.Millisecond, // 100ms + up to 25% jitter
		},
		{
			name:        "Second attempt",
			attempt:     2,
			minDuration: 200 * time.Millisecond,
			maxDuration: 350 * time.Millisecond, // 200ms + up to 25% jitter
		},
		{
			name:        "Third attempt",
			attempt:     3,
			minDuration: 400 * time.Millisecond,
			maxDuration: 700 * time.Millisecond, // 400ms + up to 25% jitter
		},
		{
			name:        "Large attempt (should cap at max)",
			attempt:     10,
			minDuration: 30 * time.Second,
			maxDuration: 37500 * time.Millisecond, // 30s + up to 25% jitter
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to account for jitter
			for i := 0; i < 10; i++ {
				backoff := calculateBackoff(tt.attempt)
				assert.GreaterOrEqual(t, backoff, tt.minDuration,
					"Backoff should be at least %v", tt.minDuration)
				assert.LessOrEqual(t, backoff, tt.maxDuration,
					"Backoff should be at most %v", tt.maxDuration)
			}
		})
	}
}

func TestBulkWriteExceptionRetry(t *testing.T) {
	t.Run("All errors retryable", func(t *testing.T) {
		err := mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{
				{WriteError: mongo.WriteError{Code: 189}}, // Primary stepped down
				{WriteError: mongo.WriteError{Code: 91}},  // Shutdown in progress
			},
		}
		assert.True(t, isRetryableError(err))
	})

	t.Run("Mixed errors not retryable", func(t *testing.T) {
		err := mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{
				{WriteError: mongo.WriteError{Code: 189}},   // Primary stepped down (retryable)
				{WriteError: mongo.WriteError{Code: 11000}}, // Duplicate key (not retryable)
			},
		}
		assert.False(t, isRetryableError(err))
	})

	t.Run("Empty write errors", func(t *testing.T) {
		err := mongo.BulkWriteException{
			WriteErrors: []mongo.BulkWriteError{},
		}
		assert.False(t, isRetryableError(err))
	})
}

// TestRetryIntegration tests retry behavior with actual MongoDB operations
func TestRetryIntegration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start MongoDB container
	uri, container, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start MongoDB container")
	defer container.Terminate(ctx)

	// Connect to MongoDB
	client, err := NewClient(ctx, uri, "")
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	db := client.GetDatabase("test_retry_db")
	coll := db.Collection("test_retry_coll")

	t.Run("Successful operation with retry", func(t *testing.T) {
		docs := []interface{}{
			map[string]interface{}{"_id": 1, "value": "test1"},
			map[string]interface{}{"_id": 2, "value": "test2"},
		}

		err := insertDocuments(ctx, coll, docs, 3)
		require.NoError(t, err)
	})

	t.Run("Duplicate key error not retried", func(t *testing.T) {
		// Insert a document
		docs := []interface{}{
			map[string]interface{}{"_id": 3, "value": "test3"},
		}
		err := insertDocuments(ctx, coll, docs, 3)
		require.NoError(t, err)

		// Try to insert the same document again
		err = insertDocuments(ctx, coll, docs, 3)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-retryable")
	})
}

// Helper function to simulate network errors
func simulateNetworkError() error {
	return fmt.Errorf("connection refused")
}
