package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
)

// RetryableFunc represents a function that can be retried
type RetryableFunc func() error

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, attempts int, operation string, fn RetryableFunc) error {
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		// Execute one retry attempt
		err := executeRetryAttempt(ctx, fn, attempt)
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Handle the error and decide whether to continue
		shouldContinue, retErr := handleRetryError(err, operation, attempt, attempts)
		if retErr != nil {
			return retErr
		}
		if !shouldContinue {
			break
		}

		// Wait before next attempt
		if waitErr := waitForRetry(ctx, attempt, operation, attempts, err); waitErr != nil {
			return waitErr
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, attempts, lastErr)
}

// executeRetryAttempt executes a single retry attempt
func executeRetryAttempt(ctx context.Context, fn RetryableFunc, attempt int) error {
	// Check context before attempting
	if ctx.Err() != nil {
		return fmt.Errorf("context canceled before attempt %d: %w", attempt, ctx.Err())
	}
	return fn()
}

// handleRetryError processes an error and determines if retry should continue
func handleRetryError(err error, operation string, attempt, maxAttempts int) (shouldContinue bool, retErr error) {
	// Check if error is retryable
	if !isRetryableError(err) {
		return false, fmt.Errorf("%s failed (non-retryable): %w", operation, err)
	}

	// Don't retry on the last attempt
	if attempt == maxAttempts {
		return false, nil
	}

	return true, nil
}

// waitForRetry waits for the backoff period before the next retry attempt
func waitForRetry(ctx context.Context, attempt int, operation string, maxAttempts int, err error) error {
	backoff := calculateBackoff(attempt)

	fmt.Printf("    %s failed (attempt %d/%d), retrying in %v: %v\n",
		operation, attempt, maxAttempts, backoff, err)

	// Wait with context cancellation support
	select {
	case <-time.After(backoff):
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
	}
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check basic MongoDB error types
	if isBasicRetryableError(err) {
		return true
	}

	// Check command errors
	if cmdErr, ok := err.(mongo.CommandError); ok {
		return isRetryableCommandError(&cmdErr)
	}

	// Check bulk write exceptions
	if bulkErr, ok := err.(mongo.BulkWriteException); ok {
		return isRetryableBulkWriteError(bulkErr)
	}

	return false
}

// isBasicRetryableError checks for basic retryable MongoDB errors
func isBasicRetryableError(err error) bool {
	// Check for MongoDB driver errors
	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) {
		return true
	}

	// Check error message for common network issues
	errMsg := err.Error()
	networkErrors := []string{
		"i/o timeout",
		"connection reset by peer",
		"broken pipe",
		"incomplete read of message header",
		"EOF",
		"use of closed network connection",
		"no reachable servers",
		"connection refused",
	}

	for _, pattern := range networkErrors {
		if contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					len(substr) < len(s) && findSubstring(s, substr)))
}

// findSubstring finds a substring in a string
func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// isRetryableCommandError checks if a command error is retryable
func isRetryableCommandError(cmdErr *mongo.CommandError) bool {
	switch cmdErr.Code {
	case 112: // WriteConflict
		return true
	case 11000, 11001: // DuplicateKey errors
		return false
	default:
		return isTransientError(cmdErr.Code)
	}
}

// isRetryableBulkWriteError checks if a bulk write error is retryable
func isRetryableBulkWriteError(bulkErr mongo.BulkWriteException) bool {
	if len(bulkErr.WriteErrors) == 0 {
		return false
	}

	// All errors must be transient for the batch to be retryable
	for _, writeErr := range bulkErr.WriteErrors {
		if !isTransientError(int32(writeErr.Code)) {
			return false
		}
	}
	return true
}

// isTransientError checks if a MongoDB error code represents a transient error
func isTransientError(code int32) bool {
	switch code {
	case 6, 7, 89, 91, 189, 262, 9001, 10107, 13435, 13436, 16500, 50:
		// Various transient errors including:
		// 6, 7: host unreachable/not found
		// 89: network timeout
		// 91: shutdown in progress
		// 189: primary stepped down
		// 262: ExceededTimeLimit
		// Others: various transient conditions
		return true
	default:
		return false
	}
}

// calculateBackoff calculates the backoff duration for a given attempt
func calculateBackoff(attempt int) time.Duration {
	// Base backoff: 100ms * 2^(attempt-1)
	// Max backoff: 30 seconds
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 30 * time.Second

	backoff := baseBackoff * time.Duration(1<<uint(attempt-1))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}

	// Add jitter (up to 25% of backoff time)
	jitter := time.Duration(float64(backoff) * 0.25 * (0.5 + 0.5*float64(time.Now().UnixNano()%1000)/1000.0))

	return backoff + jitter
}
