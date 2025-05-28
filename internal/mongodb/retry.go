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
		// Check context before attempting
		if ctx.Err() != nil {
			return fmt.Errorf("context canceled before attempt %d: %w", attempt, ctx.Err())
		}

		// Try the operation
		err := fn()
		if err == nil {
			return nil // Success
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryableError(err) {
			return fmt.Errorf("%s failed (non-retryable): %w", operation, err)
		}

		// Don't retry on the last attempt
		if attempt == attempts {
			break
		}

		// Calculate backoff duration (exponential with jitter)
		backoff := calculateBackoff(attempt)

		fmt.Printf("    %s failed (attempt %d/%d), retrying in %v: %v\n",
			operation, attempt, attempts, backoff, err)

		// Wait with context cancellation support
		select {
		case <-time.After(backoff):
			// Continue to next attempt
		case <-ctx.Done():
			return fmt.Errorf("context canceled during retry backoff: %w", ctx.Err())
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, attempts, lastErr)
}

// isRetryableError determines if an error should trigger a retry
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific MongoDB errors that are retryable
	if mongo.IsTimeout(err) {
		return true
	}

	if mongo.IsNetworkError(err) {
		return true
	}

	// Check for write conflict errors (commonly retryable)
	if cmdErr, ok := err.(mongo.CommandError); ok {
		switch cmdErr.Code {
		case 112: // WriteConflict
			return true
		case 11000, 11001: // DuplicateKey errors in batch operations might be retryable
			return false // Actually, duplicate key errors should not be retried
		default:
			// For other command errors, check if they're transient
			return isTransientError(cmdErr.Code)
		}
	}

	// Check for bulk write exceptions
	if bulkErr, ok := err.(mongo.BulkWriteException); ok {
		// If there are write errors, check if any are retryable
		for _, writeErr := range bulkErr.WriteErrors {
			if !isTransientError(int32(writeErr.Code)) {
				return false // If any error is not retryable, don't retry the batch
			}
		}
		return len(bulkErr.WriteErrors) > 0 // Retry if all errors were transient
	}

	return false
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
