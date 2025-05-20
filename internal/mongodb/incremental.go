package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// SyncState represents the state of synchronization between MongoDB clusters
type SyncState struct {
	LastSyncTime   time.Time `bson:"lastSyncTime"`
	DatabaseName   string    `bson:"databaseName"`
	CollectionName string    `bson:"collectionName"`
}

// IncrementalCopyHelper helps with incremental copying of MongoDB collections
type IncrementalCopyHelper struct {
	sourceClient  *mongo.Client
	targetClient  *mongo.Client
	syncStateDB   string
	syncStateColl string
	useTarget     bool // Whether to use target database client for sync state
}

// NewIncrementalCopyHelper creates a new incremental copy helper
func NewIncrementalCopyHelper(sourceClient, targetClient *mongo.Client, useTarget bool) *IncrementalCopyHelper {
	return &IncrementalCopyHelper{
		sourceClient:  sourceClient,
		targetClient:  targetClient,
		syncStateDB:   "nmongo_metadata",
		syncStateColl: "sync_state",
		useTarget:     useTarget,
	}
}

// GetLastSyncTime gets the last synchronization time for a collection
func (h *IncrementalCopyHelper) GetLastSyncTime(ctx context.Context, dbName, collName string) (time.Time, error) {
	// Select the appropriate client based on useTarget flag
	client := h.sourceClient
	if h.useTarget {
		client = h.targetClient
	}

	coll := client.Database(h.syncStateDB).Collection(h.syncStateColl)

	filter := bson.M{
		"databaseName":   dbName,
		"collectionName": collName,
	}

	var syncState SyncState
	err := coll.FindOne(ctx, filter).Decode(&syncState)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// No previous sync, return a zero time
			return time.Time{}, nil
		}

		// Log the error
		fmt.Printf("  Warning: Failed to access metadata in %s: %v\n", h.syncStateDB, err)

		// Try the alternate client if we got an error (maybe the collection doesn't exist in the current client)
		if h.useTarget {
			// Already using target client, just return zero time for a full copy
			fmt.Printf("  Will perform a full copy\n")
			return time.Time{}, nil
		} else {
			// Try with target client as fallback
			fmt.Printf("  Will try using target database for metadata\n")

			// Create a temporary helper with useTarget=true
			tempHelper := NewIncrementalCopyHelper(h.sourceClient, h.targetClient, true)
			return tempHelper.GetLastSyncTime(ctx, dbName, collName)
		}
	}

	return syncState.LastSyncTime, nil
}

// UpdateLastSyncTime updates the last synchronization time for a collection
func (h *IncrementalCopyHelper) UpdateLastSyncTime(ctx context.Context, dbName, collName string) error {
	// Select the appropriate client based on useTarget flag
	client := h.sourceClient
	if h.useTarget {
		client = h.targetClient
	}

	coll := client.Database(h.syncStateDB).Collection(h.syncStateColl)

	filter := bson.M{
		"databaseName":   dbName,
		"collectionName": collName,
	}

	update := bson.M{
		"$set": bson.M{
			"lastSyncTime":   time.Now(),
			"databaseName":   dbName,
			"collectionName": collName,
		},
	}

	opts := options.Update().SetUpsert(true)
	_, err := coll.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		// Log the error
		fmt.Printf("  Warning: Failed to update metadata in %s: %v\n", h.syncStateDB, err)

		// Try the alternate client if we got an error
		if h.useTarget {
			// Already using target client, just return the error
			return err
		} else {
			// Try with target client as fallback
			fmt.Printf("  Will try using target database for metadata\n")

			// Create a temporary helper with useTarget=true
			tempHelper := NewIncrementalCopyHelper(h.sourceClient, h.targetClient, true)
			return tempHelper.UpdateLastSyncTime(ctx, dbName, collName)
		}
	}

	return nil
}

// PrepareIncrementalFilter prepares a filter for incremental copying
// It uses the specified lastModifiedField to filter documents that were modified
// after the last synchronization time
func (h *IncrementalCopyHelper) PrepareIncrementalFilter(ctx context.Context, dbName, collName, lastModifiedField string) (bson.M, error) {
	// Get the last sync time using the appropriate client (source or target)
	lastSyncTime, err := h.GetLastSyncTime(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	// If no previous sync, copy everything
	if lastSyncTime.IsZero() {
		fmt.Printf("  No previous sync time found, will copy all documents\n")
		return bson.M{}, nil
	}

	fmt.Printf("  Last sync time: %v\n", lastSyncTime)

	// If lastModifiedField is specified, use it to filter documents
	if lastModifiedField != "" {
		filter := bson.M{lastModifiedField: bson.M{"$gt": lastSyncTime}}
		fmt.Printf("  Using last modified field '%s' for incremental filtering\n", lastModifiedField)
		return filter, nil
	}

	// If no lastModifiedField specified, warn the user
	fmt.Printf("  Note: Proper incremental filtering requires a lastModified field in documents.\n")
	fmt.Printf("  Without it, all documents will be copied and duplicates handled on insert.\n")
	fmt.Printf("  Consider using --last-modified-field to specify the field that tracks changes.\n")

	return bson.M{}, nil
}
