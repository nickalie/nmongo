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
	client        *mongo.Client
	syncStateDB   string
	syncStateColl string
}

// NewIncrementalCopyHelper creates a new incremental copy helper
func NewIncrementalCopyHelper(client *mongo.Client) *IncrementalCopyHelper {
	return &IncrementalCopyHelper{
		client:        client,
		syncStateDB:   "nmongo_metadata",
		syncStateColl: "sync_state",
	}
}

// GetLastSyncTime gets the last synchronization time for a collection
func (h *IncrementalCopyHelper) GetLastSyncTime(ctx context.Context, dbName, collName string) (time.Time, error) {
	coll := h.client.Database(h.syncStateDB).Collection(h.syncStateColl)

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
		return time.Time{}, err
	}

	return syncState.LastSyncTime, nil
}

// UpdateLastSyncTime updates the last synchronization time for a collection
func (h *IncrementalCopyHelper) UpdateLastSyncTime(ctx context.Context, dbName, collName string) error {
	coll := h.client.Database(h.syncStateDB).Collection(h.syncStateColl)

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
	return err
}

// PrepareIncrementalFilter prepares a filter for incremental copying
// It uses the specified lastModifiedField to filter documents that were modified
// after the last synchronization time
func (h *IncrementalCopyHelper) PrepareIncrementalFilter(ctx context.Context, dbName, collName, lastModifiedField string) (bson.M, error) {
	lastSyncTime, err := h.GetLastSyncTime(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	// If no previous sync, copy everything
	if lastSyncTime.IsZero() {
		return bson.M{}, nil
	}

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
