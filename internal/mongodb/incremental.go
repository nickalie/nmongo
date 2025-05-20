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
// This is a placeholder. In a real-world scenario, MongoDB documents would have
// a field like 'lastModified' that would be used for incremental copying
func (h *IncrementalCopyHelper) PrepareIncrementalFilter(ctx context.Context, dbName, collName string) (bson.M, error) {
	lastSyncTime, err := h.GetLastSyncTime(ctx, dbName, collName)
	if err != nil {
		return nil, err
	}

	// If no previous sync, copy everything
	if lastSyncTime.IsZero() {
		return bson.M{}, nil
	}

	// In a real MongoDB schema, you would have a lastModified field that you would query
	// filter = bson.M{"lastModified": bson.M{"$gt": lastSyncTime}}

	// Since we can't assume a lastModified field exists, this is just a placeholder
	// In a real implementation, either:
	// 1. Use a lastModified field in documents
	// 2. Use MongoDB change streams for real-time synchronization
	// 3. Use _id ObjectID timestamps as a heuristic (not accurate for custom _ids)

	// Just return an empty filter for now (will copy all documents)
	// With a note to the user that proper incremental filtering requires a lastModified field
	fmt.Printf("  Note: Proper incremental filtering requires a lastModified field in documents.\n")
	fmt.Printf("  Without it, all documents will be copied and duplicates handled on insert.\n")

	return bson.M{}, nil
}
