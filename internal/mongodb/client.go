// Package mongodb provides MongoDB client functionality for the nmongo application.
// It includes utilities for connecting to MongoDB clusters and copying data between them.
package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Client represents a MongoDB client wrapper with utilities for copying data
type Client struct {
	client *mongo.Client
	uri    string
}

// NewClient creates a new MongoDB client wrapper
func NewClient(ctx context.Context, uri string) (*Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the MongoDB server to verify the connection
	if err := client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &Client{
		client: client,
		uri:    uri,
	}, nil
}

// Disconnect closes the MongoDB connection
func (c *Client) Disconnect(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// ListDatabases returns a list of database names, excluding system databases
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	dbs, err := c.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	// Filter out system databases
	var filteredDbs []string
	for _, db := range dbs {
		if db != "admin" && db != "local" && db != "config" {
			filteredDbs = append(filteredDbs, db)
		}
	}

	return filteredDbs, nil
}

// ListCollections returns a list of collection names for a database, excluding system collections
func (c *Client) ListCollections(ctx context.Context, dbName string) ([]string, error) {
	db := c.client.Database(dbName)
	colls, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections for database %s: %w", dbName, err)
	}

	// Filter out system collections
	var filteredColls []string
	for _, coll := range colls {
		if !isSystemCollection(coll) {
			filteredColls = append(filteredColls, coll)
		}
	}

	return filteredColls, nil
}

// isSystemCollection returns true if the collection is a system collection
func isSystemCollection(collName string) bool {
	return collName == "system.profile" || collName == "system.views" || collName == "system.indexes"
}

// GetDatabase returns a mongo.Database for the given database name
func (c *Client) GetDatabase(dbName string) *mongo.Database {
	return c.client.Database(dbName)
}

// CopyCollection copies documents from source to target collection
func CopyCollection(
	ctx context.Context,
	sourceDB, targetDB *mongo.Database,
	collName string,
	incremental bool,
	batchSize int,
) error {
	fmt.Printf("  Copying collection: %s\n", collName)

	// Get source and target collections
	sourceColl := sourceDB.Collection(collName)
	targetColl := targetDB.Collection(collName)

	// Prepare filter for query
	filter, err := prepareFilter(ctx, sourceDB, collName, incremental)
	if err != nil {
		return err
	}

	// Create a cursor for the source collection
	cursor, err := createCursor(ctx, sourceColl, filter, batchSize)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Process documents in batches
	return processBatches(ctx, cursor, targetColl, collName, incremental, batchSize)
}

// prepareFilter creates the appropriate query filter based on the incremental flag
func prepareFilter(ctx context.Context, sourceDB *mongo.Database, collName string, incremental bool) (bson.M, error) {
	filter := bson.M{}

	if !incremental {
		return filter, nil
	}

	fmt.Printf("  Using incremental mode for collection: %s\n", collName)

	// Create an incremental copy helper
	helper := NewIncrementalCopyHelper(sourceDB.Client())

	// Get the incremental filter
	dbName := sourceDB.Name()
	var err error
	filter, err = helper.PrepareIncrementalFilter(ctx, dbName, collName)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare incremental filter: %w", err)
	}

	// Defer updating the last sync time
	defer func() {
		if err := helper.UpdateLastSyncTime(ctx, dbName, collName); err != nil {
			fmt.Printf("  Warning: Failed to update last sync time: %v\n", err)
		}
	}()

	return filter, nil
}

// createCursor creates a cursor for the source collection
func createCursor(ctx context.Context, sourceColl *mongo.Collection, filter bson.M, batchSize int) (*mongo.Cursor, error) {
	findOptions := options.Find().SetBatchSize(int32(batchSize))
	cursor, err := sourceColl.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to query source collection: %w", err)
	}
	return cursor, nil
}

// processBatches processes the documents in batches
func processBatches(
	ctx context.Context,
	cursor *mongo.Cursor,
	targetColl *mongo.Collection,
	collName string,
	incremental bool,
	batchSize int,
) error {
	var batch []interface{}
	var docCount int

	// Read and process documents
	if err := readAndProcessDocuments(ctx, cursor, targetColl, collName, incremental, batchSize, &batch, &docCount); err != nil {
		return err
	}

	// Insert any remaining documents
	if err := handleRemainingDocuments(ctx, targetColl, collName, incremental, batch, &docCount); err != nil {
		return err
	}

	// Check for cursor errors
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("cursor error: %w", err)
	}

	fmt.Printf("  Completed copying collection: %s (%d documents)\n", collName, docCount)
	return nil
}

// readAndProcessDocuments iterates through the cursor and processes documents in batches
func readAndProcessDocuments(
	ctx context.Context,
	cursor *mongo.Cursor,
	targetColl *mongo.Collection,
	collName string,
	incremental bool,
	batchSize int,
	batch *[]interface{},
	docCount *int,
) error {
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return fmt.Errorf("failed to decode document: %w", err)
		}

		*batch = append(*batch, doc)

		// If batch is full, insert the batch
		if len(*batch) >= batchSize {
			if err := insertBatch(ctx, targetColl, *batch, incremental); err != nil {
				return err
			}

			*docCount += len(*batch)
			fmt.Printf("    Copied %d documents to %s (total: %d)\n", len(*batch), collName, *docCount)
			*batch = (*batch)[:0] // Clear the batch
		}
	}
	return nil
}

// handleRemainingDocuments inserts any remaining documents in the batch
func handleRemainingDocuments(
	ctx context.Context,
	targetColl *mongo.Collection,
	collName string,
	incremental bool,
	batch []interface{},
	docCount *int,
) error {
	if len(batch) > 0 {
		if err := insertBatch(ctx, targetColl, batch, incremental); err != nil {
			return err
		}

		*docCount += len(batch)
		fmt.Printf("    Copied %d documents to %s (total: %d)\n", len(batch), collName, *docCount)
	}
	return nil
}

// insertBatch inserts a batch of documents into the target collection
func insertBatch(ctx context.Context, targetColl *mongo.Collection, batch []interface{}, incremental bool) error {
	opts := options.InsertMany().SetOrdered(false)
	_, err := targetColl.InsertMany(ctx, batch, opts)

	// Handle duplicate key errors for incremental copy
	if err != nil && mongo.IsDuplicateKeyError(err) && incremental {
		// In incremental mode, duplicate key errors are expected and can be ignored
		// For a more sophisticated approach, we could update the existing documents
		// instead of inserting new ones
		fmt.Printf("    Some documents already exist (expected in incremental mode)\n")
		return nil
	}

	return err
}
