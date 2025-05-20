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
	lastModifiedField string,
) error {
	fmt.Printf("  Copying collection: %s\n", collName)

	// Get source and target collections
	sourceColl := sourceDB.Collection(collName)
	targetColl := targetDB.Collection(collName)

	// Prepare filter for query
	filter, err := prepareFilter(ctx, sourceDB, collName, incremental, lastModifiedField)
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
	if err := processBatches(ctx, cursor, targetColl, collName, incremental, batchSize); err != nil {
		return err
	}

	// Copy indexes from source to target collection
	if err := CopyCollectionIndexes(ctx, sourceDB, targetDB, collName); err != nil {
		fmt.Printf("  Warning: Failed to copy indexes for collection %s: %v\n", collName, err)
		// Continue even if indexes copy fails - at least the data was copied
	}

	return nil
}

// prepareFilter creates the appropriate query filter based on the incremental flag
func prepareFilter(
	ctx context.Context,
	sourceDB *mongo.Database,
	collName string,
	incremental bool,
	lastModifiedField string,
) (bson.M, error) {
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
	filter, err = helper.PrepareIncrementalFilter(ctx, dbName, collName, lastModifiedField)
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
	if !incremental {
		return insertDocuments(ctx, targetColl, batch)
	}

	// In incremental mode, use upsert operations
	return upsertDocuments(ctx, targetColl, batch)
}

// insertDocuments inserts documents without using upsert
func insertDocuments(ctx context.Context, targetColl *mongo.Collection, batch []interface{}) error {
	opts := options.InsertMany().SetOrdered(false)
	_, err := targetColl.InsertMany(ctx, batch, opts)
	return err
}

// upsertDocuments performs upsert operations for documents that may already exist
func upsertDocuments(ctx context.Context, targetColl *mongo.Collection, batch []interface{}) error {
	bulkOps := prepareBulkOps(batch)

	if len(bulkOps) == 0 {
		return nil
	}

	bulkOptions := options.BulkWrite().SetOrdered(false)
	result, err := targetColl.BulkWrite(ctx, bulkOps, bulkOptions)

	if err != nil {
		return err
	}

	if result.UpsertedCount > 0 || result.ModifiedCount > 0 {
		fmt.Printf("    Upserted: %d, Modified: %d (incremental mode)\n",
			result.UpsertedCount, result.ModifiedCount)
	}

	return nil
}

// prepareBulkOps creates bulk operation models for documents
func prepareBulkOps(batch []interface{}) []mongo.WriteModel {
	bulkOps := make([]mongo.WriteModel, 0, len(batch))

	for _, doc := range batch {
		docMap, ok := doc.(bson.M)
		if !ok {
			continue
		}

		id, hasID := docMap["_id"]
		if !hasID {
			continue
		}

		upsert := true
		updateModel := mongo.NewReplaceOneModel().
			SetFilter(bson.M{"_id": id}).
			SetReplacement(docMap).
			SetUpsert(upsert)

		bulkOps = append(bulkOps, updateModel)
	}

	return bulkOps
}

// ListCollectionIndexes returns all indexes for a collection
func ListCollectionIndexes(ctx context.Context, db *mongo.Database, collName string) ([]bson.M, error) {
	coll := db.Collection(collName)
	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes for collection %s: %w", collName, err)
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, fmt.Errorf("failed to decode indexes for collection %s: %w", collName, err)
	}

	return indexes, nil
}

// CopyCollectionIndexes copies all indexes from source collection to target collection
func CopyCollectionIndexes(ctx context.Context, sourceDB, targetDB *mongo.Database, collName string) error {
	fmt.Printf("  Copying indexes for collection: %s\n", collName)

	// Get all indexes from source collection
	indexes, err := ListCollectionIndexes(ctx, sourceDB, collName)
	if err != nil {
		return err
	}

	if len(indexes) <= 1 {
		// Only _id index exists, nothing to copy
		fmt.Printf("  No custom indexes found for collection: %s\n", collName)
		return nil
	}

	// Create indexes on target collection
	targetColl := targetDB.Collection(collName)
	indexCount := 0

	for _, indexDoc := range indexes {
		// Skip the _id index which is created automatically
		if isIDIndex(indexDoc) {
			continue
		}

		// Convert the index document to createIndexes command format
		indexModel, err := convertToIndexModel(indexDoc)
		if err != nil {
			fmt.Printf("    Warning: Failed to convert index %v: %v. Skipping.\n", indexDoc, err)
			continue
		}

		// Create the index
		indexName, err := targetColl.Indexes().CreateOne(ctx, indexModel)
		if err != nil {
			fmt.Printf("    Warning: Failed to create index %v: %v. Skipping.\n", indexModel, err)
			continue
		}

		indexCount++
		fmt.Printf("    Created index %s\n", indexName)
	}

	fmt.Printf("  Copied %d indexes for collection: %s\n", indexCount, collName)
	return nil
}

// isIDIndex checks if the index is the default _id index
func isIDIndex(indexDoc bson.M) bool {
	name, ok := indexDoc["name"].(string)
	if !ok {
		return false
	}
	return name == "_id_"
}

// convertToIndexModel converts a MongoDB index document to an IndexModel
// Refactored to reduce cyclomatic complexity
func convertToIndexModel(indexDoc bson.M) (mongo.IndexModel, error) {
	// Extract key fields
	keyDoc, ok := indexDoc["key"].(bson.M)
	if !ok {
		return mongo.IndexModel{}, fmt.Errorf("index does not have a valid key field")
	}

	// Convert keys to proper format
	keys := extractIndexKeys(keyDoc)

	// Set up index options
	indexOptions := buildIndexOptions(indexDoc)

	// Create the IndexModel
	model := mongo.IndexModel{
		Keys:    keys,
		Options: indexOptions,
	}

	return model, nil
}

// extractIndexKeys converts MongoDB key document to index keys format
func extractIndexKeys(keyDoc bson.M) bson.D {
	keys := bson.D{}

	for k, v := range keyDoc {
		// Convert value to int for index direction (1 or -1)
		switch val := v.(type) {
		case int32:
			keys = append(keys, bson.E{Key: k, Value: val})
		case float64:
			// Convert from double/float (may happen in some MongoDB versions)
			keys = append(keys, bson.E{Key: k, Value: int32(val)})
		default:
			// For text or other special indexes, keep the original value
			keys = append(keys, bson.E{Key: k, Value: v})
		}
	}

	return keys
}

// buildIndexOptions creates index options from the index document
func buildIndexOptions(indexDoc bson.M) *options.IndexOptions {
	opts := options.IndexOptions{}

	// Set name if it exists
	if name, ok := indexDoc["name"].(string); ok {
		opts.SetName(name)
	}

	// Set unique flag
	if unique, ok := indexDoc["unique"].(bool); ok && unique {
		opts.SetUnique(true)
	}

	// Set sparse flag
	if sparse, ok := indexDoc["sparse"].(bool); ok && sparse {
		opts.SetSparse(true)
	}

	// Handle TTL indexes (expireAfterSeconds)
	if expireAfterSeconds, ok := indexDoc["expireAfterSeconds"].(int32); ok {
		opts.SetExpireAfterSeconds(expireAfterSeconds)
	}

	// Note: We're removing the background option as it's deprecated in MongoDB 4.2+
	// MongoDB now always builds indexes in the background

	return &opts
}
