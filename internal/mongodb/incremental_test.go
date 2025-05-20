package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

// TestIncrementalCopy tests incremental copy functionality using testcontainers
func TestIncrementalCopy(t *testing.T) {
	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start source MongoDB container
	sourceURI, sourceContainer, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start source MongoDB container")
	defer sourceContainer.Terminate(ctx)
	// Start target MongoDB container
	targetURI, targetContainer, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start target MongoDB container")
	defer targetContainer.Terminate(ctx)
	t.Logf("Source MongoDB URI: %s", sourceURI)
	t.Logf("Target MongoDB URI: %s", targetURI)

	// Connect to source MongoDB
	sourceClient, err := NewClient(ctx, sourceURI)
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceClient.Disconnect(ctx)
	// Connect to target MongoDB
	targetClient, err := NewClient(ctx, targetURI)
	require.NoError(t, err, "Failed to connect to target MongoDB")
	defer targetClient.Disconnect(ctx)

	// Set up test data
	dbName := "test_db"
	collName := "test_collection"
	// Clear existing data
	sourceClient.GetDatabase(dbName).Collection(collName).Drop(ctx)
	targetClient.GetDatabase(dbName).Collection(collName).Drop(ctx)

	// Insert test documents
	sourceColl := sourceClient.GetDatabase(dbName).Collection(collName)
	docs := []interface{}{
		bson.M{"_id": 1, "name": "Document 1", "value": 100},
		bson.M{"_id": 2, "name": "Document 2", "value": 200},
		bson.M{"_id": 3, "name": "Document 3", "value": 300},
	}
	_, err = sourceColl.InsertMany(ctx, docs)
	require.NoError(t, err, "Failed to insert test documents")
	// Test copying
	sourceDB := sourceClient.GetDatabase(dbName)
	targetDB := targetClient.GetDatabase(dbName)
	// First copy - should copy all documents
	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10)
	require.NoError(t, err, "Failed to copy collection")
	// Verify documents were copied
	targetColl := targetClient.GetDatabase(dbName).Collection(collName)
	count, err := targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents")
	assert.Equal(t, int64(3), count, "Expected 3 documents to be copied initially")

	// Insert more documents to the source
	newDocs := []interface{}{
		bson.M{"_id": 4, "name": "Document 4", "value": 400},
		bson.M{"_id": 5, "name": "Document 5", "value": 500},
	}
	_, err = sourceColl.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new test documents")
	// Second copy - should only copy the new documents
	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10)
	require.NoError(t, err, "Failed to copy collection incrementally")
	// Verify all documents were copied
	count, err = targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents")
	assert.Equal(t, int64(5), count, "Expected 5 documents after incremental copy")

	t.Logf("Successfully tested incremental copy functionality")
}
