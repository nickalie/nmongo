package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

// TestIncrementalCopyWithLastModifiedField tests incremental copy using the lastModifiedField
func TestIncrementalCopyWithLastModifiedField(t *testing.T) {
	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start MongoDB containers
	sourceURI, sourceContainer, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start source MongoDB container")
	defer sourceContainer.Terminate(ctx)

	targetURI, targetContainer, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start target MongoDB container")
	defer targetContainer.Terminate(ctx)

	t.Logf("Source MongoDB URI: %s", sourceURI)
	t.Logf("Target MongoDB URI: %s", targetURI)

	// Connect to MongoDB instances
	sourceClient, err := NewClient(ctx, sourceURI, "")
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceClient.Disconnect(ctx)

	targetClient, err := NewClient(ctx, targetURI, "")
	require.NoError(t, err, "Failed to connect to target MongoDB")
	defer targetClient.Disconnect(ctx)

	// Set up test data
	dbName := "test_lastmod_db"
	collName := "test_lastmod_collection"
	lastModField := "updatedAt"

	// Clear existing data
	sourceClient.GetDatabase(dbName).Collection(collName).Drop(ctx)
	targetClient.GetDatabase(dbName).Collection(collName).Drop(ctx)

	// Insert test documents with lastModifiedField
	sourceColl := sourceClient.GetDatabase(dbName).Collection(collName)

	// First batch - older timestamps
	pastTime := time.Now().Add(-24 * time.Hour)
	docs := []interface{}{
		bson.M{"_id": 1, "name": "Doc 1", "value": 100, lastModField: pastTime},
		bson.M{"_id": 2, "name": "Doc 2", "value": 200, lastModField: pastTime},
		bson.M{"_id": 3, "name": "Doc 3", "value": 300, lastModField: pastTime},
	}
	_, err = sourceColl.InsertMany(ctx, docs)
	require.NoError(t, err, "Failed to insert initial test documents")

	// First copy - should copy all documents
	sourceDB := sourceClient.GetDatabase(dbName)
	targetDB := targetClient.GetDatabase(dbName)

	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10, lastModField, 5)
	require.NoError(t, err, "Failed to copy collection")

	// Verify initial documents were copied
	targetColl := targetClient.GetDatabase(dbName).Collection(collName)
	count, err := targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents")
	assert.Equal(t, int64(3), count, "Expected 3 documents to be copied initially")

	// Update one document and add new documents with current timestamp
	currentTime := time.Now()

	// Update an existing document
	_, err = sourceColl.UpdateOne(
		ctx,
		bson.M{"_id": 2},
		bson.M{"$set": bson.M{"value": 250, lastModField: currentTime}},
	)
	require.NoError(t, err, "Failed to update document")

	// Insert new documents
	newDocs := []interface{}{
		bson.M{"_id": 4, "name": "Doc 4", "value": 400, lastModField: currentTime},
		bson.M{"_id": 5, "name": "Doc 5", "value": 500, lastModField: currentTime},
	}
	_, err = sourceColl.InsertMany(ctx, newDocs)
	require.NoError(t, err, "Failed to insert new test documents")

	// Second copy - should only copy new and updated documents
	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10, lastModField, 5)
	require.NoError(t, err, "Failed to copy collection incrementally")

	// Verify new documents were added
	count, err = targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents")
	assert.Equal(t, int64(5), count, "Expected 5 documents total after incremental copy")

	// Verify the updated document has the new value
	var updatedDoc bson.M
	err = targetColl.FindOne(ctx, bson.M{"_id": 2}).Decode(&updatedDoc)
	require.NoError(t, err, "Failed to find updated document")
	assert.Equal(t, int32(250), updatedDoc["value"], "Expected document to be updated")

	t.Logf("Successfully tested incremental copy with lastModifiedField")
}
