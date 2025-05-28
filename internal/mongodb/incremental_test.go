package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	sourceClient, err := NewClient(ctx, sourceURI, "")
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceClient.Disconnect(ctx)
	// Connect to target MongoDB
	targetClient, err := NewClient(ctx, targetURI, "")
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
	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10, "", 5)
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
	err = CopyCollection(ctx, sourceDB, targetDB, collName, true, 10, "", 5)
	require.NoError(t, err, "Failed to copy collection incrementally")
	// Verify all documents were copied
	count, err = targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents")
	assert.Equal(t, int64(5), count, "Expected 5 documents after incremental copy")

	t.Logf("Successfully tested incremental copy functionality")
}

// startMongoContainer is imported from client_test.go in the same package

// TestIncrementalCopyHelperUseTarget tests the useTarget flag in IncrementalCopyHelper
func TestIncrementalCopyHelperUseTarget(t *testing.T) {
	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	sourceClient, err := NewClient(ctx, sourceURI, "")
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceClient.Disconnect(ctx)

	// Connect to target MongoDB
	targetClient, err := NewClient(ctx, targetURI, "")
	require.NoError(t, err, "Failed to connect to target MongoDB")
	defer targetClient.Disconnect(ctx)

	// Set up test data
	dbName := "test_db"
	collName := "test_collection"
	metadataDB := "nmongo_metadata"
	metadataColl := "sync_state"

	// Clear existing data
	sourceClient.GetDatabase(dbName).Collection(collName).Drop(ctx)
	targetClient.GetDatabase(dbName).Collection(collName).Drop(ctx)
	sourceClient.GetDatabase(metadataDB).Collection(metadataColl).Drop(ctx)
	targetClient.GetDatabase(metadataDB).Collection(metadataColl).Drop(ctx)

	// Create test document in target metadata collection
	syncTime := time.Now().Add(-1 * time.Hour) // 1 hour ago
	metadataDoc := bson.M{
		"databaseName":   dbName,
		"collectionName": collName,
		"lastSyncTime":   syncTime,
	}

	// Insert metadata document into target metadata collection
	_, err = targetClient.GetDatabase(metadataDB).Collection(metadataColl).InsertOne(ctx, metadataDoc)
	require.NoError(t, err, "Failed to insert metadata document")

	// Test with useTarget=true
	t.Run("With useTarget=true", func(t *testing.T) {
		// Create helper with useTarget=true
		helper := NewIncrementalCopyHelper(sourceClient.client, targetClient.client, true)

		// Retrieve the sync time from target database
		retrievedTime, err := helper.GetLastSyncTime(ctx, dbName, collName)
		require.NoError(t, err, "Failed to get last sync time")

		// Verify the time matches what we inserted in target
		// Use UTC and format as string to avoid timezone issues
		assert.Equal(t, syncTime.UTC().Format(time.RFC3339), retrievedTime.UTC().Format(time.RFC3339),
			"Last sync time should match what was inserted in target when useTarget=true")

		// Test update sync time on target
		err = helper.UpdateLastSyncTime(ctx, dbName, collName)
		require.NoError(t, err, "Failed to update last sync time")

		// Verify the updated time is in target database
		var updatedDoc bson.M
		err = targetClient.GetDatabase(metadataDB).Collection(metadataColl).FindOne(ctx, bson.M{
			"databaseName":   dbName,
			"collectionName": collName,
		}).Decode(&updatedDoc)
		require.NoError(t, err, "Failed to get updated metadata document")

		// Check that the lastSyncTime was updated and is newer than our original time
		updatedTime := updatedDoc["lastSyncTime"].(primitive.DateTime).Time()
		assert.True(t, updatedTime.After(syncTime), "Last sync time should be updated to a newer time")
	})

	// Test with useTarget=false
	t.Run("With useTarget=false", func(t *testing.T) {
		// Create helper with useTarget=false
		helper := NewIncrementalCopyHelper(sourceClient.client, targetClient.client, false)

		// Add different metadata to source
		sourceSyncTime := time.Now().Add(-2 * time.Hour) // 2 hours ago
		sourceMetadataDoc := bson.M{
			"databaseName":   dbName,
			"collectionName": collName,
			"lastSyncTime":   sourceSyncTime,
		}

		// Insert metadata document into source metadata collection
		_, err = sourceClient.GetDatabase(metadataDB).Collection(metadataColl).InsertOne(ctx, sourceMetadataDoc)
		require.NoError(t, err, "Failed to insert source metadata document")

		// Retrieve the sync time
		retrievedTime, err := helper.GetLastSyncTime(ctx, dbName, collName)
		require.NoError(t, err, "Failed to get last sync time")

		// Verify the time matches what we inserted in source
		// Use UTC and format as string to avoid timezone issues
		assert.Equal(t, sourceSyncTime.UTC().Format(time.RFC3339), retrievedTime.UTC().Format(time.RFC3339),
			"Last sync time should match what was inserted in source when useTarget=false")

		// Test update sync time
		err = helper.UpdateLastSyncTime(ctx, dbName, collName)
		require.NoError(t, err, "Failed to update last sync time")

		// Verify the updated time is in source database
		var updatedDoc bson.M
		err = sourceClient.GetDatabase(metadataDB).Collection(metadataColl).FindOne(ctx, bson.M{
			"databaseName":   dbName,
			"collectionName": collName,
		}).Decode(&updatedDoc)
		require.NoError(t, err, "Failed to get updated source metadata document")

		// Check that the lastSyncTime was updated and is newer than our original time
		updatedTime := updatedDoc["lastSyncTime"].(primitive.DateTime).Time()
		assert.True(t, updatedTime.After(sourceSyncTime), "Last sync time should be updated to a newer time")
	})
}
