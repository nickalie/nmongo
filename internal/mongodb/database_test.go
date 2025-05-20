package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestDatabaseCopy tests copying an entire database using testcontainers
func TestDatabaseCopy(t *testing.T) {
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
	// Connect directly to MongoDB (not using our client wrapper)
	// This allows us to directly test the copying logic
	sourceClientOpts := options.Client().ApplyURI(sourceURI)
	sourceMongoClient, err := mongo.Connect(ctx, sourceClientOpts)
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceMongoClient.Disconnect(ctx)

	targetClientOpts := options.Client().ApplyURI(targetURI)
	targetMongoClient, err := mongo.Connect(ctx, targetClientOpts)
	require.NoError(t, err, "Failed to connect to target MongoDB")
	defer targetMongoClient.Disconnect(ctx)

	// Create our client wrappers
	sourceClient, err := NewClient(ctx, sourceURI, "")
	require.NoError(t, err, "Failed to connect via client wrapper to source MongoDB")
	defer sourceClient.Disconnect(ctx)

	targetClient, err := NewClient(ctx, targetURI, "")
	require.NoError(t, err, "Failed to connect via client wrapper to target MongoDB")
	defer targetClient.Disconnect(ctx)

	// Set up test data with multiple collections
	dbName := "test_full_db"
	// Drop the database if it exists
	sourceMongoClient.Database(dbName).Drop(ctx)
	targetMongoClient.Database(dbName).Drop(ctx)

	// Create collections with test data
	collections := []string{"coll1", "coll2", "coll3"}
	sourceDB := sourceMongoClient.Database(dbName)

	for i, collName := range collections {
		coll := sourceDB.Collection(collName)

		// Insert different number of documents in each collection
		numDocs := (i + 1) * 5
		var docs []interface{}

		for j := 0; j < numDocs; j++ {
			doc := bson.M{
				"_id":        j + 1,
				"collection": collName,
				"index":      j,
				"value":      j * 10,
			}
			docs = append(docs, doc)
		}

		_, err = coll.InsertMany(ctx, docs)
		require.NoError(t, err, "Failed to insert test documents into %s", collName)

		t.Logf("Inserted %d documents into collection %s", numDocs, collName)
	}
	// Now copy the database using our client
	sourceDBClient := sourceClient.GetDatabase(dbName)
	targetDBClient := targetClient.GetDatabase(dbName)

	// Get the list of collections
	collNames, err := sourceClient.ListCollections(ctx, dbName)
	require.NoError(t, err, "Failed to list collections")
	// Copy each collection
	for _, collName := range collNames {
		err = CopyCollection(ctx, sourceDBClient, targetDBClient, collName, false, 10, "")
		require.NoError(t, err, "Failed to copy collection %s", collName)
	}
	// Verify all collections and documents were copied
	targetDB := targetMongoClient.Database(dbName)

	// Check if all collections exist
	targetCollNames, err := targetClient.ListCollections(ctx, dbName)
	require.NoError(t, err, "Failed to list collections in target")

	assert.Equal(t, len(collections), len(targetCollNames), "Expected %d collections, got %d", len(collections), len(targetCollNames))
	// Check document counts in each collection
	for i, collName := range collections {
		expectedCount := (i + 1) * 5
		count, err := targetDB.Collection(collName).CountDocuments(ctx, bson.M{})
		require.NoError(t, err, "Failed to count documents in %s", collName)

		assert.Equal(t, int64(expectedCount), count, "Expected %d documents in %s, got %d", expectedCount, collName, count)

		t.Logf("Verified %d documents in collection %s", count, collName)
	}

	t.Logf("Successfully tested database copy functionality")
}
