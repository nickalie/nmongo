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

	// Start destination MongoDB container
	destURI, destContainer, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start destination MongoDB container")
	defer destContainer.Terminate(ctx)

	t.Logf("Source MongoDB URI: %s", sourceURI)
	t.Logf("Destination MongoDB URI: %s", destURI)

	// Connect directly to MongoDB (not using our client wrapper)
	// This allows us to directly test the copying logic
	sourceClientOpts := options.Client().ApplyURI(sourceURI)
	sourceMongoClient, err := mongo.Connect(ctx, sourceClientOpts)
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceMongoClient.Disconnect(ctx)

	destClientOpts := options.Client().ApplyURI(destURI)
	destMongoClient, err := mongo.Connect(ctx, destClientOpts)
	require.NoError(t, err, "Failed to connect to destination MongoDB")
	defer destMongoClient.Disconnect(ctx)

	// Create our client wrappers
	sourceClient, err := NewClient(ctx, sourceURI)
	require.NoError(t, err, "Failed to connect via client wrapper to source MongoDB")
	defer sourceClient.Disconnect(ctx)

	destClient, err := NewClient(ctx, destURI)
	require.NoError(t, err, "Failed to connect via client wrapper to destination MongoDB")
	defer destClient.Disconnect(ctx)

	// Set up test data with multiple collections
	dbName := "test_full_db"

	// Drop the database if it exists
	sourceMongoClient.Database(dbName).Drop(ctx)
	destMongoClient.Database(dbName).Drop(ctx)

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
	destDBClient := destClient.GetDatabase(dbName)

	// Get the list of collections
	collNames, err := sourceClient.ListCollections(ctx, dbName)
	require.NoError(t, err, "Failed to list collections")

	// Copy each collection
	for _, collName := range collNames {
		err = CopyCollection(ctx, sourceDBClient, destDBClient, collName, false, 10)
		require.NoError(t, err, "Failed to copy collection %s", collName)
	}

	// Verify all collections and documents were copied
	destDB := destMongoClient.Database(dbName)

	// Check if all collections exist
	destCollNames, err := destClient.ListCollections(ctx, dbName)
	require.NoError(t, err, "Failed to list collections in destination")

	assert.Equal(t, len(collections), len(destCollNames), "Expected %d collections, got %d", len(collections), len(destCollNames))

	// Check document counts in each collection
	for i, collName := range collections {
		expectedCount := (i + 1) * 5
		count, err := destDB.Collection(collName).CountDocuments(ctx, bson.M{})
		require.NoError(t, err, "Failed to count documents in %s", collName)

		assert.Equal(t, int64(expectedCount), count, "Expected %d documents in %s, got %d", expectedCount, collName, count)

		t.Logf("Verified %d documents in collection %s", count, collName)
	}

	t.Logf("Successfully tested database copy functionality")
}
