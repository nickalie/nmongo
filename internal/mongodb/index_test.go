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

// TestIndexCopy tests copying indexes between collections
func TestIndexCopy(t *testing.T) {
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

	// Connect directly to MongoDB
	sourceClientOpts := options.Client().ApplyURI(sourceURI)
	sourceMongoClient, err := mongo.Connect(ctx, sourceClientOpts)
	require.NoError(t, err, "Failed to connect to source MongoDB")
	defer sourceMongoClient.Disconnect(ctx)

	targetClientOpts := options.Client().ApplyURI(targetURI)
	targetMongoClient, err := mongo.Connect(ctx, targetClientOpts)
	require.NoError(t, err, "Failed to connect to target MongoDB")
	defer targetMongoClient.Disconnect(ctx)

	// Create our client wrappers
	sourceClient, err := NewClient(ctx, sourceURI)
	require.NoError(t, err, "Failed to connect via client wrapper to source MongoDB")
	defer sourceClient.Disconnect(ctx)

	targetClient, err := NewClient(ctx, targetURI)
	require.NoError(t, err, "Failed to connect via client wrapper to target MongoDB")
	defer targetClient.Disconnect(ctx)

	// Set up test data
	dbName := "test_index_copy_db"
	collName := "test_index_collection"

	// Drop the collections if they exist
	sourceMongoClient.Database(dbName).Collection(collName).Drop(ctx)
	targetMongoClient.Database(dbName).Collection(collName).Drop(ctx)

	// Create source collection with data
	sourceColl := sourceMongoClient.Database(dbName).Collection(collName)

	// Insert some test documents
	docs := []interface{}{
		bson.M{"_id": 1, "username": "user1", "email": "user1@example.com", "age": 25},
		bson.M{"_id": 2, "username": "user2", "email": "user2@example.com", "age": 30},
		bson.M{"_id": 3, "username": "user3", "email": "user3@example.com", "age": 35},
	}
	_, err = sourceColl.InsertMany(ctx, docs)
	require.NoError(t, err, "Failed to insert test documents")

	// Create some indexes on the source collection
	indexModels := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "username", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("unique_username"),
		},
		{
			Keys:    bson.D{{Key: "email", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("unique_email"),
		},
		{
			Keys:    bson.D{{Key: "age", Value: 1}},
			Options: options.Index().SetName("age_index"),
		},
	}

	// Create the indexes on the source collection
	_, err = sourceColl.Indexes().CreateMany(ctx, indexModels)
	require.NoError(t, err, "Failed to create indexes on source collection")

	// Verify indexes were created on source collection
	sourceIndexes, err := ListCollectionIndexes(ctx, sourceMongoClient.Database(dbName), collName)
	require.NoError(t, err, "Failed to list indexes on source collection")
	assert.Equal(t, 4, len(sourceIndexes), "Should have 4 indexes including _id index")

	// Now copy the collection using our client
	sourceDB := sourceClient.GetDatabase(dbName)
	targetDB := targetClient.GetDatabase(dbName)

	// Copy the collection
	err = CopyCollection(ctx, sourceDB, targetDB, collName, false, 10, "")
	require.NoError(t, err, "Failed to copy collection")

	// Verify indexes were copied to target collection
	targetIndexes, err := ListCollectionIndexes(ctx, targetMongoClient.Database(dbName), collName)
	require.NoError(t, err, "Failed to list indexes on target collection")
	assert.Equal(t, 4, len(targetIndexes), "Should have 4 indexes including _id index")

	// Verify index names were preserved
	targetIndexNames := make(map[string]bool)
	for _, idx := range targetIndexes {
		name, ok := idx["name"].(string)
		require.True(t, ok, "Index should have a name")
		targetIndexNames[name] = true
	}

	// Check that all our custom indexes exist in the target
	assert.True(t, targetIndexNames["unique_username"], "Should have unique_username index")
	assert.True(t, targetIndexNames["unique_email"], "Should have unique_email index")
	assert.True(t, targetIndexNames["age_index"], "Should have age_index index")

	t.Logf("Successfully tested index copying functionality")
}
