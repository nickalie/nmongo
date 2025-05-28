package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

// startMongoContainer starts a MongoDB container and returns the connection URI
func startMongoContainer(ctx context.Context) (string, *mongodb.MongoDBContainer, error) {
	container, err := mongodb.Run(ctx, "mongo:8.0")
	if err != nil {
		return "", nil, err
	}

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		return "", nil, err
	}

	return uri, container, nil
}

// TestClient tests basic MongoDB client functionality using testcontainers
func TestClient(t *testing.T) {
	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start MongoDB container
	uri, container, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start MongoDB container")
	defer container.Terminate(ctx)

	t.Logf("MongoDB URI: %s", uri)

	// Connect to MongoDB with connection timeout
	connCtx, connCancel := context.WithTimeout(context.Background(), 30*time.Second)
	client, err := NewClient(connCtx, uri, "")
	connCancel()
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	// Test listing databases
	dbs, err := client.ListDatabases(ctx)
	require.NoError(t, err, "Failed to list databases")
	t.Logf("Found %d databases", len(dbs))

	// Test creating a database and collection
	dbName := "test_db"
	collName := "test_collection"

	db := client.GetDatabase(dbName)
	coll := db.Collection(collName)

	// Insert a test document
	_, err = coll.InsertOne(ctx, bson.M{"name": "Test Document", "value": 42})
	require.NoError(t, err, "Failed to insert test document")

	// Verify database and collection were created
	dbs, err = client.ListDatabases(ctx)
	require.NoError(t, err, "Failed to list databases")

	foundDB := false
	for _, db := range dbs {
		if db == dbName {
			foundDB = true
			break
		}
	}

	assert.True(t, foundDB, "Test database should be in the database list")

	colls, err := client.ListCollections(ctx, dbName)
	require.NoError(t, err, "Failed to list collections")

	foundColl := false
	for _, coll := range colls {
		if coll == collName {
			foundColl = true
			break
		}
	}

	assert.True(t, foundColl, "Test collection should be in the collection list")
	t.Logf("Successfully tested MongoDB client functionality")
}

// TestOperationTimeouts tests that operations can proceed with individual timeouts
func TestOperationTimeouts(t *testing.T) {
	// Set up test context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start MongoDB container
	uri, container, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start MongoDB container")
	defer container.Terminate(ctx)

	// Create a background context for the main operation
	bgCtx := context.Background()

	// Connect to MongoDB with connection timeout
	connCtx, connCancel := context.WithTimeout(bgCtx, 10*time.Second)
	client, err := NewClient(connCtx, uri, "")
	connCancel()
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(bgCtx)

	// Set up source and target databases
	sourceDB := client.GetDatabase("source_test_db")
	targetDB := client.GetDatabase("target_test_db")
	collName := "timeout_test_collection"

	// Insert test data - at least 100 documents
	sourceColl := sourceDB.Collection(collName)
	var docs []interface{}
	for i := 0; i < 100; i++ {
		docs = append(docs, bson.M{"test_id": i, "data": fmt.Sprintf("Test data %d", i)})
	}
	_, err = sourceColl.InsertMany(ctx, docs)
	require.NoError(t, err, "Failed to insert test documents")

	// Test CopyCollection with a long-running operation context
	err = CopyCollection(
		bgCtx,
		sourceDB, targetDB,
		collName,
		false,
		10, // Small batch size to test multiple batches
		"lastModified",
		5, // retry attempts
	)
	require.NoError(t, err, "CopyCollection should succeed with a background context")

	// Count documents in target collection to verify copy worked
	targetColl := targetDB.Collection(collName)
	count, err := targetColl.CountDocuments(ctx, bson.M{})
	require.NoError(t, err, "Failed to count documents in target collection")
	assert.Equal(t, int64(100), count, "Target collection should have 100 documents")
}
