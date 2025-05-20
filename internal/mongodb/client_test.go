package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
)

// startMongoContainer starts a MongoDB container and returns the connection URI
func startMongoContainer(ctx context.Context) (string, testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "mongo:latest",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("failed to start container: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "27017")
	if err != nil {
		return "", nil, fmt.Errorf("failed to get container port: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get container host: %w", err)
	}

	uri := fmt.Sprintf("mongodb://%s:%s", host, mappedPort.Port())
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

	// Connect to MongoDB
	client, err := NewClient(ctx, uri, "")
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
