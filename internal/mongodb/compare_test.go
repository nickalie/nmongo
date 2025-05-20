package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// setupMongoContainer sets up a MongoDB container for testing
func setupMongoContainer(t *testing.T) (testcontainers.Container, string) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "mongo:5.0",
		ExposedPorts: []string{"27017/tcp"},
		WaitingFor:   wait.ForLog("Waiting for connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start MongoDB container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get MongoDB container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "27017")
	if err != nil {
		t.Fatalf("Failed to get MongoDB container port: %v", err)
	}

	connectionString := "mongodb://" + host + ":" + port.Port()
	return container, connectionString
}

// setupTestData sets up test data for comparison
func setupTestData(t *testing.T, connString string, dbName, collName string) *mongo.Client {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Create a test collection with documents
	coll := client.Database(dbName).Collection(collName)

	// Insert documents
	docs := []interface{}{
		bson.M{"_id": 1, "name": "Document 1", "value": 100, "lastModified": time.Now()},
		bson.M{"_id": 2, "name": "Document 2", "value": 200, "lastModified": time.Now()},
		bson.M{"_id": 3, "name": "Document 3", "value": 300, "lastModified": time.Now()},
	}

	_, err = coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create an index on the collection
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetUnique(true),
	}

	_, err = coll.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}

	return client
}

func TestCompareCollectionCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up two MongoDB containers
	sourceContainer, sourceConnString := setupMongoContainer(t)
	defer sourceContainer.Terminate(context.Background())

	targetContainer, targetConnString := setupMongoContainer(t)
	defer targetContainer.Terminate(context.Background())

	// Set up test databases
	dbName := "testdb"
	collName := "testcoll"

	sourceClient := setupTestData(t, sourceConnString, dbName, collName)
	defer sourceClient.Disconnect(context.Background())

	targetClient := setupTestData(t, targetConnString, dbName, collName)
	defer targetClient.Disconnect(context.Background())

	// Add one more document to source
	ctx := context.Background()
	sourceColl := sourceClient.Database(dbName).Collection(collName)
	_, err := sourceColl.InsertOne(ctx, bson.M{"_id": 4, "name": "Document 4", "value": 400, "lastModified": time.Now()})
	assert.NoError(t, err)

	// Run the comparison
	sourceDB := sourceClient.Database(dbName)
	targetDB := targetClient.Database(dbName)

	result, err := CompareCollectionCounts(ctx, sourceDB, targetDB, collName)
	assert.NoError(t, err)

	// Verify the results
	assert.Equal(t, dbName, result.Database)
	assert.Equal(t, collName, result.Collection)
	assert.Equal(t, int64(4), result.SourceCount)
	assert.Equal(t, int64(3), result.TargetCount)
	assert.Equal(t, int64(1), result.Difference)
}

func TestCompareCollectionData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up two MongoDB containers
	sourceContainer, sourceConnString := setupMongoContainer(t)
	defer sourceContainer.Terminate(context.Background())

	targetContainer, targetConnString := setupMongoContainer(t)
	defer targetContainer.Terminate(context.Background())

	// Set up test databases
	dbName := "testdb"
	collName := "testcoll"

	sourceClient := setupTestData(t, sourceConnString, dbName, collName)
	defer sourceClient.Disconnect(context.Background())

	targetClient := setupTestData(t, targetConnString, dbName, collName)
	defer targetClient.Disconnect(context.Background())

	ctx := context.Background()
	sourceColl := sourceClient.Database(dbName).Collection(collName)
	targetColl := targetClient.Database(dbName).Collection(collName)

	// Set up comparison scenarios:
	// 1. Add a document to source (will be missing in target)
	_, err := sourceColl.InsertOne(ctx, bson.M{"_id": 4, "name": "Document 4", "value": 400, "lastModified": time.Now()})
	assert.NoError(t, err)

	// 2. Add a document to target (will be missing in source)
	_, err = targetColl.InsertOne(ctx, bson.M{"_id": 5, "name": "Document 5", "value": 500, "lastModified": time.Now()})
	assert.NoError(t, err)

	// 3. Modify a document in target to be different from source
	_, err = targetColl.UpdateOne(
		ctx,
		bson.M{"_id": 2},
		bson.M{"$set": bson.M{"value": 201, "modified": true}},
	)
	assert.NoError(t, err)

	// Run the comparison
	sourceDB := sourceClient.Database(dbName)
	targetDB := targetClient.Database(dbName)

	result, err := CompareCollectionData(ctx, sourceDB, targetDB, collName, 100, true)
	assert.NoError(t, err)

	// Verify the results
	assert.Equal(t, dbName, result.Database)
	assert.Equal(t, collName, result.Collection)
	assert.Equal(t, int64(4), result.SourceCount)        // Original 3 + 1 new in source
	assert.Equal(t, int64(4), result.TargetCount)        // Original 3 + 1 new in target
	assert.Equal(t, int64(0), result.Difference)         // Same count but different content
	assert.Equal(t, int64(1), result.MissingInTarget)    // Document 4
	assert.Equal(t, int64(1), result.DifferentDocuments) // Document 2
	// Note: MissingInSource field was removed as it's no longer calculated
}

func TestBsonEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        bson.M
		b        bson.M
		expected bool
	}{
		{
			name:     "Empty documents",
			a:        bson.M{},
			b:        bson.M{},
			expected: true,
		},
		{
			name:     "Same simple document",
			a:        bson.M{"foo": "bar", "num": 42},
			b:        bson.M{"foo": "bar", "num": 42},
			expected: true,
		},
		{
			name:     "Different values",
			a:        bson.M{"foo": "bar", "num": 42},
			b:        bson.M{"foo": "bar", "num": 43},
			expected: false,
		},
		{
			name:     "Different keys",
			a:        bson.M{"foo": "bar", "num": 42},
			b:        bson.M{"foo": "bar", "number": 42},
			expected: false,
		},
		{
			name:     "Nested documents equal",
			a:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2}},
			b:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2}},
			expected: true,
		},
		{
			name:     "Nested documents different",
			a:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2}},
			b:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 3}},
			expected: false,
		},
		{
			name:     "Array equal",
			a:        bson.M{"foo": "bar", "arr": []interface{}{1, 2, 3}},
			b:        bson.M{"foo": "bar", "arr": []interface{}{1, 2, 3}},
			expected: true,
		},
		{
			name:     "Array different values",
			a:        bson.M{"foo": "bar", "arr": []interface{}{1, 2, 3}},
			b:        bson.M{"foo": "bar", "arr": []interface{}{1, 2, 4}},
			expected: false,
		},
		{
			name:     "Array different length",
			a:        bson.M{"foo": "bar", "arr": []interface{}{1, 2, 3}},
			b:        bson.M{"foo": "bar", "arr": []interface{}{1, 2}},
			expected: false,
		},
		{
			name:     "Complex nested structure",
			a:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2, "arr": []interface{}{bson.M{"x": 1}, bson.M{"y": 2}}}},
			b:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2, "arr": []interface{}{bson.M{"x": 1}, bson.M{"y": 2}}}},
			expected: true,
		},
		{
			name:     "Complex nested structure different",
			a:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2, "arr": []interface{}{bson.M{"x": 1}, bson.M{"y": 2}}}},
			b:        bson.M{"foo": "bar", "nested": bson.M{"a": 1, "b": 2, "arr": []interface{}{bson.M{"x": 1}, bson.M{"y": 3}}}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bsonEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompareIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up two MongoDB containers
	sourceContainer, sourceConnString := setupMongoContainer(t)
	defer sourceContainer.Terminate(context.Background())

	targetContainer, targetConnString := setupMongoContainer(t)
	defer targetContainer.Terminate(context.Background())

	// Set up test databases
	dbName := "testdb"
	collName := "testcoll"

	sourceClient := setupTestData(t, sourceConnString, dbName, collName)
	defer sourceClient.Disconnect(context.Background())

	targetClient := setupTestData(t, targetConnString, dbName, collName)
	defer targetClient.Disconnect(context.Background())

	// Run index comparison when indexes are the same
	ctx := context.Background()
	sourceDB := sourceClient.Database(dbName)
	targetDB := targetClient.Database(dbName)

	equal, reason, err := CompareIndexes(ctx, sourceDB, targetDB, collName)
	assert.NoError(t, err)
	assert.True(t, equal)
	assert.Empty(t, reason)

	// Add a different index to target
	targetColl := targetClient.Database(dbName).Collection(collName)
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "value", Value: 1}},
		Options: options.Index().SetName("value_index"),
	}

	_, err = targetColl.Indexes().CreateOne(ctx, indexModel)
	assert.NoError(t, err)

	// Run index comparison after adding different index
	equal, reason, err = CompareIndexes(ctx, sourceDB, targetDB, collName)
	assert.NoError(t, err)
	assert.False(t, equal)
	assert.Contains(t, reason, "Index count mismatch")
}
