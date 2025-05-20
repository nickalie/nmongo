package mongodb

import (
	"context"
	"io"
	"os"
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

func TestUpdateSourceProgress(t *testing.T) {
	// Redirect stdout to capture output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	collName := "testCollection"
	result := &DocumentProcessingResult{
		docCount:        100,
		missingInTarget: 5,
		different:       10,
	}
	sourceCount := int64(200)

	updateSourceProgress(collName, result, sourceCount)

	// Restore stdout and get output
	w.Close()
	os.Stdout = oldStdout

	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify the output
	assert.Contains(t, outputStr, "Compared 100/200 documents in testCollection")
	assert.Contains(t, outputStr, "Missing in target: 5, Different: 10")
}

func TestFilterByExclusionList(t *testing.T) {
	testCases := []struct {
		name           string
		items          []string
		exclusionList  []string
		expectedResult []string
	}{
		{
			name:           "Empty items and exclusions",
			items:          []string{},
			exclusionList:  []string{},
			expectedResult: []string{},
		},
		{
			name:           "Empty exclusions",
			items:          []string{"a", "b", "c"},
			exclusionList:  []string{},
			expectedResult: []string{"a", "b", "c"},
		},
		{
			name:           "Empty items",
			items:          []string{},
			exclusionList:  []string{"a", "b"},
			expectedResult: []string{},
		},
		{
			name:           "No matches in exclusion list",
			items:          []string{"a", "b", "c"},
			exclusionList:  []string{"d", "e"},
			expectedResult: []string{"a", "b", "c"},
		},
		{
			name:           "Some matches in exclusion list",
			items:          []string{"a", "b", "c", "d", "e"},
			exclusionList:  []string{"b", "d"},
			expectedResult: []string{"a", "c", "e"},
		},
		{
			name:           "All items in exclusion list",
			items:          []string{"a", "b", "c"},
			exclusionList:  []string{"a", "b", "c", "d"},
			expectedResult: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := filterByExclusionList(tc.items, tc.exclusionList)
			assert.Equal(t, tc.expectedResult, result)
		})
	}
}

func TestCompareCollections(t *testing.T) {
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
	collName1 := "testcoll1"
	collName2 := "testcoll2"

	// Setup source client and collections
	ctx := context.Background()
	sourceMongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceConnString))
	assert.NoError(t, err)
	defer sourceMongoClient.Disconnect(ctx)

	sourceClient, err := NewClient(ctx, sourceConnString, "")
	assert.NoError(t, err)
	defer sourceClient.Disconnect(ctx)

	// Create test collections in source
	sourceDB := sourceMongoClient.Database(dbName)
	coll1 := sourceDB.Collection(collName1)
	coll2 := sourceDB.Collection(collName2)

	// Insert test data
	docs1 := []interface{}{
		bson.M{"_id": 1, "name": "Doc1Coll1", "value": 100},
		bson.M{"_id": 2, "name": "Doc2Coll1", "value": 200},
	}
	_, err = coll1.InsertMany(ctx, docs1)
	assert.NoError(t, err)

	docs2 := []interface{}{
		bson.M{"_id": 1, "name": "Doc1Coll2", "value": 300},
		bson.M{"_id": 2, "name": "Doc2Coll2", "value": 400},
	}
	_, err = coll2.InsertMany(ctx, docs2)
	assert.NoError(t, err)

	// Setup target client and collections
	targetMongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(targetConnString))
	assert.NoError(t, err)
	defer targetMongoClient.Disconnect(ctx)

	targetClient, err := NewClient(ctx, targetConnString, "")
	assert.NoError(t, err)
	defer targetClient.Disconnect(ctx)

	// Create test collections in target
	targetDB := targetMongoClient.Database(dbName)
	targetColl1 := targetDB.Collection(collName1)
	targetColl2 := targetDB.Collection(collName2)

	// Insert test data with some differences
	targetDocs1 := []interface{}{
		bson.M{"_id": 1, "name": "Doc1Coll1", "value": 100},         // Same as source
		bson.M{"_id": 2, "name": "Doc2Coll1Modified", "value": 201}, // Different from source
	}
	_, err = targetColl1.InsertMany(ctx, targetDocs1)
	assert.NoError(t, err)

	// Only insert one document in collection 2
	targetDocs2 := []interface{}{
		bson.M{"_id": 1, "name": "Doc1Coll2", "value": 300}, // Same as source
	}
	_, err = targetColl2.InsertMany(ctx, targetDocs2)
	assert.NoError(t, err)

	// Test CompareCollections with specified collections
	results, err := CompareCollections(ctx, sourceClient, targetClient, dbName, []string{collName1, collName2}, nil, 100, true)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Verify results for collection 1
	var coll1Result, coll2Result *ComparisonResult
	for _, res := range results {
		switch res.Collection {
		case collName1:
			coll1Result = res
		case collName2:
			coll2Result = res
		}
	}

	assert.NotNil(t, coll1Result)
	assert.Equal(t, dbName, coll1Result.Database)
	assert.Equal(t, collName1, coll1Result.Collection)
	assert.Equal(t, int64(2), coll1Result.SourceCount)
	assert.Equal(t, int64(2), coll1Result.TargetCount)
	assert.Equal(t, int64(0), coll1Result.Difference)
	assert.Equal(t, int64(0), coll1Result.MissingInTarget)
	assert.Equal(t, int64(1), coll1Result.DifferentDocuments)

	assert.NotNil(t, coll2Result)
	assert.Equal(t, dbName, coll2Result.Database)
	assert.Equal(t, collName2, coll2Result.Collection)
	assert.Equal(t, int64(2), coll2Result.SourceCount)
	assert.Equal(t, int64(1), coll2Result.TargetCount)
	assert.Equal(t, int64(1), coll2Result.Difference)
	assert.Equal(t, int64(1), coll2Result.MissingInTarget)
	assert.Equal(t, int64(0), coll2Result.DifferentDocuments)

	// Test CompareCollections with an exclusion list
	results, err = CompareCollections(ctx, sourceClient, targetClient, dbName, nil, []string{collName2}, 100, true)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, collName1, results[0].Collection)
}

func TestGetCollectionsToCompare(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up MongoDB container
	container, connString := setupMongoContainer(t)
	defer container.Terminate(context.Background())

	// Set up test database with multiple collections
	ctx := context.Background()
	dbName := "testdb"

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	assert.NoError(t, err)
	defer mongoClient.Disconnect(ctx)

	// Create our client wrapper
	client, err := NewClient(ctx, connString, "")
	assert.NoError(t, err)
	defer client.Disconnect(ctx)

	// Create test collections
	db := mongoClient.Database(dbName)
	collections := []string{"coll1", "coll2", "coll3"}

	for _, collName := range collections {
		coll := db.Collection(collName)
		_, err = coll.InsertOne(ctx, bson.M{"_id": 1, "name": "test"})
		assert.NoError(t, err)
	}

	// Test with specified collections
	specifiedColls := []string{"coll1", "coll2"}
	result, err := getCollectionsToCompare(ctx, client, dbName, specifiedColls, nil)
	assert.NoError(t, err)
	assert.Equal(t, specifiedColls, result)

	// Test with nil collections (should list all except system collections)
	result, err = getCollectionsToCompare(ctx, client, dbName, nil, nil)
	assert.NoError(t, err)
	// Should not include system.views
	assert.ElementsMatch(t, []string{"coll1", "coll2", "coll3"}, result)

	// Test with exclusion list
	result, err = getCollectionsToCompare(ctx, client, dbName, nil, []string{"coll3"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"coll1", "coll2"}, result)

	// Test with both specified collections and exclusion list
	result, err = getCollectionsToCompare(ctx, client, dbName, []string{"coll1", "coll2", "coll3"}, []string{"coll2"})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"coll1", "coll3"}, result)
}

func TestCompareCollectionSet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set up two MongoDB containers
	sourceContainer, sourceConnString := setupMongoContainer(t)
	defer sourceContainer.Terminate(context.Background())

	targetContainer, targetConnString := setupMongoContainer(t)
	defer targetContainer.Terminate(context.Background())

	// Set up test database
	ctx := context.Background()
	dbName := "testdb"

	// Source setup
	sourceMongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(sourceConnString))
	assert.NoError(t, err)
	defer sourceMongoClient.Disconnect(ctx)

	sourceDB := sourceMongoClient.Database(dbName)

	// Create test collections in source
	coll1 := sourceDB.Collection("coll1")
	_, err = coll1.InsertMany(ctx, []interface{}{
		bson.M{"_id": 1, "name": "Doc1"},
		bson.M{"_id": 2, "name": "Doc2"},
	})
	assert.NoError(t, err)

	coll2 := sourceDB.Collection("coll2")
	_, err = coll2.InsertMany(ctx, []interface{}{
		bson.M{"_id": 3, "name": "Doc3"},
		bson.M{"_id": 4, "name": "Doc4"},
	})
	assert.NoError(t, err)

	// Target setup
	targetMongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(targetConnString))
	assert.NoError(t, err)
	defer targetMongoClient.Disconnect(ctx)

	targetDB := targetMongoClient.Database(dbName)

	// Create collections in target with differences
	targetColl1 := targetDB.Collection("coll1")
	_, err = targetColl1.InsertMany(ctx, []interface{}{
		bson.M{"_id": 1, "name": "Doc1"},
		// Missing Doc2
	})
	assert.NoError(t, err)

	targetColl2 := targetDB.Collection("coll2")
	_, err = targetColl2.InsertMany(ctx, []interface{}{
		bson.M{"_id": 3, "name": "Doc3Modified"}, // Different
		bson.M{"_id": 4, "name": "Doc4"},
	})
	assert.NoError(t, err)

	// Test compareCollectionSet
	results := []*ComparisonResult{}
	collections := []string{"coll1", "coll2"}

	results, err = compareCollectionSet(ctx, sourceDB, targetDB, collections, 100, true, results)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// Verify results
	var coll1Result, coll2Result *ComparisonResult
	for _, result := range results {
		switch result.Collection {
		case "coll1":
			coll1Result = result
		case "coll2":
			coll2Result = result
		}
	}

	assert.NotNil(t, coll1Result)
	assert.Equal(t, dbName, coll1Result.Database)
	assert.Equal(t, int64(2), coll1Result.SourceCount)
	assert.Equal(t, int64(1), coll1Result.TargetCount)
	assert.Equal(t, int64(1), coll1Result.Difference)
	assert.Equal(t, int64(1), coll1Result.MissingInTarget)

	assert.NotNil(t, coll2Result)
	assert.Equal(t, dbName, coll2Result.Database)
	assert.Equal(t, int64(2), coll2Result.SourceCount)
	assert.Equal(t, int64(2), coll2Result.TargetCount)
	assert.Equal(t, int64(0), coll2Result.Difference)
	assert.Equal(t, int64(0), coll2Result.MissingInTarget)
	assert.Equal(t, int64(1), coll2Result.DifferentDocuments)

	// Test with count-only
	results = []*ComparisonResult{}
	results, err = compareCollectionSet(ctx, sourceDB, targetDB, collections, 100, false, results)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(results))

	// For count-only, we should still get collection counts but no missing or different docs
	for _, result := range results {
		if result.Collection == "coll1" {
			assert.Equal(t, int64(2), result.SourceCount)
			assert.Equal(t, int64(1), result.TargetCount)
			assert.Equal(t, int64(1), result.Difference)
			assert.Equal(t, int64(0), result.MissingInTarget) // Not calculated in count-only mode
		}
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
