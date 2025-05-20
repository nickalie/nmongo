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

// TestDocumentOperations tests various document operations using a table-driven approach
func TestDocumentOperations(t *testing.T) {
	// Set up test context
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Start MongoDB container
	uri, container, err := startMongoContainer(ctx)
	require.NoError(t, err, "Failed to start MongoDB container")
	defer container.Terminate(ctx)

	t.Logf("MongoDB URI: %s", uri)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer client.Disconnect(ctx)

	// Create test database and collection
	dbName := "test_operations_db"
	collName := "test_operations"

	collection := client.Database(dbName).Collection(collName)

	// Define test cases using a table-driven approach
	testCases := []struct {
		name            string
		initialDocs     []interface{}
		operation       func(coll *mongo.Collection) error
		expectedCount   int64
		verifyOperation func(t *testing.T, coll *mongo.Collection)
	}{
		{
			name: "Insert documents",
			initialDocs: []interface{}{
				bson.M{"_id": 1, "name": "Doc 1", "category": "A", "value": 10},
				bson.M{"_id": 2, "name": "Doc 2", "category": "B", "value": 20},
			},
			operation: func(coll *mongo.Collection) error {
				_, err := coll.InsertOne(ctx, bson.M{"_id": 3, "name": "Doc 3", "category": "A", "value": 30})
				return err
			},
			expectedCount: 3,
			verifyOperation: func(t *testing.T, coll *mongo.Collection) {
				var doc bson.M
				err := coll.FindOne(ctx, bson.M{"_id": 3}).Decode(&doc)
				assert.NoError(t, err, "Document with _id=3 should exist")
				assert.Equal(t, "Doc 3", doc["name"], "Document name should match")
				assert.Equal(t, "A", doc["category"], "Document category should match")
				assert.Equal(t, int32(30), doc["value"], "Document value should match")
			},
		},
		{
			name: "Update documents",
			initialDocs: []interface{}{
				bson.M{"_id": 1, "name": "Doc 1", "category": "A", "value": 10},
				bson.M{"_id": 2, "name": "Doc 2", "category": "B", "value": 20},
				bson.M{"_id": 3, "name": "Doc 3", "category": "A", "value": 30},
			},
			operation: func(coll *mongo.Collection) error {
				_, err := coll.UpdateMany(
					ctx,
					bson.M{"category": "A"},
					bson.M{"$set": bson.M{"updated": true}, "$inc": bson.M{"value": 5}},
				)
				return err
			},
			expectedCount: 3,
			verifyOperation: func(t *testing.T, coll *mongo.Collection) {
				// Verify documents with category A were updated
				cursor, err := coll.Find(ctx, bson.M{"category": "A"})
				require.NoError(t, err, "Should find category A documents")

				var docs []bson.M
				err = cursor.All(ctx, &docs)
				require.NoError(t, err, "Should decode documents")

				assert.Len(t, docs, 2, "Should have 2 documents in category A")

				// Check that all category A documents were properly updated
				for _, doc := range docs {
					assert.True(t, doc["updated"].(bool), "Document should be marked as updated")
					assert.GreaterOrEqual(t, doc["value"], int32(15), "Document value should be increased by 5")
				}

				// Verify category B document was not updated
				var docB bson.M
				err = coll.FindOne(ctx, bson.M{"category": "B"}).Decode(&docB)
				require.NoError(t, err, "Should find category B document")
				_, hasUpdated := docB["updated"]
				assert.False(t, hasUpdated, "Category B document should not have 'updated' field")
				assert.Equal(t, int32(20), docB["value"], "Category B document value should not change")
			},
		},
		{
			name: "Delete documents",
			initialDocs: []interface{}{
				bson.M{"_id": 1, "name": "Doc 1", "category": "A", "value": 10},
				bson.M{"_id": 2, "name": "Doc 2", "category": "B", "value": 20},
				bson.M{"_id": 3, "name": "Doc 3", "category": "A", "value": 30},
			},
			operation: func(coll *mongo.Collection) error {
				_, err := coll.DeleteMany(ctx, bson.M{"category": "A"})
				return err
			},
			expectedCount: 1,
			verifyOperation: func(t *testing.T, coll *mongo.Collection) {
				// Verify category A documents were deleted
				count, err := coll.CountDocuments(ctx, bson.M{"category": "A"})
				require.NoError(t, err, "Should count category A documents")
				assert.Equal(t, int64(0), count, "All category A documents should be deleted")

				// Verify category B document still exists
				count, err = coll.CountDocuments(ctx, bson.M{"category": "B"})
				require.NoError(t, err, "Should count category B documents")
				assert.Equal(t, int64(1), count, "Category B document should still exist")
			},
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear collection
			collection.Drop(ctx)

			// Insert initial documents
			if len(tc.initialDocs) > 0 {
				_, err := collection.InsertMany(ctx, tc.initialDocs)
				require.NoError(t, err, "Failed to insert initial documents")
			}

			// Perform operation
			err := tc.operation(collection)
			require.NoError(t, err, "Operation should succeed")

			// Verify document count
			count, err := collection.CountDocuments(ctx, bson.M{})
			require.NoError(t, err, "Should count documents")
			assert.Equal(t, tc.expectedCount, count, "Document count should match expected")

			// Run specific verification for the test case
			if tc.verifyOperation != nil {
				tc.verifyOperation(t, collection)
			}
		})
	}
}
