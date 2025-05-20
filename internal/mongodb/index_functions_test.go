package mongodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestIsIDIndex tests the isIDIndex function
func TestIsIDIndex(t *testing.T) {
	tests := []struct {
		name     string
		indexDoc bson.M
		expected bool
	}{
		{
			name: "ID index",
			indexDoc: bson.M{
				"name": "_id_",
				"key":  bson.M{"_id": 1},
			},
			expected: true,
		},
		{
			name: "Non-ID index",
			indexDoc: bson.M{
				"name": "name_1",
				"key":  bson.M{"name": 1},
			},
			expected: false,
		},
		{
			name: "No name field",
			indexDoc: bson.M{
				"key": bson.M{"name": 1},
			},
			expected: false,
		},
		{
			name: "Name not string",
			indexDoc: bson.M{
				"name": 123,
				"key":  bson.M{"name": 1},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isIDIndex(tt.indexDoc)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractIndexKeys tests the extractIndexKeys function
func TestExtractIndexKeys(t *testing.T) {
	tests := []struct {
		name     string
		keyDoc   bson.M
		expected bson.D
	}{
		{
			name: "Single int32 key",
			keyDoc: bson.M{
				"name": int32(1),
			},
			expected: bson.D{
				{Key: "name", Value: int32(1)},
			},
		},
		{
			name: "Single float key (converts to int32)",
			keyDoc: bson.M{
				"age": float64(-1),
			},
			expected: bson.D{
				{Key: "age", Value: int32(-1)},
			},
		},
		{
			name: "Text index key",
			keyDoc: bson.M{
				"description": "text",
			},
			expected: bson.D{
				{Key: "description", Value: "text"},
			},
		},
		{
			name: "Multiple keys mixed types",
			keyDoc: bson.M{
				"name":        int32(1),
				"age":         float64(-1),
				"description": "text",
			},
			expected: bson.D{
				{Key: "name", Value: int32(1)},
				{Key: "age", Value: int32(-1)},
				{Key: "description", Value: "text"},
			},
		},
		{
			name:     "Empty key doc",
			keyDoc:   bson.M{},
			expected: bson.D{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractIndexKeys(tt.keyDoc)

			// Sort the results for reliable comparison
			// Note: bson.D from extractIndexKeys may have elements in a different order
			// than expected because map iteration order is not guaranteed
			assert.Equal(t, len(tt.expected), len(result), "Result should have same number of elements")

			// Create maps for easier comparison
			resultMap := make(map[string]interface{})
			for _, e := range result {
				resultMap[e.Key] = e.Value
			}

			expectedMap := make(map[string]interface{})
			for _, e := range tt.expected {
				expectedMap[e.Key] = e.Value
			}

			assert.Equal(t, expectedMap, resultMap, "Result should have same elements")
		})
	}
}

// TestBuildIndexOptions tests the buildIndexOptions function
func TestBuildIndexOptions(t *testing.T) {
	tests := []struct {
		name     string
		indexDoc bson.M
		check    func(*testing.T, *options.IndexOptions)
	}{
		{
			name: "With name",
			indexDoc: bson.M{
				"name": "test_index",
			},
			check: func(t *testing.T, opts *options.IndexOptions) {
				assert.Equal(t, "test_index", *opts.Name)
			},
		},
		{
			name: "With unique",
			indexDoc: bson.M{
				"unique": true,
			},
			check: func(t *testing.T, opts *options.IndexOptions) {
				assert.True(t, *opts.Unique)
			},
		},
		{
			name: "With sparse",
			indexDoc: bson.M{
				"sparse": true,
			},
			check: func(t *testing.T, opts *options.IndexOptions) {
				assert.True(t, *opts.Sparse)
			},
		},
		{
			name: "With expireAfterSeconds",
			indexDoc: bson.M{
				"expireAfterSeconds": int32(3600),
			},
			check: func(t *testing.T, opts *options.IndexOptions) {
				assert.Equal(t, int32(3600), *opts.ExpireAfterSeconds)
			},
		},
		{
			name: "With multiple options",
			indexDoc: bson.M{
				"name":               "test_index",
				"unique":             true,
				"sparse":             true,
				"expireAfterSeconds": int32(3600),
			},
			check: func(t *testing.T, opts *options.IndexOptions) {
				assert.Equal(t, "test_index", *opts.Name)
				assert.True(t, *opts.Unique)
				assert.True(t, *opts.Sparse)
				assert.Equal(t, int32(3600), *opts.ExpireAfterSeconds)
			},
		},
		{
			name:     "Empty index doc",
			indexDoc: bson.M{},
			check: func(t *testing.T, opts *options.IndexOptions) {
				// Should create options with no settings
				assert.Nil(t, opts.Name)
				assert.Nil(t, opts.Unique)
				assert.Nil(t, opts.Sparse)
				assert.Nil(t, opts.ExpireAfterSeconds)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildIndexOptions(tt.indexDoc)
			tt.check(t, result)
		})
	}
}

// TestConvertToIndexModel tests the convertToIndexModel function
func TestConvertToIndexModel(t *testing.T) {
	// Test with valid key field
	t.Run("Valid index document", func(t *testing.T) {
		indexDoc := bson.M{
			"name": "name_1",
			"key":  bson.M{"name": int32(1)},
		}

		model, err := convertToIndexModel(indexDoc)
		assert.NoError(t, err)

		// Check keys
		keys, ok := model.Keys.(bson.D)
		assert.True(t, ok, "Keys should be bson.D")
		assert.Equal(t, 1, len(keys), "Should have 1 key")
		assert.Equal(t, "name", keys[0].Key)
		assert.Equal(t, int32(1), keys[0].Value)

		// Check options
		assert.Equal(t, "name_1", *model.Options.Name)
	})

	// Test with missing key field
	t.Run("Missing key field", func(t *testing.T) {
		indexDoc := bson.M{
			"name": "name_1",
			// Missing key field
		}

		_, err := convertToIndexModel(indexDoc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index does not have a valid key field")
	})

	// Test with invalid key field type
	t.Run("Invalid key field type", func(t *testing.T) {
		indexDoc := bson.M{
			"name": "name_1",
			"key":  "not a bson.M",
		}

		_, err := convertToIndexModel(indexDoc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "index does not have a valid key field")
	})
}
