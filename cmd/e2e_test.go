package cmd

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"nmongo/internal/config"
)

func TestEndToEndCopyAndCompare(t *testing.T) {
	ctx := context.Background()

	// Reset command state
	configFile = ""

	// Start source MongoDB container
	sourceContainer, err := mongodb.Run(ctx, "mongo:8.0")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := sourceContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate source container: %s", err)
		}
	})

	srcURI, err := sourceContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Start target MongoDB container
	targetContainer, err := mongodb.Run(ctx, "mongo:8.0")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := targetContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate target container: %s", err)
		}
	})

	tgtURI, err := targetContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// Seed source MongoDB with random data
	err = seedMongoDB(ctx, srcURI)
	require.NoError(t, err)

	// Create temp config file for copy command
	tempConfigFile, err := createTempConfigFile(srcURI, tgtURI)
	require.NoError(t, err)
	defer os.Remove(tempConfigFile)

	t.Logf("Created config file at: %s", tempConfigFile)
	t.Logf("Source URI: %s", srcURI)
	t.Logf("Target URI: %s", tgtURI)

	// Load config and set variables for copy
	cfg, err := config.LoadConfig(tempConfigFile)
	require.NoError(t, err)

	// Debug: print config
	t.Logf("Config loaded: sourceURI=%s, targetURI=%s", cfg.SourceURI, cfg.TargetURI)

	// Set copy command variables
	sourceURI = cfg.SourceURI
	targetURI = cfg.TargetURI
	databases = cfg.Databases
	collections = cfg.Collections

	// Execute copy command
	err = runCopy()
	require.NoError(t, err)

	// Set compare command variables
	compareSourceURI = cfg.SourceURI
	compareTargetURI = cfg.TargetURI
	compareDatabases = cfg.Databases
	compareCollections = cfg.Collections

	// Execute compare command
	err = runCompare()
	require.NoError(t, err)

	// Verify that there are no differences
	// The compare command should exit with no error if databases are identical
	assert.NoError(t, err, "Compare command should find no differences between source and target")
}

func seedMongoDB(ctx context.Context, uri string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Seed with 10 databases
	for dbIdx := 0; dbIdx < 10; dbIdx++ {
		dbName := fmt.Sprintf("testdb_%d", dbIdx)
		db := client.Database(dbName)

		// Create 10 collections per database
		for colIdx := 0; colIdx < 10; colIdx++ {
			colName := fmt.Sprintf("collection_%d", colIdx)
			collection := db.Collection(colName)

			// Insert 10,000 documents per collection
			documents := make([]interface{}, 10000)
			for docIdx := 0; docIdx < 10000; docIdx++ {
				documents[docIdx] = generateRandomDocument(dbIdx, colIdx, docIdx)
			}

			// Insert in batches for better performance
			batchSize := 1000
			for i := 0; i < len(documents); i += batchSize {
				end := i + batchSize
				if end > len(documents) {
					end = len(documents)
				}
				_, err := collection.InsertMany(ctx, documents[i:end])
				if err != nil {
					return fmt.Errorf("failed to insert documents: %w", err)
				}
			}
		}
	}

	return nil
}

func generateRandomDocument(dbIdx, colIdx, docIdx int) bson.M {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(dbIdx*10000+colIdx*1000+docIdx)))

	doc := bson.M{
		"_id":          primitive.NewObjectID(),
		"dbIndex":      dbIdx,
		"colIndex":     colIdx,
		"docIndex":     docIdx,
		"timestamp":    time.Now().Add(time.Duration(r.Intn(1000)) * time.Hour),
		"lastModified": time.Now(),
		"stringField":  fmt.Sprintf("string_%d_%d_%d", dbIdx, colIdx, docIdx),
		"intField":     r.Intn(10000),
		"floatField":   r.Float64() * 1000,
		"boolField":    r.Intn(2) == 1,
		"arrayField":   []int{r.Intn(100), r.Intn(100), r.Intn(100)},
		"nestedField": bson.M{
			"nestedString": fmt.Sprintf("nested_%d", r.Intn(1000)),
			"nestedInt":    r.Intn(1000),
			"nestedArray":  []string{"item1", "item2", "item3"},
		},
		"tags": generateRandomTags(r),
	}

	// Add some variety - not all documents have all fields
	if r.Float32() < 0.3 {
		doc["optionalField"] = fmt.Sprintf("optional_%d", r.Intn(100))
	}
	if r.Float32() < 0.2 {
		doc["rareField"] = bson.M{
			"data": r.Intn(1000),
			"info": "rare information",
		}
	}

	return doc
}

func generateRandomTags(r *rand.Rand) []string {
	tagOptions := []string{"tag1", "tag2", "tag3", "tag4", "tag5", "important", "archived", "pending", "processed"}
	numTags := r.Intn(5) + 1
	tags := make([]string, 0, numTags)
	used := make(map[string]bool)

	for i := 0; i < numTags; i++ {
		tag := tagOptions[r.Intn(len(tagOptions))]
		if !used[tag] {
			tags = append(tags, tag)
			used[tag] = true
		}
	}

	return tags
}

func createTempConfigFile(sourceURI, targetURI string) (string, error) {
	config := fmt.Sprintf(`sourceUri: "%s"
targetUri: "%s"
databases: []
collections: []
`, sourceURI, targetURI)

	tmpFile, err := os.CreateTemp("", "nmongo-test-config-*.yaml")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := tmpFile.Write([]byte(config)); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to write config: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	return tmpFile.Name(), nil
}
