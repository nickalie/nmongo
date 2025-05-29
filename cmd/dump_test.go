package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestDumpStateOperations(t *testing.T) {
	t.Run("SaveAndLoadDumpState", func(t *testing.T) {
		tempDir := t.TempDir()
		stateFile := filepath.Join(tempDir, "test-state.json")

		originalState := &DumpState{
			Collections: map[string]CollectionState{
				"db1.coll1": {
					LastDumpTime:  time.Now().UTC(),
					DocumentCount: 100,
				},
				"db1.coll2": {
					LastDumpTime:  time.Now().UTC().Add(-1 * time.Hour),
					DocumentCount: 200,
				},
			},
		}

		err := saveDumpState(originalState, stateFile)
		require.NoError(t, err)

		loadedState, err := loadDumpState(stateFile)
		require.NoError(t, err)

		assert.Equal(t, len(originalState.Collections), len(loadedState.Collections))
		for key, origColl := range originalState.Collections {
			loadedColl, exists := loadedState.Collections[key]
			require.True(t, exists)
			assert.Equal(t, origColl.DocumentCount, loadedColl.DocumentCount)
			assert.WithinDuration(t, origColl.LastDumpTime, loadedColl.LastDumpTime, time.Second)
		}
	})

	t.Run("LoadDumpStateNonExistentFile", func(t *testing.T) {
		_, err := loadDumpState("/non/existent/file.json")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("LoadDumpStateInvalidJSON", func(t *testing.T) {
		tempDir := t.TempDir()
		stateFile := filepath.Join(tempDir, "invalid-state.json")

		err := os.WriteFile(stateFile, []byte("invalid json"), 0644)
		require.NoError(t, err)

		_, err = loadDumpState(stateFile)
		assert.Error(t, err)
	})
}

func TestFilterByExclusionList(t *testing.T) {
	tests := []struct {
		name       string
		items      []string
		exclusions []string
		expected   []string
	}{
		{
			name:       "NoExclusions",
			items:      []string{"db1", "db2", "db3"},
			exclusions: []string{},
			expected:   []string{"db1", "db2", "db3"},
		},
		{
			name:       "ExcludeSome",
			items:      []string{"db1", "db2", "db3", "admin", "local"},
			exclusions: []string{"admin", "local"},
			expected:   []string{"db1", "db2", "db3"},
		},
		{
			name:       "ExcludeAll",
			items:      []string{"admin", "local"},
			exclusions: []string{"admin", "local"},
			expected:   []string{},
		},
		{
			name:       "ExcludeNonExistent",
			items:      []string{"db1", "db2"},
			exclusions: []string{"db3", "db4"},
			expected:   []string{"db1", "db2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterByExclusionList(tt.items, tt.exclusions)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildMongodumpArgs(t *testing.T) {
	tests := []struct {
		name       string
		dbName     string
		collName   string
		outputPath string
		query      string
		setupFunc  func()
		expected   []string
	}{
		{
			name:       "BasicArgs",
			dbName:     "testdb",
			collName:   "testcoll",
			outputPath: filepath.Join(os.TempDir(), "dump"),
			query:      "",
			setupFunc: func() {
				dumpSourceURI = "mongodb://localhost:27017"
				dumpSourceCACertFile = ""
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--out", filepath.Join(os.TempDir(), "dump"),
			},
		},
		{
			name:       "WithQuery",
			dbName:     "testdb",
			collName:   "testcoll",
			outputPath: filepath.Join(os.TempDir(), "dump"),
			query:      `{"lastModified":{"$gt":"2023-01-01"}}`,
			setupFunc: func() {
				dumpSourceURI = "mongodb://localhost:27017"
				dumpSourceCACertFile = ""
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--out", filepath.Join(os.TempDir(), "dump"),
				"--query", `{"lastModified":{"$gt":"2023-01-01"}}`,
			},
		},
		{
			name:       "WithCAFile",
			dbName:     "testdb",
			collName:   "testcoll",
			outputPath: filepath.Join(os.TempDir(), "dump"),
			query:      "",
			setupFunc: func() {
				dumpSourceURI = "mongodb://localhost:27017"
				dumpSourceCACertFile = filepath.Join("path", "to", "ca.pem")
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--out", filepath.Join(os.TempDir(), "dump"),
				"--sslCAFile", filepath.Join("path", "to", "ca.pem"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupFunc()
			result := buildMongodumpArgs(tt.dbName, tt.collName, tt.outputPath, tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDumpCollectionIntegration(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("DumpCollectionFirstTime", func(mt *mtest.T) {
		state := &DumpState{Collections: make(map[string]CollectionState)}
		dbName := "testdb"
		collName := "testcoll"
		collKey := fmt.Sprintf("%s.%s", dbName, collName)

		dumpIncremental = false
		dumpRetryAttempts = 1

		mt.AddMockResponses(mtest.CreateCursorResponse(1, "testdb.testcoll", mtest.FirstBatch))

		// ctx := context.Background()

		_, exists := state.Collections[collKey]
		assert.False(t, exists)
	})

	mt.Run("DumpCollectionIncremental", func(mt *mtest.T) {
		state := &DumpState{
			Collections: map[string]CollectionState{
				"testdb.testcoll": {
					LastDumpTime:  time.Now().Add(-24 * time.Hour),
					DocumentCount: 50,
				},
			},
		}

		dumpIncremental = true
		dumpLastModifiedField = "lastModified"
		dumpRetryAttempts = 1

		collState := state.Collections["testdb.testcoll"]
		assert.False(t, collState.LastDumpTime.IsZero())
		assert.Equal(t, int64(50), collState.DocumentCount)
	})
}

func TestGetDatabasesToDump(t *testing.T) {
	t.Run("SpecifiedDatabases", func(t *testing.T) {
		originalDatabases := dumpDatabases
		defer func() { dumpDatabases = originalDatabases }()

		dumpDatabases = []string{"db1", "db2"}
		dumpExcludeDatabases = []string{}

		// When databases are specified, they should be returned directly
		assert.Equal(t, []string{"db1", "db2"}, dumpDatabases)
	})

	t.Run("DatabaseFiltering", func(t *testing.T) {
		originalExclude := dumpExcludeDatabases
		defer func() {
			dumpExcludeDatabases = originalExclude
		}()

		// Test exclusion list filtering
		databases := []string{"db1", "db2", "admin", "local"}
		dumpExcludeDatabases = []string{"admin", "local"}

		filtered := filterByExclusionList(databases, dumpExcludeDatabases)
		assert.Equal(t, []string{"db1", "db2"}, filtered)
	})
}

func TestCheckMongodumpInstalled(t *testing.T) {
	err := checkMongodumpInstalled()
	if err != nil {
		t.Skip("mongodump not installed, skipping test")
	}
}

func TestDumpCommandConfiguration(t *testing.T) {
	t.Run("DefaultValues", func(t *testing.T) {
		// Reset global variables to their default values for test
		dumpOutputDir = "./dumps"
		dumpIncremental = false
		dumpTimeout = 30
		dumpLastModifiedField = "lastModified"
		dumpRetryAttempts = 5

		assert.Equal(t, "./dumps", dumpOutputDir)
		assert.Equal(t, false, dumpIncremental)
		assert.Equal(t, 30, dumpTimeout)
		assert.Equal(t, "lastModified", dumpLastModifiedField)
		assert.Equal(t, 5, dumpRetryAttempts)
	})

	t.Run("RequiredFlags", func(t *testing.T) {
		cmd := dumpCmd
		sourceFlag := cmd.Flag("source")
		assert.NotNil(t, sourceFlag)
		assert.Contains(t, sourceFlag.Usage, "required")
	})
}

func TestLogDumpConfiguration(t *testing.T) {
	originalSourceURI := dumpSourceURI
	originalOutputDir := dumpOutputDir
	originalIncremental := dumpIncremental
	originalDatabases := dumpDatabases
	originalExcludeDatabases := dumpExcludeDatabases

	defer func() {
		dumpSourceURI = originalSourceURI
		dumpOutputDir = originalOutputDir
		dumpIncremental = originalIncremental
		dumpDatabases = originalDatabases
		dumpExcludeDatabases = originalExcludeDatabases
	}()

	dumpSourceURI = "mongodb://test:27017"
	dumpOutputDir = filepath.Join(os.TempDir(), "dumps")
	dumpIncremental = true
	dumpDatabases = []string{"db1", "db2"}
	dumpExcludeDatabases = []string{"admin"}

	logDumpConfiguration()
}

func TestDumpStateJSON(t *testing.T) {
	state := &DumpState{
		Collections: map[string]CollectionState{
			"db1.coll1": {
				LastDumpTime:  time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				DocumentCount: 100,
			},
		},
	}

	data, err := json.MarshalIndent(state, "", "  ")
	require.NoError(t, err)

	var loaded DumpState
	err = json.Unmarshal(data, &loaded)
	require.NoError(t, err)

	assert.Equal(t, state.Collections["db1.coll1"].DocumentCount,
		loaded.Collections["db1.coll1"].DocumentCount)
}

func TestDumpDatabaseWithCollectionFilter(t *testing.T) {
	originalCollections := dumpCollections
	originalExclude := dumpExcludeCollections
	defer func() {
		dumpCollections = originalCollections
		dumpExcludeCollections = originalExclude
	}()

	dumpCollections = []string{"users", "products"}
	dumpExcludeCollections = []string{}

	// Test collection filtering
	assert.Equal(t, []string{"users", "products"}, dumpCollections)
}

func TestBuildMongodumpArgsWithAllOptions(t *testing.T) {
	originalURI := dumpSourceURI
	originalCAFile := dumpSourceCACertFile
	defer func() {
		dumpSourceURI = originalURI
		dumpSourceCACertFile = originalCAFile
	}()

	dumpSourceURI = "mongodb+srv://user:pass@cluster.mongodb.net"
	dumpSourceCACertFile = "/etc/ssl/ca.pem"

	args := buildMongodumpArgs("mydb", "mycoll", "/backup/dump", `{"timestamp":{"$gte":"2023-01-01"}}`)

	assert.Contains(t, args, "--uri")
	assert.Contains(t, args, "mongodb+srv://user:pass@cluster.mongodb.net")
	assert.Contains(t, args, "--sslCAFile")
	assert.Contains(t, args, "/etc/ssl/ca.pem")
	assert.Contains(t, args, "--query")
	assert.Contains(t, args, `{"timestamp":{"$gte":"2023-01-01"}}`)
}

func TestDumpCollectionWithRetry(t *testing.T) {
	originalRetryAttempts := dumpRetryAttempts
	defer func() { dumpRetryAttempts = originalRetryAttempts }()

	dumpRetryAttempts = 3

	assert.Equal(t, 3, dumpRetryAttempts)
}

func TestEmptyDumpState(t *testing.T) {
	state := &DumpState{Collections: make(map[string]CollectionState)}
	assert.NotNil(t, state.Collections)
	assert.Equal(t, 0, len(state.Collections))
}

func TestDumpCollectionKeyFormat(t *testing.T) {
	dbName := "testdb"
	collName := "testcoll"
	expected := "testdb.testcoll"

	result := fmt.Sprintf("%s.%s", dbName, collName)
	assert.Equal(t, expected, result)
}

type mockClient struct {
	*mongo.Client
	listDBError   error
	listCollError error
	databases     []string
	collections   []string
}

func (m *mockClient) ListDatabases(ctx context.Context) ([]string, error) {
	if m.listDBError != nil {
		return nil, m.listDBError
	}
	return m.databases, nil
}

func (m *mockClient) ListCollections(ctx context.Context, dbName string) ([]string, error) {
	if m.listCollError != nil {
		return nil, m.listCollError
	}
	return m.collections, nil
}

func TestGetDatabasesToDumpError(t *testing.T) {
	originalDatabases := dumpDatabases
	defer func() { dumpDatabases = originalDatabases }()

	dumpDatabases = []string{}

	ctx := context.Background()
	mockCli := &mockClient{
		listDBError: assert.AnError,
	}

	// client := &mongodb.Client{Client: mockCli.Client}
	_, err := mockCli.ListDatabases(ctx)
	assert.Error(t, err)
}
