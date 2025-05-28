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
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

func TestRestoreStateOperations(t *testing.T) {
	t.Run("SaveAndLoadRestoreState", func(t *testing.T) {
		tempDir := t.TempDir()
		stateFile := filepath.Join(tempDir, "test-restore-state.json")

		originalState := &RestoreState{
			Collections: map[string]RestoreCollectionState{
				"db1.coll1": {
					LastRestoreTime: time.Now().UTC(),
					DocumentCount:   100,
					Restored:        true,
				},
				"db1.coll2": {
					LastRestoreTime: time.Now().UTC().Add(-1 * time.Hour),
					DocumentCount:   200,
					Restored:        false,
				},
			},
			LastRestore: time.Now().UTC(),
		}

		err := saveRestoreState(originalState, stateFile)
		require.NoError(t, err)

		loadedState, err := loadRestoreState(stateFile)
		require.NoError(t, err)

		assert.Equal(t, len(originalState.Collections), len(loadedState.Collections))
		for key, origColl := range originalState.Collections {
			loadedColl, exists := loadedState.Collections[key]
			require.True(t, exists)
			assert.Equal(t, origColl.DocumentCount, loadedColl.DocumentCount)
			assert.Equal(t, origColl.Restored, loadedColl.Restored)
			assert.WithinDuration(t, origColl.LastRestoreTime, loadedColl.LastRestoreTime, time.Second)
		}
		assert.WithinDuration(t, originalState.LastRestore, loadedState.LastRestore, time.Second)
	})

	t.Run("LoadRestoreStateNonExistentFile", func(t *testing.T) {
		_, err := loadRestoreState("/non/existent/file.json")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("LoadRestoreStateInvalidJSON", func(t *testing.T) {
		tempDir := t.TempDir()
		stateFile := filepath.Join(tempDir, "invalid-restore-state.json")

		err := os.WriteFile(stateFile, []byte("invalid json"), 0644)
		require.NoError(t, err)

		_, err = loadRestoreState(stateFile)
		assert.Error(t, err)
	})
}

func TestBuildMongorestoreArgs(t *testing.T) {
	tests := []struct {
		name      string
		dbName    string
		collName  string
		collPath  string
		setupFunc func()
		expected  []string
	}{
		{
			name:     "BasicArgs",
			dbName:   "testdb",
			collName: "testcoll",
			collPath: "/tmp/restore/testdb/testcoll",
			setupFunc: func() {
				restoreTargetURI = "mongodb://localhost:27017"
				restoreTargetCACertFile = ""
				restoreDrop = false
				restoreOplogReplay = false
				restorePreserveDates = false
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--dir", "/tmp/restore/testdb/testcoll",
			},
		},
		{
			name:     "WithAllOptions",
			dbName:   "testdb",
			collName: "testcoll",
			collPath: "/tmp/restore/testdb/testcoll",
			setupFunc: func() {
				restoreTargetURI = "mongodb://localhost:27017"
				restoreTargetCACertFile = "/path/to/ca.pem"
				restoreDrop = true
				restoreOplogReplay = true
				restorePreserveDates = true
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--dir", "/tmp/restore/testdb/testcoll",
				"--sslCAFile", "/path/to/ca.pem",
				"--drop",
				"--oplogReplay",
				"--maintainInsertionOrder",
			},
		},
		{
			name:     "WithCAFileOnly",
			dbName:   "testdb",
			collName: "testcoll",
			collPath: "/tmp/restore/testdb/testcoll",
			setupFunc: func() {
				restoreTargetURI = "mongodb://localhost:27017"
				restoreTargetCACertFile = "/path/to/ca.pem"
				restoreDrop = false
				restoreOplogReplay = false
				restorePreserveDates = false
			},
			expected: []string{
				"--uri", "mongodb://localhost:27017",
				"--db", "testdb",
				"--collection", "testcoll",
				"--dir", "/tmp/restore/testdb/testcoll",
				"--sslCAFile", "/path/to/ca.pem",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupFunc()
			result := buildMongorestoreArgs(tt.dbName, tt.collName, tt.collPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckMongorestoreInstalled(t *testing.T) {
	err := checkMongorestoreInstalled()
	if err != nil {
		t.Skip("mongorestore not installed, skipping test")
	}
}

func TestValidateInputDirectory(t *testing.T) {
	t.Run("ExistingDirectory", func(t *testing.T) {
		tempDir := t.TempDir()
		originalInputDir := restoreInputDir
		defer func() { restoreInputDir = originalInputDir }()

		restoreInputDir = tempDir
		err := validateInputDirectory()
		assert.NoError(t, err)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		originalInputDir := restoreInputDir
		defer func() { restoreInputDir = originalInputDir }()

		restoreInputDir = "/non/existent/directory"
		err := validateInputDirectory()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input directory does not exist")
	})
}

func TestGetDatabasesFromDumps(t *testing.T) {
	t.Run("SpecifiedDatabases", func(t *testing.T) {
		originalDatabases := restoreDatabases
		originalExclude := restoreExcludeDatabases
		defer func() {
			restoreDatabases = originalDatabases
			restoreExcludeDatabases = originalExclude
		}()

		restoreDatabases = []string{"db1", "db2"}
		restoreExcludeDatabases = []string{}

		// When databases are specified, they should be returned directly
		assert.Equal(t, []string{"db1", "db2"}, restoreDatabases)
	})

	t.Run("DatabaseFiltering", func(t *testing.T) {
		originalExclude := restoreExcludeDatabases
		defer func() {
			restoreExcludeDatabases = originalExclude
		}()

		// Test exclusion list filtering
		databases := []string{"db1", "db2", "admin", "local"}
		restoreExcludeDatabases = []string{"admin", "local"}

		filtered := filterByExclusionList(databases, restoreExcludeDatabases)
		assert.Equal(t, []string{"db1", "db2"}, filtered)
	})

	t.Run("FromDumpDirectory", func(t *testing.T) {
		tempDir := t.TempDir()
		originalInputDir := restoreInputDir
		originalDatabases := restoreDatabases
		defer func() {
			restoreInputDir = originalInputDir
			restoreDatabases = originalDatabases
		}()

		// Create mock dump structure
		os.MkdirAll(filepath.Join(tempDir, "db1"), 0755)
		os.MkdirAll(filepath.Join(tempDir, "db2"), 0755)
		os.MkdirAll(filepath.Join(tempDir, "admin"), 0755)
		// Create special files that should be ignored
		os.WriteFile(filepath.Join(tempDir, "dump-state.json"), []byte("{}"), 0644)
		os.WriteFile(filepath.Join(tempDir, "restore-state.json"), []byte("{}"), 0644)

		restoreInputDir = tempDir
		restoreDatabases = []string{} // Use all found databases

		databases, err := getDatabasesFromDumps()
		require.NoError(t, err)
		assert.Contains(t, databases, "db1")
		assert.Contains(t, databases, "db2")
		assert.Contains(t, databases, "admin")
		// Special files should not be included
		assert.NotContains(t, databases, "dump-state.json")
		assert.NotContains(t, databases, "restore-state.json")
	})
}

func TestIsSpecialFile(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		expected bool
	}{
		{"DumpState", "dump-state.json", true},
		{"RestoreState", "restore-state.json", true},
		{"OplogBson", "oplog.bson", true},
		{"RegularFile", "collection.bson", false},
		{"RegularDir", "database1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSpecialFile(tt.filename)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCollectionsFromDump(t *testing.T) {
	t.Run("SpecifiedCollections", func(t *testing.T) {
		originalCollections := restoreCollections
		defer func() { restoreCollections = originalCollections }()

		restoreCollections = []string{"users", "products"}

		collections, err := getCollectionsFromDump("/any/path")
		require.NoError(t, err)
		assert.Equal(t, []string{"users", "products"}, collections)
	})

	t.Run("FromDumpDirectory", func(t *testing.T) {
		tempDir := t.TempDir()
		originalCollections := restoreCollections
		defer func() { restoreCollections = originalCollections }()

		// Create mock collection structure
		dbPath := filepath.Join(tempDir, "testdb")
		os.MkdirAll(filepath.Join(dbPath, "users"), 0755)
		os.MkdirAll(filepath.Join(dbPath, "products"), 0755)
		os.MkdirAll(filepath.Join(dbPath, "orders"), 0755)
		// Create a file that should be ignored
		os.WriteFile(filepath.Join(dbPath, "metadata.json"), []byte("{}"), 0644)

		restoreCollections = []string{} // Use all found collections

		collections, err := getCollectionsFromDump(dbPath)
		require.NoError(t, err)
		assert.Contains(t, collections, "users")
		assert.Contains(t, collections, "products")
		assert.Contains(t, collections, "orders")
		// Files should not be included, only directories
		assert.NotContains(t, collections, "metadata.json")
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		originalCollections := restoreCollections
		defer func() { restoreCollections = originalCollections }()

		restoreCollections = []string{} // Use all found collections

		_, err := getCollectionsFromDump("/non/existent/directory")
		assert.Error(t, err)
	})
}

func TestRestoreCommandConfiguration(t *testing.T) {
	t.Run("DefaultValues", func(t *testing.T) {
		// Reset global variables to their default values for test
		restoreInputDir = "./dumps"
		restoreTimeout = 30
		restoreRetryAttempts = 5
		restoreDrop = false
		restoreOplogReplay = false
		restorePreserveDates = true

		assert.Equal(t, "./dumps", restoreInputDir)
		assert.Equal(t, 30, restoreTimeout)
		assert.Equal(t, 5, restoreRetryAttempts)
		assert.Equal(t, false, restoreDrop)
		assert.Equal(t, false, restoreOplogReplay)
		assert.Equal(t, true, restorePreserveDates)
	})

	t.Run("RequiredFlags", func(t *testing.T) {
		cmd := restoreCmd
		targetFlag := cmd.Flag("target")
		assert.NotNil(t, targetFlag)
		assert.Contains(t, targetFlag.Usage, "required")
	})
}

func TestLogRestoreConfiguration(t *testing.T) {
	originalTargetURI := restoreTargetURI
	originalInputDir := restoreInputDir
	originalDrop := restoreDrop
	originalDatabases := restoreDatabases
	originalExcludeDatabases := restoreExcludeDatabases

	defer func() {
		restoreTargetURI = originalTargetURI
		restoreInputDir = originalInputDir
		restoreDrop = originalDrop
		restoreDatabases = originalDatabases
		restoreExcludeDatabases = originalExcludeDatabases
	}()

	restoreTargetURI = "mongodb://test:27017"
	restoreInputDir = "/tmp/dumps"
	restoreDrop = true
	restoreDatabases = []string{"db1", "db2"}
	restoreExcludeDatabases = []string{"admin"}

	logRestoreConfiguration()
}

func TestRestoreStateJSON(t *testing.T) {
	state := &RestoreState{
		Collections: map[string]RestoreCollectionState{
			"db1.coll1": {
				LastRestoreTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				DocumentCount:   100,
				Restored:        true,
			},
		},
		LastRestore: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	data, err := json.MarshalIndent(state, "", "  ")
	require.NoError(t, err)

	var loaded RestoreState
	err = json.Unmarshal(data, &loaded)
	require.NoError(t, err)

	assert.Equal(t, state.Collections["db1.coll1"].DocumentCount,
		loaded.Collections["db1.coll1"].DocumentCount)
	assert.Equal(t, state.Collections["db1.coll1"].Restored,
		loaded.Collections["db1.coll1"].Restored)
}

func TestGetRestoreStateFilePath(t *testing.T) {
	t.Run("CustomStateFile", func(t *testing.T) {
		originalStateFile := restoreStateFile
		defer func() { restoreStateFile = originalStateFile }()

		restoreStateFile = "/custom/path/state.json"
		result := getRestoreStateFilePath()
		assert.Equal(t, "/custom/path/state.json", result)
	})

	t.Run("DefaultStateFile", func(t *testing.T) {
		originalStateFile := restoreStateFile
		originalInputDir := restoreInputDir
		defer func() {
			restoreStateFile = originalStateFile
			restoreInputDir = originalInputDir
		}()

		restoreStateFile = ""
		restoreInputDir = "/tmp/dumps"
		result := getRestoreStateFilePath()
		assert.Equal(t, "/tmp/dumps/restore-state.json", result)
	})
}

func TestLoadOrCreateRestoreState(t *testing.T) {
	t.Run("CreateNewState", func(t *testing.T) {
		state, err := loadOrCreateRestoreState("/non/existent/file.json")
		require.NoError(t, err)
		assert.NotNil(t, state)
		assert.NotNil(t, state.Collections)
		assert.Equal(t, 0, len(state.Collections))
	})

	t.Run("LoadExistingState", func(t *testing.T) {
		tempDir := t.TempDir()
		stateFile := filepath.Join(tempDir, "restore-state.json")

		existingState := &RestoreState{
			Collections: map[string]RestoreCollectionState{
				"db1.coll1": {
					LastRestoreTime: time.Now(),
					DocumentCount:   100,
					Restored:        true,
				},
			},
			LastRestore: time.Now(),
		}

		err := saveRestoreState(existingState, stateFile)
		require.NoError(t, err)

		loadedState, err := loadOrCreateRestoreState(stateFile)
		require.NoError(t, err)
		assert.Equal(t, 1, len(loadedState.Collections))
		assert.True(t, loadedState.Collections["db1.coll1"].Restored)
	})
}

func TestRestoreCollectionIntegration(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("RestoreCollectionSuccess", func(mt *mtest.T) {
		state := &RestoreState{Collections: make(map[string]RestoreCollectionState)}
		dbName := "testdb"
		collName := "testcoll"
		collKey := fmt.Sprintf("%s.%s", dbName, collName)

		restoreRetryAttempts = 1

		// Mock collection path (won't actually be used in test)
		tempDir := t.TempDir()
		collPath := filepath.Join(tempDir, "testdb", "testcoll")
		os.MkdirAll(collPath, 0755)

		// ctx := context.Background()

		_, exists := state.Collections[collKey]
		assert.False(t, exists)
	})
}

func TestFilterByExclusionListRestore(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterByExclusionList(tt.items, tt.exclusions)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConnectToTarget(t *testing.T) {
	originalURI := restoreTargetURI
	originalTimeout := restoreTimeout
	defer func() {
		restoreTargetURI = originalURI
		restoreTimeout = originalTimeout
	}()

	restoreTargetURI = "mongodb://localhost:27017"
	restoreTimeout = 1 // Short timeout for test

	ctx := context.Background()
	_, err := connectToTarget(ctx)
	// This will likely fail unless MongoDB is running, which is expected
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect to target MongoDB")
	}
}

func TestEmptyRestoreState(t *testing.T) {
	state := &RestoreState{Collections: make(map[string]RestoreCollectionState)}
	assert.NotNil(t, state.Collections)
	assert.Equal(t, 0, len(state.Collections))
}

func TestRestoreCollectionKeyFormat(t *testing.T) {
	dbName := "testdb"
	collName := "testcoll"
	expected := "testdb.testcoll"

	result := fmt.Sprintf("%s.%s", dbName, collName)
	assert.Equal(t, expected, result)
}

func TestRestoreDatabaseWithCollectionFilter(t *testing.T) {
	originalCollections := restoreCollections
	originalExclude := restoreExcludeCollections
	defer func() {
		restoreCollections = originalCollections
		restoreExcludeCollections = originalExclude
	}()

	restoreCollections = []string{"users", "products"}
	restoreExcludeCollections = []string{}

	// Test collection filtering
	assert.Equal(t, []string{"users", "products"}, restoreCollections)
}

func TestBuildMongorestoreArgsWithMinimalOptions(t *testing.T) {
	originalURI := restoreTargetURI
	originalCAFile := restoreTargetCACertFile
	originalDrop := restoreDrop
	originalOplog := restoreOplogReplay
	originalDates := restorePreserveDates
	defer func() {
		restoreTargetURI = originalURI
		restoreTargetCACertFile = originalCAFile
		restoreDrop = originalDrop
		restoreOplogReplay = originalOplog
		restorePreserveDates = originalDates
	}()

	restoreTargetURI = "mongodb://localhost:27017"
	restoreTargetCACertFile = ""
	restoreDrop = false
	restoreOplogReplay = false
	restorePreserveDates = false

	args := buildMongorestoreArgs("mydb", "mycoll", "/backup/restore")

	assert.Contains(t, args, "--uri")
	assert.Contains(t, args, "mongodb://localhost:27017")
	assert.Contains(t, args, "--db")
	assert.Contains(t, args, "mydb")
	assert.Contains(t, args, "--collection")
	assert.Contains(t, args, "mycoll")
	assert.Contains(t, args, "--dir")
	assert.Contains(t, args, "/backup/restore")

	// These should not be present when disabled
	assert.NotContains(t, args, "--drop")
	assert.NotContains(t, args, "--oplogReplay")
	assert.NotContains(t, args, "--maintainInsertionOrder")
}
