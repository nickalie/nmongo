package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"

	"nmongo/internal/config"
	"nmongo/internal/mongodb"
)

var (
	dumpSourceURI          string
	dumpSourceCACertFile   string
	dumpOutputDir          string
	dumpIncremental        bool
	dumpTimeout            int
	dumpDatabases          []string
	dumpCollections        []string
	dumpExcludeDatabases   []string
	dumpExcludeCollections []string
	dumpLastModifiedField  string
	dumpRetryAttempts      int
	dumpStateFile          string
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Create incremental dumps of MongoDB databases",
	Long: `Create incremental dumps of MongoDB databases using the mongodump CLI tool.
Supports incremental dumping by tracking the last dump timestamp for each collection.

Examples:
  nmongo dump --source "mongodb://host:27017" --output ./dumps --incremental
  nmongo dump --source "mongodb://host:27017" --output ./dumps --databases "db1,db2"
  nmongo dump --source "mongodb://host:27017" --output ./dumps --exclude-databases "admin,local,config"`,
	Run: func(cmd *cobra.Command, args []string) {
		if configFile != "" {
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				log.Fatalf("Error loading configuration: %v", err)
			}

			if dumpSourceURI == "" {
				dumpSourceURI = cfg.SourceURI
			}
			if dumpSourceCACertFile == "" {
				dumpSourceCACertFile = cfg.SourceCACertFile
			}
			if !cmd.Flags().Changed("incremental") {
				dumpIncremental = cfg.Incremental
			}
			if !cmd.Flags().Changed("timeout") {
				dumpTimeout = cfg.Timeout
			}
			if len(dumpDatabases) == 0 {
				dumpDatabases = cfg.Databases
			}
			if len(dumpCollections) == 0 {
				dumpCollections = cfg.Collections
			}
			if len(dumpExcludeDatabases) == 0 {
				dumpExcludeDatabases = cfg.ExcludeDatabases
			}
			if len(dumpExcludeCollections) == 0 {
				dumpExcludeCollections = cfg.ExcludeCollections
			}
			if !cmd.Flags().Changed("last-modified-field") {
				dumpLastModifiedField = cfg.LastModifiedField
			}
			if !cmd.Flags().Changed("retry-attempts") && cfg.RetryAttempts > 0 {
				dumpRetryAttempts = cfg.RetryAttempts
			}
		}

		if saveConfig {
			configPath, err := config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}

			cfg := &config.Config{
				SourceURI:          dumpSourceURI,
				SourceCACertFile:   dumpSourceCACertFile,
				Incremental:        dumpIncremental,
				Timeout:            dumpTimeout,
				Databases:          dumpDatabases,
				Collections:        dumpCollections,
				ExcludeDatabases:   dumpExcludeDatabases,
				ExcludeCollections: dumpExcludeCollections,
				LastModifiedField:  dumpLastModifiedField,
				RetryAttempts:      dumpRetryAttempts,
			}

			if err := config.SaveConfig(cfg, configPath); err != nil {
				log.Fatalf("Error saving configuration: %v", err)
			}

			log.Printf("Configuration saved to %s", configPath)
		}

		if err := runDump(); err != nil {
			log.Fatalf("Error executing dump command: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(dumpCmd)
	dumpCmd.Flags().StringVar(&dumpSourceURI, "source", "", "Source MongoDB connection string (required)")
	dumpCmd.Flags().StringVar(&dumpSourceCACertFile, "source-ca-cert-file", "",
		"Path to CA certificate file for source MongoDB TLS connections")
	dumpCmd.Flags().StringVar(&dumpOutputDir, "output", "./dumps", "Output directory for dump files")
	dumpCmd.Flags().BoolVar(&dumpIncremental, "incremental", false, "Perform incremental dump (only dump new or updated documents)")
	dumpCmd.Flags().IntVar(&dumpTimeout, "timeout", 30, "Connection timeout in seconds")
	dumpCmd.Flags().StringSliceVar(&dumpDatabases, "databases", []string{}, "List of databases to dump (empty means all)")
	dumpCmd.Flags().StringSliceVar(&dumpCollections, "collections", []string{}, "List of collections to dump (empty means all)")
	dumpCmd.Flags().StringSliceVar(&dumpExcludeDatabases, "exclude-databases", []string{}, "List of databases to exclude from dump")
	dumpCmd.Flags().StringSliceVar(&dumpExcludeCollections, "exclude-collections", []string{}, "List of collections to exclude from dump")
	dumpCmd.Flags().StringVar(&dumpLastModifiedField, "last-modified-field", "lastModified",
		"Field name to use for tracking document modifications in incremental dump")
	dumpCmd.Flags().IntVar(&dumpRetryAttempts, "retry-attempts", 5, "Number of retry attempts for failed operations")
	dumpCmd.Flags().StringVar(&dumpStateFile, "state-file", "",
		"Path to state file for tracking dump progress (defaults to <output>/dump-state.json)")

	dumpCmd.MarkFlagRequired("source")
}

// DumpState tracks the state of dumps for incremental operations
type DumpState struct {
	Collections map[string]CollectionState `json:"collections"`
}

// CollectionState tracks the state of a single collection dump
type CollectionState struct {
	LastDumpTime  time.Time `json:"lastDumpTime"`
	DocumentCount int64     `json:"documentCount"`
}

func runDump() error {
	logDumpConfiguration()

	if err := checkMongodumpInstalled(); err != nil {
		return err
	}

	ctx := context.Background()

	sourceClient, err := connectToSource(ctx)
	if err != nil {
		return err
	}
	defer sourceClient.Disconnect(ctx)

	if err := os.MkdirAll(dumpOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	stateFilePath := getStateFilePath()
	state, err := loadOrCreateDumpState(stateFilePath)
	if err != nil {
		return err
	}

	if err := performDump(ctx, sourceClient, state); err != nil {
		return err
	}

	if err := saveDumpState(state, stateFilePath); err != nil {
		return fmt.Errorf("failed to save dump state: %w", err)
	}

	fmt.Println("MongoDB dump operation completed successfully")
	return nil
}

func connectToSource(ctx context.Context) (*mongodb.Client, error) {
	connCtx, connCancel := context.WithTimeout(ctx, time.Duration(dumpTimeout)*time.Second)
	defer connCancel()

	sourceClient, err := mongodb.NewClient(connCtx, dumpSourceURI, dumpSourceCACertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}
	return sourceClient, nil
}

func getStateFilePath() string {
	if dumpStateFile != "" {
		return dumpStateFile
	}
	return filepath.Join(dumpOutputDir, "dump-state.json")
}

func loadOrCreateDumpState(stateFilePath string) (*DumpState, error) {
	state, err := loadDumpState(stateFilePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load dump state: %w", err)
	}
	if state == nil {
		state = &DumpState{Collections: make(map[string]CollectionState)}
	}
	return state, nil
}

func performDump(ctx context.Context, sourceClient *mongodb.Client, state *DumpState) error {
	dbsToDump, err := getDatabasesToDump(ctx, sourceClient)
	if err != nil {
		return err
	}

	for _, dbName := range dbsToDump {
		if err := dumpDatabase(ctx, sourceClient, dbName, state); err != nil {
			return fmt.Errorf("failed to dump database %s: %w", dbName, err)
		}
	}
	return nil
}

func checkMongodumpInstalled() error {
	cmd := exec.Command("mongodump", "--version")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongodump is not installed or not in PATH: %w", err)
	}
	return nil
}

func logDumpConfiguration() {
	logBasicDumpConfig()
	logIncrementalDumpConfig()
	logFilterDumpConfig()
}

func logBasicDumpConfig() {
	fmt.Println("Starting MongoDB dump operation")
	fmt.Printf("Source: %s\n", dumpSourceURI)
	fmt.Printf("Output directory: %s\n", dumpOutputDir)
	if dumpSourceCACertFile != "" {
		fmt.Printf("Source CA Certificate File: %s\n", dumpSourceCACertFile)
	}
	fmt.Printf("Connection timeout: %d seconds\n", dumpTimeout)
	fmt.Printf("Retry attempts: %d\n", dumpRetryAttempts)
}

func logIncrementalDumpConfig() {
	fmt.Printf("Incremental mode: %v\n", dumpIncremental)
	if dumpIncremental && dumpLastModifiedField != "" {
		fmt.Printf("Last modified field: %s\n", dumpLastModifiedField)
	}
}

func logFilterDumpConfig() {
	if len(dumpDatabases) > 0 {
		fmt.Printf("Included databases: %v\n", dumpDatabases)
	}
	if len(dumpCollections) > 0 {
		fmt.Printf("Included collections: %v\n", dumpCollections)
	}
	if len(dumpExcludeDatabases) > 0 {
		fmt.Printf("Excluded databases: %v\n", dumpExcludeDatabases)
	}
	if len(dumpExcludeCollections) > 0 {
		fmt.Printf("Excluded collections: %v\n", dumpExcludeCollections)
	}
}

func getDatabasesToDump(ctx context.Context, sourceClient *mongodb.Client) ([]string, error) {
	var dbsToDump []string
	var err error

	if len(dumpDatabases) > 0 {
		dbsToDump = dumpDatabases
		fmt.Printf("Using specified databases: %v\n", dbsToDump)
	} else {
		dbsToDump, err = sourceClient.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get databases: %w", err)
		}
		fmt.Printf("Found %d databases in source\n", len(dbsToDump))
	}

	originalCount := len(dbsToDump)
	if len(dumpExcludeDatabases) > 0 {
		dbsToDump = filterByExclusionList(dbsToDump, dumpExcludeDatabases)
		fmt.Printf("Filtered out %d databases, %d remaining\n", originalCount-len(dbsToDump), len(dbsToDump))
	}

	return dbsToDump, nil
}

func dumpDatabase(ctx context.Context, sourceClient *mongodb.Client, dbName string, state *DumpState) error {
	fmt.Printf("Dumping database: %s\n", dbName)

	var collsToDump []string
	if len(dumpCollections) > 0 {
		collsToDump = dumpCollections
		fmt.Printf("  Using specified collections: %v\n", collsToDump)
	} else {
		var err error
		collsToDump, err = sourceClient.ListCollections(ctx, dbName)
		if err != nil {
			return fmt.Errorf("failed to get collections for database %s: %w", dbName, err)
		}
		fmt.Printf("  Found %d collections in database %s\n", len(collsToDump), dbName)
	}

	originalCount := len(collsToDump)
	if len(dumpExcludeCollections) > 0 {
		collsToDump = filterByExclusionList(collsToDump, dumpExcludeCollections)
		if originalCount != len(collsToDump) {
			fmt.Printf("  Filtered out %d collections, %d remaining\n", originalCount-len(collsToDump), len(collsToDump))
		}
	}

	fmt.Printf("  Dumping %d collections in database %s\n", len(collsToDump), dbName)
	for _, collName := range collsToDump {
		fmt.Printf("    Dumping collection: %s.%s\n", dbName, collName)
		if err := dumpCollection(ctx, sourceClient, dbName, collName, state); err != nil {
			return fmt.Errorf("failed to dump collection %s.%s: %w", dbName, collName, err)
		}
	}
	return nil
}

func dumpCollection(ctx context.Context, sourceClient *mongodb.Client, dbName, collName string, state *DumpState) error {
	collKey := fmt.Sprintf("%s.%s", dbName, collName)

	query, err := buildIncrementalQuery(collKey, state)
	if err != nil {
		return err
	}

	outputPath := filepath.Join(dumpOutputDir, dbName, collName)
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	if err := executeDumpWithRetry(dbName, collName, outputPath, query); err != nil {
		return err
	}

	return updateCollectionState(ctx, sourceClient, dbName, collName, collKey, state)
}

func buildIncrementalQuery(collKey string, state *DumpState) (string, error) {
	collState, exists := state.Collections[collKey]
	if !dumpIncremental || !exists || collState.LastDumpTime.IsZero() {
		return "", nil
	}

	queryDoc := bson.M{
		dumpLastModifiedField: bson.M{"$gt": collState.LastDumpTime},
	}
	queryBytes, err := json.Marshal(queryDoc)
	if err != nil {
		return "", fmt.Errorf("failed to create query: %w", err)
	}
	query := string(queryBytes)
	fmt.Printf("      Using incremental query: %s\n", query)
	return query, nil
}

func executeDumpWithRetry(dbName, collName, outputPath, query string) error {
	args := buildMongodumpArgs(dbName, collName, outputPath, query)

	var lastErr error
	for attempt := 1; attempt <= dumpRetryAttempts; attempt++ {
		if attempt > 1 {
			fmt.Printf("      Retry attempt %d/%d for %s.%s\n", attempt, dumpRetryAttempts, dbName, collName)
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		cmd := exec.Command("mongodump", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return fmt.Errorf("failed after %d attempts: %w", dumpRetryAttempts, lastErr)
}

func updateCollectionState(ctx context.Context, sourceClient *mongodb.Client, dbName, collName, collKey string, state *DumpState) error {
	db := sourceClient.GetDatabase(dbName)
	coll := db.Collection(collName)
	count, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("failed to count documents: %w", err)
	}

	state.Collections[collKey] = CollectionState{
		LastDumpTime:  time.Now(),
		DocumentCount: count,
	}

	fmt.Printf("      Successfully dumped %s.%s (%d documents)\n", dbName, collName, count)
	return nil
}

func buildMongodumpArgs(dbName, collName, outputPath, query string) []string {
	args := []string{
		"--uri", dumpSourceURI,
		"--db", dbName,
		"--collection", collName,
		"--out", outputPath,
	}

	if dumpSourceCACertFile != "" {
		args = append(args, "--sslCAFile", dumpSourceCACertFile)
	}

	if query != "" {
		args = append(args, "--query", query)
	}

	return args
}

func loadDumpState(filePath string) (*DumpState, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var state DumpState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func saveDumpState(state *DumpState, filePath string) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}
