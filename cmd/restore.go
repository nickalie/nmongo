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

	"nmongo/internal/config"
	"nmongo/internal/mongodb"
)

var (
	restoreTargetURI          string
	restoreTargetCACertFile   string
	restoreInputDir           string
	restoreTimeout            int
	restoreDatabases          []string
	restoreCollections        []string
	restoreExcludeDatabases   []string
	restoreExcludeCollections []string
	restoreRetryAttempts      int
	restoreStateFile          string
	restoreDrop               bool
	restoreOplogReplay        bool
	restorePreserveDates      bool
)

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore MongoDB databases from dumps created by the dump command",
	Long: `Restore MongoDB databases from dumps created by the dump command using mongorestore CLI tool.
Supports restoring from incremental dumps with proper state tracking.

Examples:
  nmongo restore --target "mongodb://host:27017" --input ./dumps
  nmongo restore --target "mongodb://host:27017" --input ./dumps --databases "db1,db2"
  nmongo restore --target "mongodb://host:27017" --input ./dumps --drop`,
	Run: func(cmd *cobra.Command, args []string) {
		if configFile != "" {
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				log.Fatalf("Error loading configuration: %v", err)
			}

			if restoreTargetURI == "" {
				restoreTargetURI = cfg.TargetURI
			}
			if restoreTargetCACertFile == "" {
				restoreTargetCACertFile = cfg.TargetCACertFile
			}
			if !cmd.Flags().Changed("timeout") {
				restoreTimeout = cfg.Timeout
			}
			if len(restoreDatabases) == 0 {
				restoreDatabases = cfg.Databases
			}
			if len(restoreCollections) == 0 {
				restoreCollections = cfg.Collections
			}
			if len(restoreExcludeDatabases) == 0 {
				restoreExcludeDatabases = cfg.ExcludeDatabases
			}
			if len(restoreExcludeCollections) == 0 {
				restoreExcludeCollections = cfg.ExcludeCollections
			}
			if !cmd.Flags().Changed("retry-attempts") && cfg.RetryAttempts > 0 {
				restoreRetryAttempts = cfg.RetryAttempts
			}
		}

		if saveConfig {
			configPath, err := config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}

			cfg := &config.Config{
				TargetURI:          restoreTargetURI,
				TargetCACertFile:   restoreTargetCACertFile,
				Timeout:            restoreTimeout,
				Databases:          restoreDatabases,
				Collections:        restoreCollections,
				ExcludeDatabases:   restoreExcludeDatabases,
				ExcludeCollections: restoreExcludeCollections,
				RetryAttempts:      restoreRetryAttempts,
			}

			if err := config.SaveConfig(cfg, configPath); err != nil {
				log.Fatalf("Error saving configuration: %v", err)
			}

			log.Printf("Configuration saved to %s", configPath)
		}

		if err := runRestore(); err != nil {
			log.Fatalf("Error executing restore command: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)
	restoreCmd.Flags().StringVar(&restoreTargetURI, "target", "", "Target MongoDB connection string (required)")
	restoreCmd.Flags().StringVar(&restoreTargetCACertFile, "target-ca-cert-file", "",
		"Path to CA certificate file for target MongoDB TLS connections")
	restoreCmd.Flags().StringVar(&restoreInputDir, "input", "./dumps", "Input directory containing dump files")
	restoreCmd.Flags().IntVar(&restoreTimeout, "timeout", 30, "Connection timeout in seconds")
	restoreCmd.Flags().StringSliceVar(&restoreDatabases, "databases", []string{}, "List of databases to restore (empty means all)")
	restoreCmd.Flags().StringSliceVar(&restoreCollections, "collections", []string{}, "List of collections to restore (empty means all)")
	restoreCmd.Flags().StringSliceVar(&restoreExcludeDatabases, "exclude-databases", []string{}, "List of databases to exclude from restore")
	restoreCmd.Flags().StringSliceVar(&restoreExcludeCollections, "exclude-collections", []string{},
		"List of collections to exclude from restore")
	restoreCmd.Flags().IntVar(&restoreRetryAttempts, "retry-attempts", 5, "Number of retry attempts for failed operations")
	restoreCmd.Flags().StringVar(&restoreStateFile, "state-file", "",
		"Path to state file for tracking restore progress (defaults to <input>/restore-state.json)")
	restoreCmd.Flags().BoolVar(&restoreDrop, "drop", false, "Drop collections before restoring")
	restoreCmd.Flags().BoolVar(&restoreOplogReplay, "oplog-replay", false, "Replay oplog after restoring")
	restoreCmd.Flags().BoolVar(&restorePreserveDates, "preserve-dates", true, "Preserve original document timestamps")

	restoreCmd.MarkFlagRequired("target")
}

// RestoreState tracks the state of restores for incremental operations
type RestoreState struct {
	Collections map[string]RestoreCollectionState `json:"collections"`
	LastRestore time.Time                         `json:"lastRestore"`
}

// RestoreCollectionState tracks the state of a single collection restore
type RestoreCollectionState struct {
	LastRestoreTime time.Time `json:"lastRestoreTime"`
	DocumentCount   int64     `json:"documentCount"`
	Restored        bool      `json:"restored"`
}

func runRestore() error {
	logRestoreConfiguration()

	if err := checkMongorestoreInstalled(); err != nil {
		return err
	}

	if err := validateInputDirectory(); err != nil {
		return err
	}

	ctx := context.Background()

	targetClient, err := connectToTarget(ctx)
	if err != nil {
		return err
	}
	defer targetClient.Disconnect(ctx)

	stateFilePath := getRestoreStateFilePath()
	state, err := loadOrCreateRestoreState(stateFilePath)
	if err != nil {
		return err
	}

	if err := performRestore(ctx, targetClient, state); err != nil {
		return err
	}

	state.LastRestore = time.Now()
	if err := saveRestoreState(state, stateFilePath); err != nil {
		return fmt.Errorf("failed to save restore state: %w", err)
	}

	fmt.Println("MongoDB restore operation completed successfully")
	return nil
}

func checkMongorestoreInstalled() error {
	cmd := exec.Command("mongorestore", "--version")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mongorestore is not installed or not in PATH: %w", err)
	}
	return nil
}

func validateInputDirectory() error {
	if _, err := os.Stat(restoreInputDir); os.IsNotExist(err) {
		return fmt.Errorf("input directory does not exist: %s", restoreInputDir)
	}
	return nil
}

func logRestoreConfiguration() {
	logBasicRestoreConfig()
	logRestoreOptions()
	logFilterRestoreConfig()
}

func logBasicRestoreConfig() {
	fmt.Println("Starting MongoDB restore operation")
	fmt.Printf("Target: %s\n", restoreTargetURI)
	fmt.Printf("Input directory: %s\n", restoreInputDir)
	if restoreTargetCACertFile != "" {
		fmt.Printf("Target CA Certificate File: %s\n", restoreTargetCACertFile)
	}
	fmt.Printf("Connection timeout: %d seconds\n", restoreTimeout)
	fmt.Printf("Retry attempts: %d\n", restoreRetryAttempts)
}

func logRestoreOptions() {
	fmt.Printf("Drop collections before restore: %v\n", restoreDrop)
	fmt.Printf("Replay oplog: %v\n", restoreOplogReplay)
	fmt.Printf("Preserve dates: %v\n", restorePreserveDates)
}

func logFilterRestoreConfig() {
	if len(restoreDatabases) > 0 {
		fmt.Printf("Included databases: %v\n", restoreDatabases)
	}
	if len(restoreCollections) > 0 {
		fmt.Printf("Included collections: %v\n", restoreCollections)
	}
	if len(restoreExcludeDatabases) > 0 {
		fmt.Printf("Excluded databases: %v\n", restoreExcludeDatabases)
	}
	if len(restoreExcludeCollections) > 0 {
		fmt.Printf("Excluded collections: %v\n", restoreExcludeCollections)
	}
}

func connectToTarget(ctx context.Context) (*mongodb.Client, error) {
	connCtx, connCancel := context.WithTimeout(ctx, time.Duration(restoreTimeout)*time.Second)
	defer connCancel()

	targetClient, err := mongodb.NewClient(connCtx, restoreTargetURI, restoreTargetCACertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	return targetClient, nil
}

func getRestoreStateFilePath() string {
	if restoreStateFile != "" {
		return restoreStateFile
	}
	return filepath.Join(restoreInputDir, "restore-state.json")
}

func loadOrCreateRestoreState(stateFilePath string) (*RestoreState, error) {
	state, err := loadRestoreState(stateFilePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load restore state: %w", err)
	}
	if state == nil {
		state = &RestoreState{Collections: make(map[string]RestoreCollectionState)}
	}
	return state, nil
}

func performRestore(ctx context.Context, targetClient *mongodb.Client, state *RestoreState) error {
	databasesToRestore, err := getDatabasesFromDumps()
	if err != nil {
		return err
	}

	for _, dbName := range databasesToRestore {
		if err := restoreDatabase(ctx, targetClient, dbName, state); err != nil {
			return fmt.Errorf("failed to restore database %s: %w", dbName, err)
		}
	}
	return nil
}

func getDatabasesFromDumps() ([]string, error) {
	allDatabases, err := scanDumpDirectory()
	if err != nil {
		return nil, err
	}

	databasesToRestore := selectDatabases(allDatabases)
	return applyDatabaseFilters(databasesToRestore), nil
}

func scanDumpDirectory() ([]string, error) {
	var allDatabases []string

	entries, err := os.ReadDir(restoreInputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read input directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != "." && entry.Name() != ".." &&
			!isSpecialFile(entry.Name()) {
			allDatabases = append(allDatabases, entry.Name())
		}
	}

	return allDatabases, nil
}

func selectDatabases(allDatabases []string) []string {
	if len(restoreDatabases) > 0 {
		fmt.Printf("Using specified databases: %v\n", restoreDatabases)
		return restoreDatabases
	}
	fmt.Printf("Found %d databases in dumps\n", len(allDatabases))
	return allDatabases
}

func applyDatabaseFilters(databases []string) []string {
	originalCount := len(databases)
	if len(restoreExcludeDatabases) > 0 {
		databases = filterByExclusionList(databases, restoreExcludeDatabases)
		fmt.Printf("Filtered out %d databases, %d remaining\n", originalCount-len(databases), len(databases))
	}
	return databases
}

func isSpecialFile(name string) bool {
	specialFiles := []string{"dump-state.json", "restore-state.json", "oplog.bson"}
	for _, special := range specialFiles {
		if name == special {
			return true
		}
	}
	return false
}

func restoreDatabase(ctx context.Context, targetClient *mongodb.Client, dbName string, state *RestoreState) error {
	fmt.Printf("Restoring database: %s\n", dbName)

	dbPath := filepath.Join(restoreInputDir, dbName)
	collectionsToRestore, err := getCollectionsFromDump(dbPath)
	if err != nil {
		return err
	}

	originalCount := len(collectionsToRestore)
	if len(restoreExcludeCollections) > 0 {
		collectionsToRestore = filterByExclusionList(collectionsToRestore, restoreExcludeCollections)
		if originalCount != len(collectionsToRestore) {
			fmt.Printf("  Filtered out %d collections, %d remaining\n", originalCount-len(collectionsToRestore), len(collectionsToRestore))
		}
	}

	fmt.Printf("  Restoring %d collections in database %s\n", len(collectionsToRestore), dbName)
	for _, collName := range collectionsToRestore {
		fmt.Printf("    Restoring collection: %s.%s\n", dbName, collName)
		if err := restoreCollection(ctx, targetClient, dbName, collName, state); err != nil {
			return fmt.Errorf("failed to restore collection %s.%s: %w", dbName, collName, err)
		}
	}
	return nil
}

func getCollectionsFromDump(dbPath string) ([]string, error) {
	var allCollections []string

	if len(restoreCollections) > 0 {
		allCollections = restoreCollections
		fmt.Printf("  Using specified collections: %v\n", allCollections)
		return allCollections, nil
	}

	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read database directory %s: %w", dbPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			allCollections = append(allCollections, entry.Name())
		}
	}

	fmt.Printf("  Found %d collections in database dump\n", len(allCollections))
	return allCollections, nil
}

func restoreCollection(ctx context.Context, targetClient *mongodb.Client, dbName, collName string, state *RestoreState) error {
	collKey := fmt.Sprintf("%s.%s", dbName, collName)

	collPath := filepath.Join(restoreInputDir, dbName, collName)
	if _, err := os.Stat(collPath); os.IsNotExist(err) {
		fmt.Printf("      Skipping collection %s.%s (no dump found)\n", dbName, collName)
		return nil
	}

	if err := executeRestoreWithRetry(dbName, collName, collPath); err != nil {
		return err
	}

	return updateRestoreCollectionState(ctx, targetClient, dbName, collName, collKey, state)
}

func executeRestoreWithRetry(dbName, collName, collPath string) error {
	args := buildMongorestoreArgs(dbName, collName, collPath)

	var lastErr error
	for attempt := 1; attempt <= restoreRetryAttempts; attempt++ {
		if attempt > 1 {
			fmt.Printf("      Retry attempt %d/%d for %s.%s\n", attempt, restoreRetryAttempts, dbName, collName)
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		cmd := exec.Command("mongorestore", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return fmt.Errorf("failed after %d attempts: %w", restoreRetryAttempts, lastErr)
}

func buildMongorestoreArgs(dbName, collName, collPath string) []string {
	args := []string{
		"--uri", restoreTargetURI,
		"--db", dbName,
		"--collection", collName,
		"--dir", collPath,
	}

	if restoreTargetCACertFile != "" {
		args = append(args, "--sslCAFile", restoreTargetCACertFile)
	}

	if restoreDrop {
		args = append(args, "--drop")
	}

	if restoreOplogReplay {
		args = append(args, "--oplogReplay")
	}

	if restorePreserveDates {
		args = append(args, "--maintainInsertionOrder")
	}

	return args
}

func updateRestoreCollectionState(ctx context.Context, targetClient *mongodb.Client,
	dbName, collName, collKey string, state *RestoreState) error {
	db := targetClient.GetDatabase(dbName)
	coll := db.Collection(collName)
	count, err := coll.CountDocuments(ctx, map[string]interface{}{})
	if err != nil {
		return fmt.Errorf("failed to count documents: %w", err)
	}

	state.Collections[collKey] = RestoreCollectionState{
		LastRestoreTime: time.Now(),
		DocumentCount:   count,
		Restored:        true,
	}

	fmt.Printf("      Successfully restored %s.%s (%d documents)\n", dbName, collName, count)
	return nil
}

func loadRestoreState(filePath string) (*RestoreState, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var state RestoreState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func saveRestoreState(state *RestoreState, filePath string) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}
