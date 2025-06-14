package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"

	"nmongo/internal/config"
	"nmongo/internal/mongodb"
)

var (
	sourceURI          string
	targetURI          string
	sourceCACertFile   string
	targetCACertFile   string
	incremental        bool
	timeout            int
	socketTimeout      int
	databases          []string
	collections        []string
	excludeDatabases   []string
	excludeCollections []string
	batchSize          int
	lastModifiedField  string
	retryAttempts      int
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy data between MongoDB clusters",
	Long: `Copy all data or selected databases and collections from one MongoDB cluster to another.
Supports incremental copying to only transfer new or updated documents.

You can include specific databases/collections using --databases and --collections flags,
or exclude specific databases/collections using --exclude-databases and --exclude-collections flags.

Examples:
  nmongo copy --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --incremental
  nmongo copy --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --exclude-databases "admin,local,config"
  nmongo copy --source "mongodb://source-host:27017" --target "mongodb://target-host:27017"
    --exclude-collections "system.profile,system.users"`,
	Run: func(cmd *cobra.Command, args []string) {
		// Load configuration from file if specified
		if configFile != "" {
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				log.Fatalf("Error loading configuration: %v", err)
			}

			// Only override unset values
			if sourceURI == "" {
				sourceURI = cfg.SourceURI
			}
			if targetURI == "" {
				targetURI = cfg.TargetURI
			}
			if sourceCACertFile == "" {
				sourceCACertFile = cfg.SourceCACertFile
			}
			if targetCACertFile == "" {
				targetCACertFile = cfg.TargetCACertFile
			}
			if !cmd.Flags().Changed("incremental") {
				incremental = cfg.Incremental
			}
			if !cmd.Flags().Changed("timeout") {
				timeout = cfg.Timeout
			}
			if !cmd.Flags().Changed("socket-timeout") {
				socketTimeout = cfg.SocketTimeout
			}
			if len(databases) == 0 {
				databases = cfg.Databases
			}
			if len(collections) == 0 {
				collections = cfg.Collections
			}
			if len(excludeDatabases) == 0 {
				excludeDatabases = cfg.ExcludeDatabases
			}
			if len(excludeCollections) == 0 {
				excludeCollections = cfg.ExcludeCollections
			}
			if !cmd.Flags().Changed("batch-size") {
				batchSize = cfg.BatchSize
			}
			if !cmd.Flags().Changed("last-modified-field") {
				lastModifiedField = cfg.LastModifiedField
			}
			if !cmd.Flags().Changed("retry-attempts") && cfg.RetryAttempts > 0 {
				retryAttempts = cfg.RetryAttempts
			}
		}

		// Save configuration if requested
		if saveConfig {
			configPath, err := config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}

			cfg := &config.Config{
				SourceURI:          sourceURI,
				TargetURI:          targetURI,
				SourceCACertFile:   sourceCACertFile,
				TargetCACertFile:   targetCACertFile,
				Incremental:        incremental,
				Timeout:            timeout,
				SocketTimeout:      socketTimeout,
				Databases:          databases,
				Collections:        collections,
				ExcludeDatabases:   excludeDatabases,
				ExcludeCollections: excludeCollections,
				BatchSize:          batchSize,
				LastModifiedField:  lastModifiedField,
				RetryAttempts:      retryAttempts,
			}

			if err := config.SaveConfig(cfg, configPath); err != nil {
				log.Fatalf("Error saving configuration: %v", err)
			}

			log.Printf("Configuration saved to %s", configPath)
		}

		if err := runCopy(); err != nil {
			log.Fatalf("Error executing copy command: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)
	// Add flags for the copy command
	copyCmd.Flags().StringVar(&sourceURI, "source", "", "Source MongoDB connection string (required)")
	copyCmd.Flags().StringVar(&targetURI, "target", "", "Target MongoDB connection string (required)")
	copyCmd.Flags().StringVar(&sourceCACertFile, "source-ca-cert-file", "", "Path to CA certificate file for source MongoDB TLS connections")
	copyCmd.Flags().StringVar(&targetCACertFile, "target-ca-cert-file", "", "Path to CA certificate file for target MongoDB TLS connections")
	copyCmd.Flags().BoolVar(&incremental, "incremental", false, "Perform incremental copy (only copy new or updated documents)")
	copyCmd.Flags().IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	copyCmd.Flags().IntVar(&socketTimeout, "socket-timeout", 1800, "Socket timeout in seconds for data operations (default: 30 minutes)")
	copyCmd.Flags().StringSliceVar(&databases, "databases", []string{}, "List of databases to copy (empty means all)")
	copyCmd.Flags().StringSliceVar(&collections, "collections", []string{}, "List of collections to copy (empty means all)")
	copyCmd.Flags().StringSliceVar(&excludeDatabases, "exclude-databases", []string{}, "List of databases to exclude from copy")
	copyCmd.Flags().StringSliceVar(&excludeCollections, "exclude-collections", []string{}, "List of collections to exclude from copy")
	copyCmd.Flags().IntVar(&batchSize, "batch-size", 10000, "Batch size for document operations")
	copyCmd.Flags().StringVar(&lastModifiedField, "last-modified-field", "lastModified",
		"Field name to use for tracking document modifications in incremental copy")
	copyCmd.Flags().IntVar(&retryAttempts, "retry-attempts", 5, "Number of retry attempts for failed operations")

	// Mark required flags
	copyCmd.MarkFlagRequired("source")
	copyCmd.MarkFlagRequired("target")
}

func runCopy() error {
	logCopyConfiguration()

	// Create a background context without a global timeout
	ctx := context.Background()

	// Connect to source MongoDB with connection timeout
	connCtx, connCancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	sourceClient, err := mongodb.NewClientWithSocketTimeout(connCtx, sourceURI, sourceCACertFile, socketTimeout)
	connCancel()
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}
	defer sourceClient.Disconnect(ctx)

	// Connect to target MongoDB with connection timeout
	connCtx, connCancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	targetClient, err := mongodb.NewClientWithSocketTimeout(connCtx, targetURI, targetCACertFile, socketTimeout)
	connCancel()
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer targetClient.Disconnect(ctx)

	// Get list of databases to copy
	dbsToCopy, err := getDatabasesToCopy(ctx, sourceClient)
	if err != nil {
		return err
	}

	// Copy each database
	for _, dbName := range dbsToCopy {
		if err := copyDatabase(ctx, sourceClient, targetClient, dbName); err != nil {
			return fmt.Errorf("failed to copy database %s: %w", dbName, err)
		}
	}

	fmt.Println("MongoDB copy operation completed successfully")
	return nil
}

// logCopyConfiguration logs the configuration parameters for the copy operation
func logCopyConfiguration() {
	// Log basic configuration
	logBasicConfig()

	// Log incremental mode configuration
	logIncrementalConfig()

	// Log database and collection filters
	logFilterConfig()
}

// logBasicConfig logs the basic configuration parameters
func logBasicConfig() {
	fmt.Println("Starting MongoDB copy operation")
	fmt.Printf("Source: %s\n", sourceURI)
	fmt.Printf("Target: %s\n", targetURI)
	if sourceCACertFile != "" {
		fmt.Printf("Source CA Certificate File: %s\n", sourceCACertFile)
	}
	if targetCACertFile != "" {
		fmt.Printf("Target CA Certificate File: %s\n", targetCACertFile)
	}
	fmt.Printf("Incremental mode: %v\n", incremental)
	fmt.Printf("Batch size: %d\n", batchSize)
	fmt.Printf("Connection timeout: %d seconds (used only for initial connections)\n", timeout)
	fmt.Printf("Socket timeout: %d seconds (used for data operations)\n", socketTimeout)
	fmt.Printf("Retry attempts: %d\n", retryAttempts)
}

// logIncrementalConfig logs the incremental copy configuration
func logIncrementalConfig() {
	if incremental && lastModifiedField != "" {
		fmt.Printf("Last modified field: %s\n", lastModifiedField)
	}
}

// logFilterConfig logs the database and collection filter configuration
func logFilterConfig() {
	if len(databases) > 0 {
		fmt.Printf("Included databases: %v\n", databases)
	}
	if len(collections) > 0 {
		fmt.Printf("Included collections: %v\n", collections)
	}
	if len(excludeDatabases) > 0 {
		fmt.Printf("Excluded databases: %v\n", excludeDatabases)
	}
	if len(excludeCollections) > 0 {
		fmt.Printf("Excluded collections: %v\n", excludeCollections)
	}
}

// getDatabasesToCopy gets the list of databases to copy, applying filters based on command flags
func getDatabasesToCopy(ctx context.Context, sourceClient *mongodb.Client) ([]string, error) {
	var dbsToCopy []string
	var err error

	if len(databases) > 0 {
		dbsToCopy = databases
		fmt.Printf("Using specified databases: %v\n", dbsToCopy)
	} else {
		dbsToCopy, err = sourceClient.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get databases: %w", err)
		}
		fmt.Printf("Found %d databases in source\n", len(dbsToCopy))
	}

	// Filter out excluded databases
	originalCount := len(dbsToCopy)
	if len(excludeDatabases) > 0 {
		dbsToCopy = filterByExclusionList(dbsToCopy, excludeDatabases)
		fmt.Printf("Filtered out %d databases, %d remaining\n", originalCount-len(dbsToCopy), len(dbsToCopy))
	}

	return dbsToCopy, nil
}

// filterByExclusionList filters a slice of strings by removing items that are in the exclusion list.
// It returns a new slice containing only the non-excluded items.
func filterByExclusionList(items, exclusionList []string) []string {
	if len(exclusionList) == 0 {
		return items
	}

	// Create a map for faster lookups (O(1) instead of O(n) for slices)
	excluded := make(map[string]bool)
	for _, item := range exclusionList {
		excluded[item] = true
	}

	// Filter the items
	filtered := make([]string, 0, len(items))
	for _, item := range items {
		if !excluded[item] {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

// copyDatabase copies a single database from source to target
func copyDatabase(ctx context.Context, sourceClient, targetClient *mongodb.Client, dbName string) error {
	fmt.Printf("Copying database: %s\n", dbName)
	// Get the database
	sourceDB := sourceClient.GetDatabase(dbName)
	targetDB := targetClient.GetDatabase(dbName)

	// Get collections to copy
	var collsToCopy []string
	if len(collections) > 0 {
		collsToCopy = collections
		fmt.Printf("  Using specified collections: %v\n", collsToCopy)
	} else {
		var err error
		collsToCopy, err = sourceClient.ListCollections(ctx, dbName)
		if err != nil {
			return fmt.Errorf("failed to get collections for database %s: %w", dbName, err)
		}
		fmt.Printf("  Found %d collections in database %s\n", len(collsToCopy), dbName)
	}

	// Filter out excluded collections
	originalCount := len(collsToCopy)
	if len(excludeCollections) > 0 {
		collsToCopy = filterByExclusionList(collsToCopy, excludeCollections)
		if originalCount != len(collsToCopy) {
			fmt.Printf("  Filtered out %d collections, %d remaining\n", originalCount-len(collsToCopy), len(collsToCopy))
		}
	}

	// Copy each collection
	fmt.Printf("  Copying %d collections in database %s\n", len(collsToCopy), dbName)
	for _, collName := range collsToCopy {
		fmt.Printf("    Copying collection: %s.%s\n", dbName, collName)
		err := mongodb.CopyCollection(ctx, sourceDB, targetDB, collName,
			incremental, batchSize, lastModifiedField, retryAttempts)
		if err != nil {
			return fmt.Errorf("failed to copy collection %s.%s: %w", dbName, collName, err)
		}
	}
	return nil
}
