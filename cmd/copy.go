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
	sourceURI   string
	targetURI   string
	incremental bool
	timeout     int
	databases   []string
	collections []string
	batchSize   int
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy data between MongoDB clusters",
	Long: `Copy all data or selected databases and collections from one MongoDB cluster to another.
Supports incremental copying to only transfer new or updated documents.

Example:
  nmongo copy --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --incremental`,
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
			if !cmd.Flags().Changed("incremental") {
				incremental = cfg.Incremental
			}
			if !cmd.Flags().Changed("timeout") {
				timeout = cfg.Timeout
			}
			if len(databases) == 0 {
				databases = cfg.Databases
			}
			if len(collections) == 0 {
				collections = cfg.Collections
			}
			if !cmd.Flags().Changed("batch-size") {
				batchSize = cfg.BatchSize
			}
		}

		// Save configuration if requested
		if saveConfig {
			configPath, err := config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}

			cfg := &config.Config{
				SourceURI:   sourceURI,
				TargetURI:   targetURI,
				Incremental: incremental,
				Timeout:     timeout,
				Databases:   databases,
				Collections: collections,
				BatchSize:   batchSize,
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
	copyCmd.Flags().BoolVar(&incremental, "incremental", false, "Perform incremental copy (only copy new or updated documents)")
	copyCmd.Flags().IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	copyCmd.Flags().StringSliceVar(&databases, "databases", []string{}, "List of databases to copy (empty means all)")
	copyCmd.Flags().StringSliceVar(&collections, "collections", []string{}, "List of collections to copy (empty means all)")
	copyCmd.Flags().IntVar(&batchSize, "batch-size", 1000, "Batch size for document operations")

	// Mark required flags
	copyCmd.MarkFlagRequired("source")
	copyCmd.MarkFlagRequired("target")
}

func runCopy() error {
	fmt.Println("Starting MongoDB copy operation")
	fmt.Printf("Source: %s\n", sourceURI)
	fmt.Printf("Target: %s\n", targetURI)
	fmt.Printf("Incremental mode: %v\n", incremental)

	// Set up the context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	// Connect to source MongoDB
	sourceClient, err := mongodb.NewClient(ctx, sourceURI)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}
	defer sourceClient.Disconnect(ctx)
	// Connect to target MongoDB
	targetClient, err := mongodb.NewClient(ctx, targetURI)
	if err != nil {
		return fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}
	defer targetClient.Disconnect(ctx)

	// Get list of databases to copy
	var dbsToCopy []string
	if len(databases) > 0 {
		dbsToCopy = databases
	} else {
		dbsToCopy, err = sourceClient.ListDatabases(ctx)
		if err != nil {
			return fmt.Errorf("failed to get databases: %w", err)
		}
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
	} else {
		var err error
		collsToCopy, err = sourceClient.ListCollections(ctx, dbName)
		if err != nil {
			return fmt.Errorf("failed to get collections for database %s: %w", dbName, err)
		}
	}
	// Copy each collection
	for _, collName := range collsToCopy {
		if err := mongodb.CopyCollection(ctx, sourceDB, targetDB, collName, incremental, batchSize); err != nil {
			return fmt.Errorf("failed to copy collection %s.%s: %w", dbName, collName, err)
		}
	}
	return nil
}
