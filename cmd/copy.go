package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"nmongo/internal/config"
	"nmongo/internal/mongodb"
)

var (
	sourceURI      string
	destinationURI string
	incremental    bool
	timeout        int
	databases      []string
	collections    []string
	batchSize      int
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy data between MongoDB clusters",
	Long: `Copy all data or selected databases and collections from one MongoDB cluster to another.
Supports incremental copying to only transfer new or updated documents.

Example:
  nmongo copy --source "mongodb://source-host:27017" --destination "mongodb://dest-host:27017" --incremental`,
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
			if destinationURI == "" {
				destinationURI = cfg.DestinationURI
			}
			if cmd.Flags().Changed("incremental") == false {
				incremental = cfg.Incremental
			}
			if cmd.Flags().Changed("timeout") == false {
				timeout = cfg.Timeout
			}
			if len(databases) == 0 {
				databases = cfg.Databases
			}
			if len(collections) == 0 {
				collections = cfg.Collections
			}
			if cmd.Flags().Changed("batch-size") == false {
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
				SourceURI:      sourceURI,
				DestinationURI: destinationURI,
				Incremental:    incremental,
				Timeout:        timeout,
				Databases:      databases,
				Collections:    collections,
				BatchSize:      batchSize,
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
	copyCmd.Flags().StringVar(&destinationURI, "destination", "", "Destination MongoDB connection string (required)")
	copyCmd.Flags().BoolVar(&incremental, "incremental", false, "Perform incremental copy (only copy new or updated documents)")
	copyCmd.Flags().IntVar(&timeout, "timeout", 30, "Connection timeout in seconds")
	copyCmd.Flags().StringSliceVar(&databases, "databases", []string{}, "List of databases to copy (empty means all)")
	copyCmd.Flags().StringSliceVar(&collections, "collections", []string{}, "List of collections to copy (empty means all)")
	copyCmd.Flags().IntVar(&batchSize, "batch-size", 1000, "Batch size for document operations")

	// Mark required flags
	copyCmd.MarkFlagRequired("source")
	copyCmd.MarkFlagRequired("destination")
}

func runCopy() error {
	fmt.Println("Starting MongoDB copy operation")
	fmt.Printf("Source: %s\n", sourceURI)
	fmt.Printf("Destination: %s\n", destinationURI)
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

	// Connect to destination MongoDB
	destClient, err := mongodb.NewClient(ctx, destinationURI)
	if err != nil {
		return fmt.Errorf("failed to connect to destination MongoDB: %w", err)
	}
	defer destClient.Disconnect(ctx)

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
		if err := copyDatabase(ctx, sourceClient, destClient, dbName); err != nil {
			return fmt.Errorf("failed to copy database %s: %w", dbName, err)
		}
	}

	fmt.Println("MongoDB copy operation completed successfully")
	return nil
}

// copyDatabase copies a single database from source to destination
func copyDatabase(ctx context.Context, sourceClient, destClient *mongodb.Client, dbName string) error {
	fmt.Printf("Copying database: %s\n", dbName)

	// Get the database
	sourceDB := sourceClient.GetDatabase(dbName)
	destDB := destClient.GetDatabase(dbName)

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
		if err := mongodb.CopyCollection(ctx, sourceDB, destDB, collName, incremental, batchSize); err != nil {
			return fmt.Errorf("failed to copy collection %s.%s: %w", dbName, collName, err)
		}
	}

	return nil
}

// getCollections returns the list of collections to copy
func getCollections(ctx context.Context, db *mongo.Database, requestedColls []string) ([]string, error) {
	if len(requestedColls) > 0 {
		return requestedColls, nil
	}

	// List all collections
	colls, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	// Filter out system collections
	var filteredColls []string
	for _, coll := range colls {
		if coll != "system.profile" && coll != "system.views" {
			filteredColls = append(filteredColls, coll)
		}
	}

	return filteredColls, nil
}

// copyCollection copies a single collection from source to destination
func copyCollection(ctx context.Context, sourceDB, destDB *mongo.Database, collName string) error {
	fmt.Printf("  Copying collection: %s\n", collName)

	sourceColl := sourceDB.Collection(collName)
	destColl := destDB.Collection(collName)

	// Define the query filter based on incremental flag
	filter := bson.M{}
	if incremental {
		// In incremental mode, we need to find documents that don't exist in the destination
		// or that have been updated since the last copy
		// This is a simplified approach and might need refinement based on your requirements
		fmt.Printf("  Using incremental mode for collection: %s\n", collName)

		// For incremental copy, we would ideally use timestamps or modification dates
		// This is a placeholder for actual incremental logic
		// TODO: Implement proper incremental copy logic
	}

	// Create a cursor for the source collection
	findOptions := options.Find().SetBatchSize(int32(batchSize))
	cursor, err := sourceColl.Find(ctx, filter, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Process documents in batches
	batch := make([]interface{}, 0, batchSize)
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return err
		}
		batch = append(batch, doc)

		// If batch is full, insert the batch
		if len(batch) >= batchSize {
			if len(batch) > 0 {
				_, err := destColl.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
				if err != nil {
					// Handle duplicate key errors for incremental copy
					if mongo.IsDuplicateKeyError(err) && incremental {
						fmt.Printf("    Skipping duplicate documents in %s\n", collName)
					} else {
						return err
					}
				}

				fmt.Printf("    Copied %d documents to %s\n", len(batch), collName)
				batch = batch[:0] // Clear the batch
			}
		}
	}

	if err := cursor.Err(); err != nil {
		return err
	}

	fmt.Printf("  Completed copying collection: %s\n", collName)
	return nil
}
