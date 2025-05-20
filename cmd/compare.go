package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/spf13/cobra"

	"nmongo/internal/config"
	"nmongo/internal/mongodb"
)

var (
	// Command flags for compare command
	compareSourceURI          string
	compareTargetURI          string
	compareSourceCACertFile   string
	compareTargetCACertFile   string
	compareTimeout            int
	compareDatabases          []string
	compareCollections        []string
	compareExcludeDatabases   []string
	compareExcludeCollections []string
	compareBatchSize          int
	compareDetailed           bool
	compareOutputFile         string
)

// compareCmd represents the compare command
var compareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare data between MongoDB clusters",
	Long: `Compare data between two MongoDB clusters.
Analyze differences in document counts, data content, and index structure.

You can include specific databases/collections using --databases and --collections flags,
or exclude specific databases/collections using --exclude-databases and --exclude-collections flags.

Examples:
  nmongo compare --source "mongodb://source-host:27017" --target "mongodb://target-host:27017"
  nmongo compare --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --detailed
  nmongo compare --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --databases "mydb"
  nmongo compare --source "mongodb://source-host:27017" --target "mongodb://target-host:27017" --output "comparison.json"`,
	Run: func(cmd *cobra.Command, args []string) {
		// Load configuration from file if specified
		if configFile != "" {
			cfg, err := config.LoadConfig(configFile)
			if err != nil {
				log.Fatalf("Error loading configuration: %v", err)
			}

			// Only override unset values
			if compareSourceURI == "" {
				compareSourceURI = cfg.SourceURI
			}
			if compareTargetURI == "" {
				compareTargetURI = cfg.TargetURI
			}
			if compareSourceCACertFile == "" {
				compareSourceCACertFile = cfg.SourceCACertFile
			}
			if compareTargetCACertFile == "" {
				compareTargetCACertFile = cfg.TargetCACertFile
			}
			if !cmd.Flags().Changed("timeout") {
				compareTimeout = cfg.Timeout
			}
			if len(compareDatabases) == 0 {
				compareDatabases = cfg.Databases
			}
			if len(compareCollections) == 0 {
				compareCollections = cfg.Collections
			}
			if len(compareExcludeDatabases) == 0 {
				compareExcludeDatabases = cfg.ExcludeDatabases
			}
			if len(compareExcludeCollections) == 0 {
				compareExcludeCollections = cfg.ExcludeCollections
			}
			if !cmd.Flags().Changed("batch-size") {
				compareBatchSize = cfg.BatchSize
			}
		}

		// Save configuration if requested
		if saveConfig {
			configPath, err := config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}

			cfg := &config.Config{
				SourceURI:          compareSourceURI,
				TargetURI:          compareTargetURI,
				SourceCACertFile:   compareSourceCACertFile,
				TargetCACertFile:   compareTargetCACertFile,
				Timeout:            compareTimeout,
				Databases:          compareDatabases,
				Collections:        compareCollections,
				ExcludeDatabases:   compareExcludeDatabases,
				ExcludeCollections: compareExcludeCollections,
				BatchSize:          compareBatchSize,
			}

			if err := config.SaveConfig(cfg, configPath); err != nil {
				log.Fatalf("Error saving configuration: %v", err)
			}

			log.Printf("Configuration saved to %s", configPath)
		}

		if err := runCompare(); err != nil {
			log.Fatalf("Error executing compare command: %v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(compareCmd)

	// Add flags for the compare command
	compareCmd.Flags().StringVar(&compareSourceURI, "source", "",
		"Source MongoDB connection string (required)")
	compareCmd.Flags().StringVar(&compareTargetURI, "target", "",
		"Target MongoDB connection string (required)")
	compareCmd.Flags().StringVar(&compareSourceCACertFile, "source-ca-cert-file", "",
		"Path to CA certificate file for source MongoDB TLS connections")
	compareCmd.Flags().StringVar(&compareTargetCACertFile, "target-ca-cert-file", "",
		"Path to CA certificate file for target MongoDB TLS connections")
	compareCmd.Flags().IntVar(&compareTimeout, "timeout", 30,
		"Connection timeout in seconds")
	compareCmd.Flags().StringSliceVar(&compareDatabases, "databases", []string{},
		"List of databases to compare (empty means all)")
	compareCmd.Flags().StringSliceVar(&compareCollections, "collections", []string{},
		"List of collections to compare (empty means all)")
	compareCmd.Flags().StringSliceVar(&compareExcludeDatabases, "exclude-databases", []string{},
		"List of databases to exclude from comparison")
	compareCmd.Flags().StringSliceVar(&compareExcludeCollections, "exclude-collections", []string{},
		"List of collections to exclude from comparison")
	compareCmd.Flags().IntVar(&compareBatchSize, "batch-size", 10000,
		"Batch size for document operations")
	compareCmd.Flags().BoolVar(&compareDetailed, "detailed", false,
		"Perform detailed document-by-document comparison")
	compareCmd.Flags().StringVar(&compareOutputFile, "output", "",
		"Write comparison results to specified JSON file")

	// Mark required flags
	compareCmd.MarkFlagRequired("source")
	compareCmd.MarkFlagRequired("target")
}

// runCompare executes the compare command
// This has been refactored to reduce cyclomatic complexity
func runCompare() error {
	// Log comparison configuration
	logCompareConfiguration()

	// Connect to source and target MongoDB
	sourceClient, targetClient, err := connectToMongoDB()
	if err != nil {
		return err
	}
	defer func() {
		ctx := context.Background()
		sourceClient.Disconnect(ctx)
		targetClient.Disconnect(ctx)
	}()

	// Get list of databases to compare
	ctx := context.Background()
	dbsToCompare, err := getDatabases(ctx, sourceClient)
	if err != nil {
		return err
	}

	// Compare databases and collect results
	allResults, err := compareAllDatabases(ctx, sourceClient, targetClient, dbsToCompare)
	if err != nil {
		return err
	}

	// Summarize the comparison results
	summarizeResults(allResults)

	// Write results to output file if specified
	if compareOutputFile != "" {
		if err := writeResultsToFile(allResults, compareOutputFile); err != nil {
			return fmt.Errorf("failed to write results to file: %w", err)
		}
	}

	fmt.Println("MongoDB comparison completed successfully")
	return nil
}

// connectToMongoDB connects to source and target MongoDB clusters
func connectToMongoDB() (sourceClient, targetClient *mongodb.Client, err error) {
	ctx := context.Background()

	// Connect to source MongoDB with connection timeout
	connCtx, connCancel := context.WithTimeout(ctx, time.Duration(compareTimeout)*time.Second)
	sourceClient, err = mongodb.NewClient(connCtx, compareSourceURI, compareSourceCACertFile)
	connCancel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}

	// Connect to target MongoDB with connection timeout
	connCtx, connCancel = context.WithTimeout(ctx, time.Duration(compareTimeout)*time.Second)
	targetClient, err = mongodb.NewClient(connCtx, compareTargetURI, compareTargetCACertFile)
	connCancel()
	if err != nil {
		sourceClient.Disconnect(ctx)
		return nil, nil, fmt.Errorf("failed to connect to target MongoDB: %w", err)
	}

	return sourceClient, targetClient, nil
}

// compareAllDatabases compares all specified databases
func compareAllDatabases(
	ctx context.Context,
	sourceClient, targetClient *mongodb.Client,
	dbsToCompare []string,
) ([]*mongodb.ComparisonResult, error) {
	// Store all comparison results
	var allResults []*mongodb.ComparisonResult

	// Compare each database
	for _, dbName := range dbsToCompare {
		results, err := compareDatabase(ctx, sourceClient, targetClient, dbName)
		if err != nil {
			return allResults, fmt.Errorf("failed to compare database %s: %w", dbName, err)
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

// logCompareConfiguration logs the configuration parameters for the compare operation
// This has been refactored to reduce cyclomatic complexity
func logCompareConfiguration() {
	// Log basic information
	fmt.Println("Starting MongoDB comparison operation")
	fmt.Printf("Source: %s\n", compareSourceURI)
	fmt.Printf("Target: %s\n", compareTargetURI)

	// Log certificate information if provided
	logCertificateInfo()

	// Log comparison options
	fmt.Printf("Detailed comparison: %v\n", compareDetailed)
	fmt.Printf("Batch size: %d\n", compareBatchSize)
	fmt.Printf("Connection timeout: %d seconds (used only for initial connections)\n", compareTimeout)
	fmt.Println("Note: Longer timeouts are used automatically for data operations")

	// Log database and collection filters
	logFilterInfo()

	// Log output file if specified
	if compareOutputFile != "" {
		fmt.Printf("Output file: %s\n", compareOutputFile)
	}
}

// logCertificateInfo logs certificate information if provided
func logCertificateInfo() {
	if compareSourceCACertFile != "" {
		fmt.Printf("Source CA Certificate File: %s\n", compareSourceCACertFile)
	}
	if compareTargetCACertFile != "" {
		fmt.Printf("Target CA Certificate File: %s\n", compareTargetCACertFile)
	}
}

// logFilterInfo logs database and collection filter information
func logFilterInfo() {
	if len(compareDatabases) > 0 {
		fmt.Printf("Included databases: %v\n", compareDatabases)
	}
	if len(compareCollections) > 0 {
		fmt.Printf("Included collections: %v\n", compareCollections)
	}
	if len(compareExcludeDatabases) > 0 {
		fmt.Printf("Excluded databases: %v\n", compareExcludeDatabases)
	}
	if len(compareExcludeCollections) > 0 {
		fmt.Printf("Excluded collections: %v\n", compareExcludeCollections)
	}
}

// getDatabases gets the list of databases to compare, applying filters based on command flags
func getDatabases(ctx context.Context, sourceClient *mongodb.Client) ([]string, error) {
	var dbsToCompare []string
	var err error

	if len(compareDatabases) > 0 {
		dbsToCompare = compareDatabases
		fmt.Printf("Using specified databases: %v\n", dbsToCompare)
	} else {
		dbsToCompare, err = sourceClient.ListDatabases(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get databases: %w", err)
		}
		fmt.Printf("Found %d databases in source\n", len(dbsToCompare))
	}

	// Filter out excluded databases
	originalCount := len(dbsToCompare)
	if len(compareExcludeDatabases) > 0 {
		dbsToCompare = filterByExclusionList(dbsToCompare, compareExcludeDatabases)
		fmt.Printf("Filtered out %d databases, %d remaining\n", originalCount-len(dbsToCompare), len(dbsToCompare))
	}

	return dbsToCompare, nil
}

// compareDatabase compares a single database between source and target
func compareDatabase(ctx context.Context, sourceClient, targetClient *mongodb.Client, dbName string) ([]*mongodb.ComparisonResult, error) {
	fmt.Printf("Comparing database: %s\n", dbName)

	// Compare collections
	results, err := mongodb.CompareCollections(
		ctx,
		sourceClient,
		targetClient,
		dbName,
		compareCollections,
		compareExcludeCollections,
		compareBatchSize,
		compareDetailed,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to compare collections: %w", err)
	}

	// Compare indexes for each collection if detailed comparison is requested
	if compareDetailed {
		compareCollectionIndexes(ctx, sourceClient, targetClient, dbName, results)
	}

	return results, nil
}

// compareCollectionIndexes compares indexes for collections
func compareCollectionIndexes(
	ctx context.Context,
	sourceClient, targetClient *mongodb.Client,
	dbName string,
	results []*mongodb.ComparisonResult,
) {
	sourceDB := sourceClient.GetDatabase(dbName)
	targetDB := targetClient.GetDatabase(dbName)

	for _, result := range results {
		collName := result.Collection
		equal, reason, err := mongodb.CompareIndexes(ctx, sourceDB, targetDB, collName)
		if err != nil {
			fmt.Printf("  Warning: Failed to compare indexes for collection %s: %v\n", collName, err)
			continue
		}

		if !equal {
			fmt.Printf("  Index mismatch in collection %s: %s\n", collName, reason)
		} else {
			fmt.Printf("  Indexes in collection %s match\n", collName)
		}
	}
}

// summarizeResults displays a summary of the comparison results
// This has been refactored to reduce cyclomatic complexity
func summarizeResults(results []*mongodb.ComparisonResult) {
	// Calculate summary statistics
	stats := calculateComparisonStats(results)

	// Display summary
	fmt.Println("\nComparison Summary:")
	fmt.Println("-------------------")
	fmt.Printf("Collections compared: %d\n", len(results))
	fmt.Printf("Collections with differences: %d\n", stats.collectionsWithDifferences)
	fmt.Printf("Total documents in source: %d\n", stats.totalSource)
	fmt.Printf("Total documents in target: %d\n", stats.totalTarget)
	fmt.Printf("Total difference in document count: %d\n", stats.totalSource-stats.totalTarget)

	// Display detailed stats if detailed comparison was performed
	if compareDetailed {
		displayDetailedStats(stats)
	}

	// Display details for collections with differences
	if stats.collectionsWithDifferences > 0 {
		displayDifferences(results)
	}
}

// comparisonStats holds summary statistics for comparison results
type comparisonStats struct {
	totalSource                int64
	totalTarget                int64
	totalDifferent             int64
	totalMissingInTarget       int64
	totalMissingInSource       int64
	collectionsWithDifferences int
}

// calculateComparisonStats calculates summary statistics from results
func calculateComparisonStats(results []*mongodb.ComparisonResult) comparisonStats {
	var stats comparisonStats

	for _, result := range results {
		stats.totalSource += result.SourceCount
		stats.totalTarget += result.TargetCount
		stats.totalMissingInTarget += result.MissingInTarget
		stats.totalMissingInSource += result.MissingInSource
		stats.totalDifferent += result.DifferentDocuments

		if result.Difference != 0 || result.MissingInTarget > 0 ||
			result.MissingInSource > 0 || result.DifferentDocuments > 0 {
			stats.collectionsWithDifferences++
		}
	}

	return stats
}

// displayDetailedStats displays detailed comparison statistics
func displayDetailedStats(stats comparisonStats) {
	fmt.Printf("Documents missing in target: %d\n", stats.totalMissingInTarget)
	fmt.Printf("Documents missing in source: %d\n", stats.totalMissingInSource)
	fmt.Printf("Documents with different content: %d\n", stats.totalDifferent)
}

// displayDifferences displays details for collections with differences
func displayDifferences(results []*mongodb.ComparisonResult) {
	fmt.Println("\nCollections with differences:")
	fmt.Println("-----------------------------")

	for _, result := range results {
		hasDifference := result.Difference != 0 || result.MissingInTarget > 0 ||
			result.MissingInSource > 0 || result.DifferentDocuments > 0

		if hasDifference {
			fmt.Printf("%s.%s:\n", result.Database, result.Collection)
			fmt.Printf("  Source count: %d, Target count: %d, Difference: %d\n",
				result.SourceCount, result.TargetCount, result.Difference)

			if compareDetailed {
				fmt.Printf("  Missing in target: %d, Missing in source: %d, Different content: %d\n",
					result.MissingInTarget, result.MissingInSource, result.DifferentDocuments)
			}
		}
	}
}

// writeResultsToFile writes the comparison results to a JSON file
func writeResultsToFile(results []*mongodb.ComparisonResult, filePath string) error {
	fmt.Printf("Writing comparison results to %s\n", filePath)

	// Create the output file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Create output structure with timestamp and results
	output := struct {
		Timestamp string                      `json:"timestamp"`
		Results   []*mongodb.ComparisonResult `json:"results"`
	}{
		Timestamp: time.Now().Format(time.RFC3339),
		Results:   results,
	}

	// Convert to JSON
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		return fmt.Errorf("failed to encode results to JSON: %w", err)
	}

	return nil
}
