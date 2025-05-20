package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"nmongo/internal/config"
)

var (
	configFile string
	saveConfig bool
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage nmongo configuration",
	Long: `Manage nmongo configuration settings.
	
You can view or update the configuration file that stores default settings for
connection strings, batch sizes, and other parameters.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	Run: func(cmd *cobra.Command, args []string) {
		configPath, err := config.GetConfigFilePath()
		if err != nil {
			log.Fatalf("Error getting configuration path: %v", err)
		}

		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			log.Printf("No configuration found or error loading: %v", err)
			log.Printf("Using default configuration")
			cfg = config.DefaultConfig()
		}

		// Display configuration
		log.Println("Current configuration:")
		log.Printf("  Source URI: %s", cfg.SourceURI)
		log.Printf("  Destination URI: %s", cfg.DestinationURI)
		log.Printf("  Incremental: %v", cfg.Incremental)
		log.Printf("  Timeout: %d seconds", cfg.Timeout)
		log.Printf("  Databases: %v", cfg.Databases)
		log.Printf("  Collections: %v", cfg.Collections)
		log.Printf("  Batch Size: %d", cfg.BatchSize)
		log.Printf("Configuration file: %s", configPath)
	},
}

var configSaveCmd = &cobra.Command{
	Use:   "save",
	Short: "Save current CLI flags to configuration",
	Run: func(cmd *cobra.Command, args []string) {
		configPath, err := config.GetConfigFilePath()
		if err != nil {
			log.Fatalf("Error getting configuration path: %v", err)
		}

		// Create config from command-line flags
		cfg := &config.Config{
			SourceURI:      sourceURI,
			DestinationURI: destinationURI,
			Incremental:    incremental,
			Timeout:        timeout,
			Databases:      databases,
			Collections:    collections,
			BatchSize:      batchSize,
		}

		// Save configuration
		if err := config.SaveConfig(cfg, configPath); err != nil {
			log.Fatalf("Error saving configuration: %v", err)
		}

		log.Printf("Configuration saved to %s", configPath)
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configShowCmd)
	configCmd.AddCommand(configSaveCmd)

	copyCmd.Flags().StringVar(&configFile, "config", "", "Path to configuration file")
	copyCmd.Flags().BoolVar(&saveConfig, "save-config", false, "Save current flags to configuration file")
}
