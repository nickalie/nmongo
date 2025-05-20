package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"nmongo/internal/config"
)

var (
	configFile string
	saveConfig bool
	configFormat string
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage nmongo configuration",
	Long: `Manage nmongo configuration settings.
	
You can view or update the configuration file that stores default settings for
connection strings, batch sizes, and other parameters.

Supports multiple configuration formats: JSON, YAML, and TOML.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	Run: func(cmd *cobra.Command, args []string) {
		// Get the config path with the specified format
		var configPath string
		var err error
		
		if configFile != "" {
			configPath = configFile
		} else if configFormat != "" {
			configPath, err = config.GetConfigFilePathWithExt(configFormat)
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}
		} else {
			configPath, err = config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}
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
		// Get the config path with the specified format
		var configPath string
		var err error
		
		if configFile != "" {
			configPath = configFile
		} else if configFormat != "" {
			configPath, err = config.GetConfigFilePathWithExt(configFormat)
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}
		} else {
			configPath, err = config.GetConfigFilePath()
			if err != nil {
				log.Fatalf("Error getting configuration path: %v", err)
			}
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
	
	// Add format flag to both show and save commands
	configCmd.PersistentFlags().StringVar(&configFormat, "format", "", "Configuration file format (json, yaml, or toml)")
	configShowCmd.Flags().StringVar(&configFile, "file", "", "Path to configuration file")
	configSaveCmd.Flags().StringVar(&configFile, "file", "", "Path to configuration file")
	
	// Add config-related flags to the copy command
	copyCmd.Flags().StringVar(&configFile, "config", "", "Path to configuration file")
	copyCmd.Flags().BoolVar(&saveConfig, "save-config", false, "Save current flags to configuration file")
	copyCmd.Flags().StringVar(&configFormat, "config-format", "", "Configuration file format for saving (json, yaml, or toml)")
}
