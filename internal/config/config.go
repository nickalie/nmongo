// Package config handles configuration management for the nmongo application.
// It provides functions to load, save, and access configuration settings.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	SourceURI        string   `mapstructure:"sourceUri" json:"sourceUri" yaml:"sourceUri" toml:"sourceUri"`
	TargetURI        string   `mapstructure:"targetUri" json:"targetUri" yaml:"targetUri" toml:"targetUri"`
	SourceCACertFile string   `mapstructure:"sourceCACertFile" json:"sourceCACertFile" yaml:"sourceCACertFile" toml:"sourceCACertFile"`
	TargetCACertFile string   `mapstructure:"targetCACertFile" json:"targetCACertFile" yaml:"targetCACertFile" toml:"targetCACertFile"`
	Incremental      bool     `mapstructure:"incremental" json:"incremental" yaml:"incremental" toml:"incremental"`
	Timeout          int      `mapstructure:"timeout" json:"timeout" yaml:"timeout" toml:"timeout"`
	SocketTimeout    int      `mapstructure:"socketTimeout" json:"socketTimeout" yaml:"socketTimeout" toml:"socketTimeout"`
	Databases        []string `mapstructure:"databases" json:"databases" yaml:"databases" toml:"databases"`
	Collections      []string `mapstructure:"collections" json:"collections" yaml:"collections" toml:"collections"`
	ExcludeDatabases []string `mapstructure:"excludeDatabases" json:"excludeDatabases" yaml:"excludeDatabases" toml:"excludeDatabases"`
	// Split struct tags to avoid linter line length warning
	ExcludeCollections []string `mapstructure:"excludeCollections" json:"excludeCollections" yaml:"excludeCollections" toml:"excludeCollections"` //nolint:lll // linter line length warning
	BatchSize          int      `mapstructure:"batchSize" json:"batchSize" yaml:"batchSize" toml:"batchSize"`
	LastModifiedField  string   `mapstructure:"lastModifiedField" json:"lastModifiedField" yaml:"lastModifiedField" toml:"lastModifiedField"`
	RetryAttempts      int      `mapstructure:"retryAttempts" json:"retryAttempts" yaml:"retryAttempts" toml:"retryAttempts"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		SourceURI:          "",
		TargetURI:          "",
		SourceCACertFile:   "",
		TargetCACertFile:   "",
		Incremental:        false,
		Timeout:            30,
		SocketTimeout:      1800,
		Databases:          []string{},
		Collections:        []string{},
		ExcludeDatabases:   []string{},
		ExcludeCollections: []string{},
		BatchSize:          10000,
		LastModifiedField:  "lastModified",
		RetryAttempts:      5,
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(filePath string) (*Config, error) {
	config := DefaultConfig()

	// Initialize viper
	v := viper.New()
	// Set default values
	v.SetDefault("sourceUri", config.SourceURI)
	v.SetDefault("targetUri", config.TargetURI)
	v.SetDefault("sourceCACertFile", config.SourceCACertFile)
	v.SetDefault("targetCACertFile", config.TargetCACertFile)
	v.SetDefault("incremental", config.Incremental)
	v.SetDefault("timeout", config.Timeout)
	v.SetDefault("socketTimeout", config.SocketTimeout)
	v.SetDefault("databases", config.Databases)
	v.SetDefault("collections", config.Collections)
	v.SetDefault("excludeDatabases", config.ExcludeDatabases)
	v.SetDefault("excludeCollections", config.ExcludeCollections)
	v.SetDefault("batchSize", config.BatchSize)
	v.SetDefault("lastModifiedField", config.LastModifiedField)
	v.SetDefault("retryAttempts", config.RetryAttempts)

	// Configure Viper to use the file
	v.SetConfigFile(filePath)

	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", filePath)
	}

	// Read the configuration file
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Unmarshal the config into our struct
	if err := v.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(config *Config, filePath string) error {
	// Create the directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Initialize Viper
	v := viper.New()
	// Set the values from our config
	v.Set("sourceUri", config.SourceURI)
	v.Set("targetUri", config.TargetURI)
	v.Set("sourceCACertFile", config.SourceCACertFile)
	v.Set("targetCACertFile", config.TargetCACertFile)
	v.Set("incremental", config.Incremental)
	v.Set("timeout", config.Timeout)
	v.Set("socketTimeout", config.SocketTimeout)
	v.Set("databases", config.Databases)
	v.Set("collections", config.Collections)
	v.Set("excludeDatabases", config.ExcludeDatabases)
	v.Set("excludeCollections", config.ExcludeCollections)
	v.Set("batchSize", config.BatchSize)
	v.Set("lastModifiedField", config.LastModifiedField)
	v.Set("retryAttempts", config.RetryAttempts)

	// Set the config file
	v.SetConfigFile(filePath)

	// Determine format based on file extension
	ext := filepath.Ext(filePath)
	if ext != "" {
		v.SetConfigType(strings.TrimPrefix(ext, "."))
	} else {
		// Default to JSON if no extension
		v.SetConfigType("json")
	}

	// Write the config file
	if err := v.WriteConfig(); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// GetConfigFilePath returns the path to the config file
func GetConfigFilePath() (string, error) {
	// Get the user's home directory
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	// Create the nmongo directory if it doesn't exist
	configDir := filepath.Join(homeDir, ".nmongo")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create config directory: %w", err)
	}

	return filepath.Join(configDir, "config.json"), nil
}

// GetConfigFilePathWithExt returns the path to the config file with the specified extension
func GetConfigFilePathWithExt(extension string) (string, error) {
	// Get the base path
	basePath, err := GetConfigFilePath()
	if err != nil {
		return "", err
	}

	// If extension is empty or doesn't start with a dot, use default
	if extension == "" {
		return basePath, nil
	}

	// Make sure extension starts with a dot
	if !strings.HasPrefix(extension, ".") {
		extension = "." + extension
	}

	// Replace the extension
	baseWithoutExt := strings.TrimSuffix(basePath, filepath.Ext(basePath))
	return baseWithoutExt + extension, nil
}
