package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	// Get default config
	config := DefaultConfig()
	// Verify default values
	assert.Equal(t, "", config.SourceURI, "Default SourceURI should be empty")
	assert.Equal(t, "", config.TargetURI, "Default TargetURI should be empty")
	assert.False(t, config.Incremental, "Default Incremental should be false")
	assert.Equal(t, 30, config.Timeout, "Default Timeout should be 30")
	assert.Empty(t, config.Databases, "Default Databases should be empty")
	assert.Empty(t, config.Collections, "Default Collections should be empty")
	assert.Equal(t, 1000, config.BatchSize, "Default BatchSize should be 1000")
}

func TestConfigSaveLoadFormatTypes(t *testing.T) {
	// Test different file formats
	formats := []string{
		"json",
		"yaml",
		"toml",
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "config-test")
	require.NoError(t, err, "Failed to create temp directory")
	defer os.RemoveAll(tempDir)
	// Create a test config
	testConfig := &Config{
		SourceURI:   "mongodb://source:27017",
		TargetURI:   "mongodb://dest:27017",
		Incremental: true,
		Timeout:     60,
		Databases:   []string{"db1", "db2"},
		Collections: []string{"coll1", "coll2"},
		BatchSize:   2000,
	}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			// Create a config file path
			configFilePath := filepath.Join(tempDir, "config."+format)

			// Save the config
			err = SaveConfig(testConfig, configFilePath)
			require.NoError(t, err, "Failed to save config")

			// Verify file exists
			_, err = os.Stat(configFilePath)
			require.NoError(t, err, "Config file should exist")

			// Load the config
			loadedConfig, err := LoadConfig(configFilePath)
			require.NoError(t, err, "Failed to load config")
			// Verify loaded config matches the original
			assert.Equal(t, testConfig.SourceURI, loadedConfig.SourceURI, "Loaded SourceURI should match")
			assert.Equal(t, testConfig.TargetURI, loadedConfig.TargetURI, "Loaded TargetURI should match")
			assert.Equal(t, testConfig.Incremental, loadedConfig.Incremental, "Loaded Incremental should match")
			assert.Equal(t, testConfig.Timeout, loadedConfig.Timeout, "Loaded Timeout should match")
			assert.Equal(t, testConfig.Databases, loadedConfig.Databases, "Loaded Databases should match")
			assert.Equal(t, testConfig.Collections, loadedConfig.Collections, "Loaded Collections should match")
			assert.Equal(t, testConfig.BatchSize, loadedConfig.BatchSize, "Loaded BatchSize should match")
		})
	}
}

func TestLoadConfigNonExistent(t *testing.T) {
	// Try to load a non-existent config file
	_, err := LoadConfig("/non/existent/path.json")
	assert.Error(t, err, "Loading non-existent file should return error")
	assert.Contains(t, err.Error(), "config file not found", "Error should mention file not found")
}

func TestGetConfigFilePath(t *testing.T) {
	// Get config file path
	configPath, err := GetConfigFilePath()
	require.NoError(t, err, "Failed to get config file path")

	// Verify path contains expected elements
	homeDir, err := os.UserHomeDir()
	require.NoError(t, err, "Failed to get home directory")

	expectedPath := filepath.Join(homeDir, ".nmongo", "config.json")
	assert.Equal(t, expectedPath, configPath, "Config file path should be in user's home directory")

	// Verify the directory exists (should be created by GetConfigFilePath)
	configDir := filepath.Dir(configPath)
	dirInfo, err := os.Stat(configDir)
	assert.NoError(t, err, "Config directory should exist")
	assert.True(t, dirInfo.IsDir(), "Config path should point to a directory")
}

func TestGetConfigFilePathWithExt(t *testing.T) {
	// Test different extensions
	testCases := []struct {
		name     string
		ext      string
		expected string
	}{
		{"Empty", "", "config.json"}, // Default case
		{"JSON", "json", "config.json"},
		{"YAML", "yaml", "config.yaml"},
		{"TOML", "toml", "config.toml"},
		{"WithDot", ".yml", "config.yml"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configPath, err := GetConfigFilePathWithExt(tc.ext)
			require.NoError(t, err, "Failed to get config file path")

			homeDir, err := os.UserHomeDir()
			require.NoError(t, err, "Failed to get home directory")

			expectedPath := filepath.Join(homeDir, ".nmongo", tc.expected)
			assert.Equal(t, expectedPath, configPath, "Config file path should have the correct extension")
		})
	}
}
