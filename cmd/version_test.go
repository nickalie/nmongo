package cmd

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"nmongo/internal/version"
)

func TestVersionCommand(t *testing.T) {
	// Save original values
	originalVersion := version.Version
	originalCommit := version.Commit
	originalDate := version.Date
	originalBuildNumber := version.BuildNumber

	// Set test values
	version.Version = "v1.0.0-test"
	version.Commit = "abc123"
	version.Date = "2025-01-01_00:00:00"
	version.BuildNumber = "42"

	// Restore original values after test
	defer func() {
		version.Version = originalVersion
		version.Commit = originalCommit
		version.Date = originalDate
		version.BuildNumber = originalBuildNumber
	}()

	// Capture output
	buf := new(bytes.Buffer)

	// Create a new root command for this test to avoid side effects
	testRootCmd := &cobra.Command{Use: "nmongo"}
	testVersionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println(version.Info())
		},
	}
	testRootCmd.AddCommand(testVersionCmd)
	testRootCmd.SetOut(buf)
	testRootCmd.SetErr(buf)
	testRootCmd.SetArgs([]string{"version"})

	err := testRootCmd.Execute()
	assert.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "nmongo version v1.0.0-test")
	assert.Contains(t, output, "Commit: abc123")
	assert.Contains(t, output, "Build number: 42")
	assert.Contains(t, output, "Built at: 2025-01-01_00:00:00")
	assert.Contains(t, output, "Go version:")
	assert.Contains(t, output, "OS/Arch:")
}

func TestVersionInfo(t *testing.T) {
	// Save original values
	originalVersion := version.Version
	originalCommit := version.Commit
	originalDate := version.Date
	originalBuildNumber := version.BuildNumber

	// Set test values
	version.Version = "v2.0.0"
	version.Commit = "def456"
	version.Date = "2025-06-03_12:00:00"
	version.BuildNumber = "100"

	// Restore original values after test
	defer func() {
		version.Version = originalVersion
		version.Commit = originalCommit
		version.Date = originalDate
		version.BuildNumber = originalBuildNumber
	}()

	info := version.Info()
	assert.Contains(t, info, "nmongo version v2.0.0")
	assert.Contains(t, info, "Commit: def456")
	assert.Contains(t, info, "Build number: 100")
	assert.Contains(t, info, "Built at: 2025-06-03_12:00:00")

	short := version.Short()
	assert.Equal(t, "v2.0.0", short)
}

func TestVersionCommandRegistered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "version" {
			found = true
			assert.Equal(t, "Print version information", cmd.Short)
			break
		}
	}
	assert.True(t, found, "version command should be registered")
}
