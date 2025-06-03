package version

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInfo(t *testing.T) {
	// Save original values
	originalVersion := Version
	originalCommit := Commit
	originalDate := Date
	originalBuildNumber := BuildNumber

	// Set test values
	Version = "v1.2.3"
	Commit = "abc123def"
	Date = "2025-06-03_10:00:00"
	BuildNumber = "999"

	// Restore original values after test
	defer func() {
		Version = originalVersion
		Commit = originalCommit
		Date = originalDate
		BuildNumber = originalBuildNumber
	}()

	info := Info()

	// Check all expected components are present
	assert.Contains(t, info, "nmongo version v1.2.3")
	assert.Contains(t, info, "Commit: abc123def")
	assert.Contains(t, info, "Build number: 999")
	assert.Contains(t, info, "Built at: 2025-06-03_10:00:00")
	assert.Contains(t, info, "Go version: "+runtime.Version())
	assert.Contains(t, info, "OS/Arch: "+runtime.GOOS+"/"+runtime.GOARCH)

	// Verify format has multiple lines
	lines := strings.Split(info, "\n")
	assert.Equal(t, 6, len(lines), "Info should have exactly 6 lines")
}

func TestShort(t *testing.T) {
	// Save original value
	originalVersion := Version

	// Test different version values
	testCases := []string{
		"v1.0.0",
		"dev",
		"v2.3.4-alpha",
		"v0.0.1-rc1",
	}

	for _, tc := range testCases {
		Version = tc
		assert.Equal(t, tc, Short(), "Short() should return the exact version string")
	}

	// Restore original value
	Version = originalVersion
}

func TestDefaultValues(t *testing.T) {
	// Create new variables to test defaults
	var testVersion = Version
	var testCommit = Commit
	var testDate = Date
	var testBuildNumber = BuildNumber

	// Default values should be set
	assert.NotEmpty(t, testVersion)
	assert.NotEmpty(t, testCommit)
	assert.NotEmpty(t, testDate)
	assert.NotEmpty(t, testBuildNumber)
}
