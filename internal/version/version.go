package version

import (
	"fmt"
	"runtime"
)

var (
	// Version is the current version of nmongo
	Version = "dev"
	// Commit is the git commit hash
	Commit = "none"
	// Date is the build date
	Date = "unknown"
	// BuildNumber is the CI build number
	BuildNumber = "unknown"
)

// Info returns formatted version information
func Info() string {
	return fmt.Sprintf(`nmongo version %s
Commit: %s
Build number: %s
Built at: %s
Go version: %s
OS/Arch: %s/%s`,
		Version,
		Commit,
		BuildNumber,
		Date,
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
	)
}

// Short returns just the version string
func Short() string {
	return Version
}
