package cmd

import (
	"github.com/spf13/cobra"
	"nmongo/internal/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  `Print detailed version information including version number, commit hash, build number, and build time.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println(version.Info())
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
