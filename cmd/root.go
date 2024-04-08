/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "archivator",
	Short: "CLI tool for archiving and restoring tables of MySQL databases",
	Long: `
Usage:
  archivator [command] [flags]

Available Commands:
  init        Create config with specified database host, port, username, password
  check       Test database connections
  archive     Archive tables data from primary database
  restore     Import archived tables data to restore database
  help        Help about a command

Flags:
  -c, --config        specify path to config file
`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
