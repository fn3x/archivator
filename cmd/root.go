/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "archivator",
	Short: "CLI tool for archiving tables of MySQL databases using pt-archiver",
	Long: `
Usage:
  archivator [command] [flags]

Available Commands:
  init        Create config with specified database host, port, username, password
  archive     Archive tables
  help        Help about a command

Flags:
  -c, --config        specify path to config file
`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
