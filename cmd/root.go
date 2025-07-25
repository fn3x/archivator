/*
Copyright © 2025 fn3x <fn3x@proton.me>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:     "archi",
	Short:   "Archive MySQL tables",
	Long:    `Cli tool to archive MySQL tables with timestamp columns as well as tables
with foreign keys pointing to the tables containing timestamp columns`,
	Version: "1.0.0",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
}
