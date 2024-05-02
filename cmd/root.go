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
	Use:     "archi",
	Version: "1.0.0",
	Short:   "CLI tool for archiving tables of MySQL databases using pt-archiver",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
