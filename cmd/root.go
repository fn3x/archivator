/*
Copyright © 2025 fn3x <fn3x@proton.me>
*/
package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "archi",
	Short: "Archive MySQL tables",
	Long:  `Archive MySQL tables`,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
}
