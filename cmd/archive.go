/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive tables",
	Long:  `archives tables from source database to destination database specified in config using pt-archiver`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(
				os.Stderr,
				"Couldn't read from config file: %+v\n",
				err,
			)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)
	initConfig()
}
