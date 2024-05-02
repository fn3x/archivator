/*
Copyright © 2024 Art P fn3x@proton.me
*/
package cmd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive tables",
	Long:  `Archive tables from source database to destination database specified in config using pt-archiver`,
	RunE: func(command *cobra.Command, args []string) error {
		if err := viper.ReadInConfig(); err != nil {
			return err
		}

		tables, err := command.Flags().GetStringSlice("table")
		if err != nil {
			return err
		}

		wheres, err := command.Flags().GetStringSlice("where")
		if err != nil {
			return err
		}

		if len(tables) != len(wheres) {
			return errors.New("Number of tables and where-clauses should match")
		}

		for i := 0; i < len(tables); i++ {
			args := []string{
				"--progress",
				"10",
				"--no-delete",
				"--share-lock",
				"--commit-each",
				"--statistics",
				"--why-quit",
				"--socket",
				viper.GetString("socket"),
				"--source",
				fmt.Sprintf("h=%s,D=%s,P=%s,u=%s,p=%s,t=%s",
					viper.GetString("source.host"),
					viper.GetString("source.db"),
					viper.GetString("source.port"),
					viper.GetString("source.user"),
					viper.GetString("source.password"),
					tables[i]),
				"--dest",
				fmt.Sprintf("h=%s,D=%s,P=%s,u=%s,p=%s,t=%s",
					viper.GetString("destination.host"),
					viper.GetString("destination.db"),
					viper.GetString("destination.port"),
					viper.GetString("destination.user"),
					viper.GetString("destination.password"),
					tables[i]),
				"--where",
				fmt.Sprintf("'%s'", wheres[i]),
			}

			cmd := exec.Command("pt-archiver", args...)
			var stdout, stderr bytes.Buffer

			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			if err := cmd.Run(); err != nil {
				return errors.New(fmt.Sprintf("%s\n%s", err.Error(), stderr.String()))
			}

			fmt.Fprintln(os.Stderr, stderr.String())
			fmt.Fprintln(os.Stdout, stdout.String())
		}

		return nil
	},
}

func init() {
	initConfig()
	archiveCmd.PersistentFlags().StringSliceP("table", "t", []string{}, "table to archive")
	archiveCmd.PersistentFlags().StringSliceP("where", "w", []string{}, "where-clause to filter rows")
	rootCmd.AddCommand(archiveCmd)
}
