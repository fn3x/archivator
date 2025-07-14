/*
Copyright © 2025 fn3x <fn3x@proton.me>
*/
package cmd

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var veCmd = &cobra.Command{
	Use:   "ve",
	Short: "Archive tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("%+v\n\n%s", err, "To create config file:\n  archi config")
		}

		fks, err := cmd.Flags().GetStringSlice("foreign-key")
		if err != nil {
			return err
		}

		fts, err := cmd.Flags().GetStringSlice("foreign-table")
		if err != nil {
			return err
		}

		if len(fks) != len(fts) {
			return fmt.Errorf("number of foreign keys and foreign tables should match")
		}

		table, err := cmd.Flags().GetString("table")
		if err != nil {
			return err
		}

		limitFlag, err := cmd.Flags().GetString("limit")
		if err != nil {
			return err
		}

		limit, err := strconv.Atoi(limitFlag)
		if err != nil {
			return err
		}

		purge, err := cmd.Flags().GetBool("purge")
		if err != nil {
			return err
		}

		fmt.Printf("ve called with following arguments: --table=%s --limit=%d --purge=%t", table, limit, purge)

		return nil
	},
}

func init() {
	veCmd.SetUsageTemplate(`
Usage:
      ve --table=table_name --foreign-table='foreign_table' --foreign-key='foreign_key' [--foreign-table='another_foreign_table' --foreign-key='another_foreign_key']

Flags:
      -p, --purge           delete copied rows from source table
      -t, --foreign-table   name of the dependant table
      -k, --foreign-key     foreign key of the dependant table
      -h, --help            show this
          --table           table to archive
          --limit           how many rows to archive
`)
	veCmd.Flags().String("table", "", "table to archive")
	veCmd.Flags().Bool("purge", false, "delete rows from source table")
	veCmd.Flags().String("limit", "100", "how many rows to archive")
	veCmd.Flags().StringSliceP("foreign-table", "t", []string{}, "name of the dependant table")
	veCmd.Flags().StringSliceP("foreign-key", "k", []string{}, "foreign key of the dependant table")

	veCmd.MarkFlagsRequiredTogether("foreign-key", "foreign-table")

	rootCmd.AddCommand(veCmd)
}
