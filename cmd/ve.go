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

		relatedKeys, err := cmd.Flags().GetStringSlice("related-key")
		if err != nil {
			return err
		}

		relatedTables, err := cmd.Flags().GetStringSlice("related-table")
		if err != nil {
			return err
		}

		relatedTimestampCols, err := cmd.Flags().GetStringSlice("related-timestamp-col")
		if err != nil {
			return err
		}

		if len(relatedKeys) != len(relatedTables) || len(relatedKeys) != len(relatedTimestampCols) || len(relatedTables) != len(relatedTimestampCols) {
			return fmt.Errorf("number of related keys, tables and timestamp columns should match")
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
      ve --table=table_name --related-table='related_table' --related-key='related_key' --related-timestamp-col='timestamp_col' [--related-table='another_related_table' --related-key='another_related_key' --related-timestamp-col='timestamp_col']

Flags:
  	      --table                 table to archive
      -p, --purge                 delete targeted rows from source table
      -t, --related-table         name of the dependant table
      -k, --related-key           related key of the dependant table
      		--related-timestamp-col related timestamp column of the dependant table
      -h, --help                  show this
          --limit                 how many rows to archive
`)
	veCmd.Flags().String("table", "", "table to archive")
	veCmd.Flags().Bool("purge", false, "delete rows from source table")
	veCmd.Flags().String("limit", "100", "how many rows to archive")
	veCmd.Flags().StringSliceP("related-table", "t", []string{}, "name of the dependant table")
	veCmd.Flags().StringSliceP("related-key", "k", []string{}, "related key of the dependant table")
	veCmd.Flags().StringSlice("related-timestamp-col", []string{}, "related timestamp column of the dependant table")

	veCmd.MarkFlagsRequiredTogether("related-key", "related-table", "related-timestamp-col")

	rootCmd.AddCommand(veCmd)
}
