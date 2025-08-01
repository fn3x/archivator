/*
Copyright © 2025 fn3x <fn3x@proton.me>
*/
package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	database "github.com/fn3x/archivator/internal/db"
	"github.com/fn3x/archivator/internal/helpers"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var layouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
	time.RFC1123,
	time.RFC1123Z,
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"01/02/2006",
	"01-02-2006",
	"2006/01/02",
	"Jan 2, 2006",
	"January 2, 2006",
	"2006-01-02 15:04:05.000000",
}

var veCmd = &cobra.Command{
	Use:   "ve",
	Short: "Archive tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("%+v\n\n%s", err, "To create config file:\n  archi config")
		}

		table, err := cmd.Flags().GetString("table")
		if err != nil {
			return err
		}

		timestampCol, err := cmd.Flags().GetString("timestamp-col")
		if err != nil {
			return err
		}

		relatedKey, err := cmd.Flags().GetString("related-key")
		if err != nil {
			return err
		}

		relatedTable, err := cmd.Flags().GetString("related-table")
		if err != nil {
			return err
		}

		relatedTimestampCol, err := cmd.Flags().GetString("related-timestamp-col")
		if err != nil {
			return err
		}

		limit, err := cmd.Flags().GetInt32("limit")
		if err != nil {
			return err
		}

		cutoff, err := cmd.Flags().GetTime("cutoff")
		if err != nil {
			return err
		}

		purge, err := cmd.Flags().GetBool("purge")
		if err != nil {
			return err
		}

		code, err := cmd.Flags().GetString("code")
		if err != nil {
			return err
		}

		dbConfig := mysql.NewConfig()

		dbConfig.DBName = viper.GetString("source.db")
		dbConfig.Addr = fmt.Sprintf("%s:%d", viper.GetString("source.host"), viper.GetInt("source.port"))
		dbConfig.User = viper.GetString("source.user")
		dbConfig.Passwd = viper.GetString("source.password")
		dbConfig.Net = "tcp"

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		fmt.Print("Trying to connect to DB.. ")
		db, err := database.ConnectDB(dbConfig, ctx)

		if err != nil {
			fmt.Printf("Error connecting to DB: %+v", err)
			return nil
		}

		fmt.Print("Successfully connected to DB\n")

		if code != "" {
			tables, err := parseCode(code)
			if err != nil {
				fmt.Printf("Error parsing code: %+v", err)
				return nil
			}

			archiveConfig := database.NewArchiveManyConfig()
			archiveConfig.DB = db
			archiveConfig.Limit = limit
			archiveConfig.OutputDir = viper.GetString("outputDir")
			archiveConfig.CutoffDate = cutoff
			archiveConfig.Purge = purge

			archiveConfig.Tables = tables

			err = database.ArchiveMany(archiveConfig)
		} else {
			if err := helpers.AssertError(table != "", "--table must be present"); err != nil {
				return err
			}

			archiveConfig := database.NewArchiveConfig()
			archiveConfig.DB = db
			archiveConfig.Limit = limit
			archiveConfig.OutputDir = viper.GetString("outputDir")
			archiveConfig.CutoffDate = cutoff
			archiveConfig.Purge = purge

			archiveConfig.Table = database.Table{
				Name:            table,
				TimestampCol:    timestampCol,
				RefTable:        relatedTable,
				RefColumn:       relatedKey,
				RefTimestampCol: relatedTimestampCol,
			}

			err = database.Archive(archiveConfig)

			if err != nil {
				fmt.Printf("%+v", err)
				return nil
			}

			fmt.Print("Print the code for repeated processing with --code? (y/n) ")

			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			answer := scanner.Text()
			if scanner.Err() != nil {
				return scanner.Err()
			}

			if answer == "y" || answer == "Y" {
				code := ""

				if archiveConfig.Table.RefTable == "" {
					code = fmt.Sprintf("m:%s:%s;", archiveConfig.Table.Name, archiveConfig.Table.TimestampCol)
				} else {
					code = fmt.Sprintf("r:%s:%s:%s:%s;", archiveConfig.Table.Name, archiveConfig.Table.RefTable, archiveConfig.Table.RefColumn, archiveConfig.Table.RefTimestampCol)
				}

				fmt.Printf("Code: %s\n", code)
			}
		}

		if err != nil {
			fmt.Printf("%+v", err)
			return nil
		}

		return nil
	},
}

func init() {
	veCmd.SetUsageTemplate(`Usage:
      ve --table=table_name --timestamp-col=requestTime [--cutoff=2025-06-06 --limit=100 --purge]
      ve --table=table_name --related-table='related_table' --related-key='related_key' --related-timestamp-col='timestamp_col' [--cutoff=2025-06-06 --limit=100 --purge]
      ve --code=m:table_name:timestamp_col [--cutoff=2025-06-06 --limit=100 --purge]
      ve --code=r:table_name:relate_table:related_key:related_timestamp_col [--cutoff=2025-06-06 --limit=100 --purge]
      ve --code=m:table_name:timestamp_col;r:table_name:relate_table:related_key:related_timestamp_col [--cutoff=2025-06-06 --limit=100 --purge]

Flags:
      -p, --purge                 delete rows from the table(s) (default: false)
          --limit                 how many rows to archive from the table(s) (default: 100)
          --cutoff                cutoff timestamp (default: now)
          --table                 table to archive
          --timestamp-col         timestamp column of the table
          --related-table         name of the dependant table
          --related-key           foreign key of the dependant table
          --related-timestamp-col related timestamp column of the dependant table
          --code                  short format for appending with other codes
      -h, --help                  show this message
`)
	veCmd.Flags().String("table", "", "table to archive")
	veCmd.Flags().BoolP("purge", "p", false, "delete rows from the table")
	veCmd.Flags().Int32("limit", 100, "how many rows to archive")
	veCmd.Flags().String("timestamp-col", "", "timestamp column")
	veCmd.Flags().Time("cutoff", time.Now(), layouts, "cutoff timestamp")
	veCmd.Flags().String("related-table", "", "name of the dependant table")
	veCmd.Flags().String("related-key", "", "related key of the dependant table")
	veCmd.Flags().String("related-timestamp-col", "", "related timestamp column of the dependant table")
	veCmd.Flags().String("code", "", "short format for multiple tables")

	veCmd.MarkFlagsRequiredTogether("related-key", "related-table", "related-timestamp-col")

	veCmd.MarkFlagsMutuallyExclusive("timestamp-col", "related-table")
	veCmd.MarkFlagsMutuallyExclusive("timestamp-col", "related-key")
	veCmd.MarkFlagsMutuallyExclusive("timestamp-col", "related-timestamp-col")
	veCmd.MarkFlagsMutuallyExclusive("table", "code")
	veCmd.MarkFlagsMutuallyExclusive("timestamp-col", "code")
	veCmd.MarkFlagsMutuallyExclusive("related-table", "code")
	veCmd.MarkFlagsMutuallyExclusive("related-key", "code")
	veCmd.MarkFlagsMutuallyExclusive("related-timestamp-col", "code")

	rootCmd.AddCommand(veCmd)
}

func parseCode(code string) ([]database.Table, error) {
	tableSplits := strings.Split(code, ";")
	if len(tableSplits) == 0 {
		return nil, fmt.Errorf("no tables found")
	}

	var tables []database.Table

	for _, t := range tableSplits {
		if t == "" {
			continue
		}

		paramsSplits := strings.Split(t, ":")
		if len(paramsSplits) == 0 {
			return nil, fmt.Errorf("no params found")
		}

		if err := helpers.AssertError(paramsSplits[0] == "m" || paramsSplits[0] == "r", "must start with 'm' or 'r'"); err != nil {
			return nil, err
		}

		switch paramsSplits[0] {
		case "m":
			if len(paramsSplits) != 3 {
				return nil, fmt.Errorf("for 'm' table length of params must be 3")
			}

			table := paramsSplits[1]
			timestampCol := paramsSplits[2]

			tables = append(tables, database.Table{
				Name:         table,
				TimestampCol: timestampCol,
			})
		case "r":
			if len(paramsSplits) != 5 {
				return nil, fmt.Errorf("for 'r' table length of params must be 5")
			}

			table := paramsSplits[1]
			refTable := paramsSplits[2]
			refCol := paramsSplits[3]
			refTimestampCol := paramsSplits[4]

			tables = append(tables, database.Table{
				Name:            table,
				RefTable:        refTable,
				RefColumn:       refCol,
				RefTimestampCol: refTimestampCol,
			})
		}
	}

	return tables, nil
}
