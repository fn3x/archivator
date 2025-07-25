package db

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/fn3x/archivator/internal/helpers"
	"github.com/go-sql-driver/mysql"
)

type ArchiveConfig struct {
	DB         *sql.DB
	TargetDB   *sql.DB
	Table      Table
	CutoffDate time.Time
	OutputDir  string
	Limit      int32
	Purge      bool
}

type ArchiveManyConfig struct {
	DB         *sql.DB
	TargetDB   *sql.DB
	Tables     []Table
	CutoffDate time.Time
	OutputDir  string
	Limit      int32
	Purge      bool
}

type Table struct {
	Name            string
	TimestampCol    string
	RefColumn       string
	RefTable        string
	RefTimestampCol string
}

func NewArchiveConfig() *ArchiveConfig {
	return &ArchiveConfig{}
}

func NewArchiveManyConfig() *ArchiveManyConfig {
	return &ArchiveManyConfig{}
}

func ConnectDB(config *mysql.Config, ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func archiveOldData(db *sql.DB, tableName string, timestampCol string, cutoffDate time.Time, limit int32) error {
	cutoffFormatted := cutoffDate.Format(time.RFC3339)
	query, args, err := sq.Select("*").
		From(tableName).
		Where(fmt.Sprintf("%s < ?", timestampCol), cutoffFormatted).
		Limit(uint64(limit)).
		OrderBy("id").
		ToSql()

	if err != nil {
		return err
	}

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Printf("Archiving rows from %s with cutoff date %s\n", tableName, cutoffFormatted)

	filename := fmt.Sprintf("archived_%s_till_%s_at_%s.csv", tableName, timestampCol, time.Now().UTC().Format(time.RFC3339))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write(columns)

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else if b, ok := val.([]byte); ok {
				record[i] = string(b)
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		writer.Write(record)
	}

	return nil
}

func archiveRelatedData(db *sql.DB, table Table, cutoffDate time.Time, limit int32) error {
	cutoffFormatted := cutoffDate.Format(time.RFC3339)

	query, args, err := sq.
		Select(fmt.Sprintf("%s.*", table.Name)).
		From(table.Name).
		Join(fmt.Sprintf("%s ON %s.%s = %s.id", table.RefTable, table.Name, table.RefColumn, table.RefTable)).
		Where(fmt.Sprintf("%s.%s < ?", table.RefTable, table.RefTimestampCol), cutoffFormatted).
		Limit(uint64(limit)).
		ToSql()

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

	if err != nil {
		return err
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Printf("Archiving rows from %s with cutoff date %s\n", table.Name, cutoffFormatted)

	filename := fmt.Sprintf("archived_%s_till_%s_at_%s.csv", table.Name, cutoffFormatted, time.Now().UTC().Format(time.RFC3339))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write(columns)

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else if b, ok := val.([]byte); ok {
				record[i] = string(b)
			} else {
				record[i] = fmt.Sprintf("%v", val)
			}
		}

		writer.Write(record)
	}

	return nil
}

func deleteArchivedData(db *sql.DB, table Table, cutoffDate time.Time) error {
	cutoffFormatted := cutoffDate.Format(time.RFC3339)
	fmt.Printf("Deleting archived data from %s until %s...\n", table.Name, cutoffFormatted)

	query, args, err := sq.
		Delete(table.Name).
		Where(fmt.Printf("%s < %s", table.TimestampCol, cutoffFormatted)).
		ToSql()

	if err != nil {
		return err
	}

	_, err = db.Exec(query, args...)
	return err
}

func deleteRelatedArchivedData(db *sql.DB, table Table, cutoffDate time.Time) error {
	cutoffFormatted := cutoffDate.Format(time.RFC3339)
	fmt.Printf("Deleting archived data from %s until %s...\n", table.Name, cutoffFormatted)

	subquery := sq.
		Select("id").
		From(table.RefTable).
		Where(fmt.Sprintf("%s < %s", table.RefTimestampCol, cutoffFormatted))

	query, args, err := sq.
		Delete(table.Name).
		Where(sq.Expr(fmt.Sprintf("%s IN (?)", table.RefColumn), subquery)).
		ToSql()

	if err != nil {
		return err
	}

	_, err = db.Exec(query, args...)

	return err
}

func Archive(config *ArchiveConfig) error {
	if err := helpers.AssertError(config.Limit > 0, "Expected rows limit to be greater than zero"); err != nil {
		return err
	}

	table := config.Table

	if err := helpers.AssertError(table.Name != "", "Expected table to have a name"); err != nil {
		return err
	}

	if table.TimestampCol == "" {
		if err := helpers.AssertError(table.RefTable != "", "Expected table with no timestamp column to have reference table name"); err != nil {
			return err
		}

		if err := helpers.AssertError(table.RefColumn != "", "Expected table with no timestamp column to have reference column name"); err != nil {
			return err
		}

		if err := helpers.AssertError(table.RefTimestampCol != "", "Expected table with no timestamp column to have reference timestamp column name"); err != nil {
			return err
		}
	}

	if table.TimestampCol == "" {
		if err := archiveRelatedData(config.DB, table, config.CutoffDate, config.Limit); err != nil {
			return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
		}
	} else {
		if err := archiveOldData(config.DB, table.Name, table.TimestampCol, config.CutoffDate, config.Limit); err != nil {
			return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
		}
	}

	if table.TimestampCol == "" && config.Purge {
		if err := deleteRelatedArchivedData(config.DB, table, config.CutoffDate); err != nil {
			return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
		}
	}

	if table.TimestampCol == "" && config.Purge {
		if err := deleteArchivedData(config.DB, table, config.CutoffDate); err != nil {
			return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
		}
	}

	return nil
}

func ArchiveMany(config *ArchiveManyConfig) error {
	if err := helpers.AssertError(config.Limit > 0, "Expected rows limit to be greater than zero"); err != nil {
		return err
	}

	for _, table := range config.Tables {
		if err := helpers.AssertError(table.Name != "", "Expected table to have a name"); err != nil {
			return err
		}

		if table.TimestampCol == "" {
			if err := helpers.AssertError(table.RefTable != "", "Expected table with no timestamp column to have reference table name"); err != nil {
				return err
			}

			if err := helpers.AssertError(table.RefColumn != "", "Expected table with no timestamp column to have reference column name"); err != nil {
				return err
			}

			if err := helpers.AssertError(table.RefTimestampCol != "", "Expected table with no timestamp column to have reference timestamp column name"); err != nil {
				return err
			}
		}

		if table.TimestampCol == "" {
			if err := archiveRelatedData(config.DB, table, config.CutoffDate, config.Limit); err != nil {
				return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
			}
		} else {
			if err := archiveOldData(config.DB, table.Name, table.TimestampCol, config.CutoffDate, config.Limit); err != nil {
				return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
			}
		}
	}

	for _, table := range config.Tables {
		if table.TimestampCol == "" && config.Purge {
			if err := deleteRelatedArchivedData(config.DB, table, config.CutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}

		if table.TimestampCol == "" && config.Purge {
			if err := deleteArchivedData(config.DB, table, config.CutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}
	}

	return nil
}
