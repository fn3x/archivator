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

// archives to a file and returns slice of ids
func archiveOldData(db *sql.DB, tableName string, timestampCol string, cutoffDate time.Time, limit int32) ([]uint64, error) {
	ids := make([]uint64, 0, limit)

	cutoffFormatted := cutoffDate.Format(time.RFC3339)
	fmt.Printf("Archiving rows from %s with cutoff date %s\n", tableName, cutoffFormatted)

	query, args, err := sq.Select("*").
		From(tableName).
		Where(fmt.Sprintf("%s < ?", timestampCol), cutoffFormatted).
		Limit(uint64(limit)).
		OrderBy("id").
		ToSql()

	if err != nil {
		return ids, err
	}

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

	rows, err := db.Query(query, args...)
	if err != nil {
		return ids, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return ids, err
	}

	filename := fmt.Sprintf("archived_%s_till_%s_at_%s.csv", tableName, timestampCol, time.Now().UTC().Format(time.RFC3339))

	file, err := os.Create(filename)
	if err != nil {
		return ids, err
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
			return ids, err
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else if b, ok := val.([]byte); ok {
				record[i] = string(b)
			} else {
				record[i] = fmt.Sprintf("%v", val)
				if columns[i] == "id" {
					if v, ok := val.(uint64); ok {
						ids = append(ids, v)
					} else if v, ok := val.(int64); ok {
						ids = append(ids, uint64(v))
					} else if v, ok := val.(int32); ok {
						ids = append(ids, uint64(v))
					} else if v, ok := val.(uint32); ok {
						ids = append(ids, uint64(v))
					}
				}
			}
		}

		writer.Write(record)
	}

	return ids, nil
}

// archives to a file and returns slice of ids
func archiveRelatedData(db *sql.DB, table Table, cutoffDate time.Time, limit int32) ([]uint64, error) {
	ids := make([]uint64, 0, limit)

	cutoffFormatted := cutoffDate.Format(time.RFC3339)
	fmt.Printf("Archiving rows from %s with cutoff date %s\n", table.Name, cutoffFormatted)

	query, args, err := sq.
		Select(fmt.Sprintf("%s.*", table.Name)).
		From(table.Name).
		Join(fmt.Sprintf("%s ON %s.%s = %s.id", table.RefTable, table.Name, table.RefColumn, table.RefTable)).
		Where(fmt.Sprintf("%s.%s < ?", table.RefTable, table.RefTimestampCol), cutoffFormatted).
		Limit(uint64(limit)).
		OrderBy("id").
		ToSql()

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

	if err != nil {
		return ids, err
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return ids, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return ids, err
	}

	filename := fmt.Sprintf("archived_%s_till_%s_at_%s.csv", table.Name, cutoffFormatted, time.Now().UTC().Format(time.RFC3339))

	file, err := os.Create(filename)
	if err != nil {
		return ids, err
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
			return ids, err
		}

		record := make([]string, len(columns))
		for i, val := range values {
			if val == nil {
				record[i] = ""
			} else if b, ok := val.([]byte); ok {
				record[i] = string(b)
			} else {
				record[i] = fmt.Sprintf("%v", val)
				if columns[i] == "id" {
					if v, ok := val.(uint64); ok {
						ids = append(ids, v)
					} else if v, ok := val.(int64); ok {
						ids = append(ids, uint64(v))
					} else if v, ok := val.(int32); ok {
						ids = append(ids, uint64(v))
					} else if v, ok := val.(uint32); ok {
						ids = append(ids, uint64(v))
					}
				}
			}
		}

		writer.Write(record)
	}

	return ids, nil
}

func deleteArchivedData(db *sql.DB, table Table, ids []uint64) error {
	query, args, err := sq.
		Delete(table.Name).
		Where(sq.Eq{"id": ids}).
		ToSql()

	if err != nil {
		return err
	}

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

	_, err = db.Exec(query, args...)
	return err
}

func deleteRelatedArchivedData(db *sql.DB, table Table, ids []uint64) error {
	query, args, err := sq.
		Delete(table.Name).
		Where(sq.Eq{"id": ids}).
		ToSql()

	if err != nil {
		return err
	}

	fmt.Printf("Query:%s\nArgs:%+v\n\n", query, args)

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
		ids, err := archiveRelatedData(config.DB, table, config.CutoffDate, config.Limit)
		if err != nil {
			return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
		}

		if config.Purge {
			if err := deleteArchivedData(config.DB, table, ids); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}
	} else {
		ids, err := archiveOldData(config.DB, table.Name, table.TimestampCol, config.CutoffDate, config.Limit)
		if err != nil {
			return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
		}

		if config.Purge {
			if err := deleteRelatedArchivedData(config.DB, table, ids); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}
	}

	return nil
}

func ArchiveMany(config *ArchiveManyConfig) error {
	if err := helpers.AssertError(config.Limit > 0, "Expected rows limit to be greater than zero"); err != nil {
		return err
	}

	tablesIds := make([]struct {
		name string
		ids  []uint64
	}, 0, len(config.Tables))

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
			ids, err := archiveRelatedData(config.DB, table, config.CutoffDate, config.Limit)
			if err != nil {
				return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
			}

			if config.Purge {
				tablesIds = append(tablesIds, struct {
					name string
					ids  []uint64
				}{
					name: table.Name,
					ids:  ids,
				})
			}
		} else {
			ids, err := archiveOldData(config.DB, table.Name, table.TimestampCol, config.CutoffDate, config.Limit)
			if err != nil {
				return fmt.Errorf("failed to archive %s: %v\n", table.Name, err)
			}

			if config.Purge {
				tablesIds = append(tablesIds, struct {
					name string
					ids  []uint64
				}{
					name: table.Name,
					ids:  ids,
				})
			}
		}
	}

	if !config.Purge {
		return nil
	}

	for _, table := range config.Tables {
		var ids []uint64
	inner:
		for _, v := range tablesIds {
			if v.name == table.Name {
				ids = v.ids
				break inner
			}
		}

		if len(ids) == 0 {
			fmt.Printf("No ids found for table %s. Not deleting rows\n", table.Name)
			continue
		}

		if table.TimestampCol == "" {
			if err := deleteRelatedArchivedData(config.DB, table, ids); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}
	}

	for _, table := range config.Tables {
		var ids []uint64
	inner_related:
		for _, v := range tablesIds {
			if v.name == table.Name {
				ids = v.ids
				break inner_related
			}
		}

		if len(ids) == 0 {
			fmt.Printf("No ids found for table %s. Not deleting rows\n", table.Name)
			continue
		}

		if table.TimestampCol != "" {
			if err := deleteArchivedData(config.DB, table, ids); err != nil {
				return fmt.Errorf("failed to delete from %s: %v\n", table.Name, err)
			}
		}
	}

	return nil
}
