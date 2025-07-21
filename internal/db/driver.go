package db

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/fn3x/archivator/internal/helpers"
	"github.com/go-sql-driver/mysql"
)

type ArchiveConfig struct {
	DB         *sql.DB
	Tables     []Table
	CutoffDate time.Time
	OutputDir  string
	Limit      int32
}

type Table struct {
	Name            string
	TimestampCol    string
	RefColumn       string
	RefTable        string
	RefTimestampCol string
	Purge           bool
}

func NewArchiveConfig() *ArchiveConfig {
	return &ArchiveConfig{}
}

func ConnectDB(config *mysql.Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func archiveOldData(db *sql.DB, tableName string, timestampCol string, cutoffDate time.Time, limit int32) error {
	query := "SELECT * FROM ? WHERE ? < ? LIMIT ?"

	rows, err := db.Query(query, tableName, timestampCol, cutoffDate.Format(time.RFC3339), limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if !slices.Contains(columns, timestampCol) {
		return fmt.Errorf("Table %s doesn't contain column with the name %s", tableName, timestampCol)
	}

	fmt.Printf("Archiving old data from %s...\n", tableName)

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

func archiveRelatedData(config *ArchiveConfig, table Table, cutoffDate time.Time) error {
	query := `
		SELECT DISTINCT r.* FROM ?? r 
		JOIN ?? m ON r.?? = m.id 
		WHERE m.?? < ?
		LIMIT ?`

	rows, err := config.DB.Query(query, table.Name, table.RefTable, table.RefColumn, table.RefTimestampCol, cutoffDate.Format(time.RFC3339), config.Limit)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if !slices.Contains(columns, table.RefColumn) {
		return fmt.Errorf("Table %s doesn't contain column with the name %s", table.RefTable, table.RefColumn)
	}

	if !slices.Contains(columns, table.RefTimestampCol) {
		return fmt.Errorf("Table %s doesn't contain timestamp column with the name %s", table.RefTable, table.RefTimestampCol)
	}

	filename := fmt.Sprintf("%s/%s.csv", config.OutputDir, table.Name)
	fmt.Printf("Archiving related data from %s...\n", table.Name)

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

func deleteArchivedData(db *sql.DB, tableName string, timestampCol string, cutoffDate time.Time) error {
	_, err := db.Exec("DELETE FROM ?? WHERE ?? < ?", tableName, timestampCol, cutoffDate)
	fmt.Printf("Deleting archived data from %s...\n", tableName)
	return err
}

func deleteRelatedArchivedData(config *ArchiveConfig, table Table, cutoffDate time.Time) error {
	fmt.Printf("Deleting archived data from %s...\n", table.Name)
	_, err := config.DB.Exec(`DELETE FROM ?? 
		WHERE ?? IN (
			SELECT id FROM ?? WHERE ?? < ?
		)`, table.Name, table.RefColumn, table.RefTable, table.RefTimestampCol, cutoffDate.Format(time.RFC3339))
	return err
}

func Archive(config *ArchiveConfig) error {
	if err := helpers.AssertError(config.Limit == 0, "Expected rows limit to be greater than zero"); err != nil {
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
	}

	for _, table := range config.Tables {
		if table.TimestampCol == "" {
			if err := archiveRelatedData(config, table, config.CutoffDate); err != nil {
				return fmt.Errorf("failed to archive %s: %v", table.Name, err)
			}
		} else {
			if err := archiveOldData(config.DB, table.Name, table.TimestampCol, config.CutoffDate, config.Limit); err != nil {
				return fmt.Errorf("failed to archive %s: %v", table.Name, err)
			}
		}
	}

	for i := len(config.Tables) - 1; i >= 0; i-- {
		table := config.Tables[i]
		if table.TimestampCol == "" && table.Purge {
			if err := deleteRelatedArchivedData(config, table, config.CutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v", table.Name, err)
			}
		}
	}

	for _, table := range config.Tables {
		if table.TimestampCol == "" && table.Purge {
			if err := deleteArchivedData(config.DB, table.Name, table.TimestampCol, config.CutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v", table.Name, err)
			}
		}
	}

	return nil
}
