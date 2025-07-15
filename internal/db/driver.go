package db

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/go-sql-driver/mysql"
)

type Table struct {
	Name         string
	HasTimestamp bool
	TimestampCol string
	Dependencies []string
}

func ConnectDB(config mysql.Config) (*sql.DB, error) {
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

func archiveOldData(db *sql.DB, tableName string, timestampCol string, cutoffDate time.Time) error {
	query := "SELECT * FROM ? WHERE ? < ?"

	rows, err := db.Query(query, tableName, timestampCol, cutoffDate)
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

func archiveRelatedData(db *sql.DB, tableName string, referencedTable string, referencedCol string, cutoffDate time.Time, filename string) error {
	query := fmt.Sprintf(`
		SELECT DISTINCT r.* FROM ? r 
		JOIN ? m ON r.%s = m.id 
		WHERE m.created_at < $1`,
		referencedCol)

	rows, err := db.Query(query, cutoffDate, tableName, referencedTable)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if !slices.Contains(columns, referencedCol) {
		return fmt.Errorf("Table %s doesn't contain column with the name %s", referencedTable, referencedCol)
	}

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
	_, err := db.Exec("DELETE FROM ? WHERE ? < ?", tableName, timestampCol, cutoffDate)
	return err
}

func deleteRelatedArchivedData(db *sql.DB, tableName string, referencedTable string, referencedCol string, cutoffDate time.Time) error {
	query := fmt.Sprintf(`
		DELETE FROM %s 
		WHERE %s IN (
			SELECT id FROM %s WHERE created_at < $1
		)`,
		tableName, referencedCol, referencedTable)

	_, err := db.Exec(query, cutoffDate)
	return err
}

func ArchiveAndCleanup(db *sql.DB, tables []Table, cutoffDate time.Time, outputDir string) error {
	for _, table := range tables {
		if !table.HasTimestamp {
			filename := fmt.Sprintf("%s/%s.csv", outputDir, table.Name)
			fmt.Printf("Archiving related data from %s...\n", table.Name)

			if err := archiveRelatedData(db, table.Name, "main_table", "main_table_id", cutoffDate, filename); err != nil {
				return fmt.Errorf("failed to archive %s: %v", table.Name, err)
			}
		}
	}

	for _, table := range tables {
		if table.HasTimestamp {
			fmt.Printf("Archiving old data from %s...\n", table.Name)

			if err := archiveOldData(db, table.Name, table.TimestampCol, cutoffDate); err != nil {
				return fmt.Errorf("failed to archive %s: %v", table.Name, err)
			}
		}
	}

	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]
		if !table.HasTimestamp {
			fmt.Printf("Deleting archived data from %s...\n", table.Name)
			if err := deleteRelatedArchivedData(db, table.Name, "main_table", "main_table_id", cutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v", table.Name, err)
			}
		}
	}

	for _, table := range tables {
		if table.HasTimestamp {
			fmt.Printf("Deleting archived data from %s...\n", table.Name)
			if err := deleteArchivedData(db, table.Name, table.TimestampCol, cutoffDate); err != nil {
				return fmt.Errorf("failed to delete from %s: %v", table.Name, err)
			}
		}
	}

	return nil
}
