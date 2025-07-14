package db

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

var DB *sql.DB

func Connect(config mysql.Config) error {
	DB, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return err
	}

	pingErr := DB.Ping()

	return pingErr
}

func Query(table string, where string, limit int32) error {
	return nil
}

func QueryWithForeigns(mainTable string, )
