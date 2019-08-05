package database

import (
	"database/sql"

	// This is used for mysql driver
	_ "github.com/go-sql-driver/mysql"
)

//GetConnection is function used to get conection
func GetConnection(username, password, host, port, database string) (db *sql.DB, err error) {

	db, err = sql.Open("mysql", username+":"+password+"@tcp("+host+":"+port+")/"+database+"?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		return nil, err
	}
	return db, nil
}

//GetDatabaseName is function, used to get the database name of the connection
func GetDatabaseName(db *sql.DB) (dbname string, message string) {
	res, err := db.Query("select database() as dbname")
	if err != nil {
		return "", err.Error()
	} else {
		for res.Next() {
			res.Scan(&dbname)
		}
	}

	return dbname, ""
}

//IsTableExists is function ,used to check if the table exists in the database or not
func IsTableExists(table string, db *sql.DB) bool {
	rows, err := db.Query("show tables like '" + table + "'")
	if err != nil {
		return false
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		i++
	}
	if i == 0 {
		return false
	}

	return true
}
