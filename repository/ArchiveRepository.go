package repository

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	defaultConfig "github.com/kpswamy540/db.archive.system/config"
	"github.com/kpswamy540/db.archive.system/database"
	"github.com/kpswamy540/db.archive.system/helper"
	"github.com/kpswamy540/db.archive.system/settings"
)

//ProcessResponse holds process response
type ProcessResponse struct {
	Err     bool
	Message string
}

//VerifyGeneralConfig function is used to verify the general configuration host,port,source and destination databases
func VerifyGeneralConfig(client string, dbconfig map[string]string, tableConfigs map[string]map[string]string) (message string, error bool) {
	if dbconfig["mysql_source_host"] == dbconfig["mysql_destination_host"] && dbconfig["mysql_source_port"] == dbconfig["mysql_destination_port"] && dbconfig["mysql_source_database"] == dbconfig["mysql_destination_database"] {
		for archive, config := range tableConfigs {
			if config["source_table"] == config["destination_table"] {
				return " mysql port,source,destination databases and tables are same for " + archive, true
			}
		}
	}
	return "", false
}

//VerifyDatabaseConfig function is used to verify the all configuration details
func VerifyDatabaseConfig(client string, config map[string]string, isManual bool) (message string, error bool) {

	message = "Configuration verification - ok"
	error = false
	soureDB, err := database.GetConnection(config["mysql_source_username"], config["mysql_source_password"], config["mysql_source_host"], config["mysql_source_port"], config["mysql_source_database"])

	if err != nil {
		return "Source database error- " + err.Error(), true

	} else {
		defer soureDB.Close()
		if isManual == false {
			message, error = VerifySourceTableConfig(client, defaultConfig.ArchiveTableConfig[client], soureDB)
			if error {
				return message, error
			}
		}

	}

	destDB, err := database.GetConnection(config["mysql_destination_username"], config["mysql_destination_password"], config["mysql_destination_host"], config["mysql_destination_port"], config["mysql_destination_database"])

	if err != nil {
		return " Destination database error- " + err.Error(), true

	} else {
		defer destDB.Close()
		if isManual == false {
			message, error = VerifyDestinationTableConfig(client, defaultConfig.ArchiveTableConfig[client], destDB)
			if error {
				return message, error
			}
		}
	}
	return message, error
}

//VerifyDestinationTableConfig is function used to verify the configuration details of destintation table
func VerifyDestinationTableConfig(client string, tableConfigs map[string]map[string]string, destDB *sql.DB) (message string, error bool) {
	message = "Configuration verification - ok"
	error = false
	databaseName, errorMessage := database.GetDatabaseName(destDB)

	if len(databaseName) == 0 && len(errorMessage) > 0 {
		message = errorMessage
		error = true
	}
	for _, config := range tableConfigs {
		if config["create_destination_table_if_not_exists"] != "yes" {
			if !database.IsTableExists(config["destination_table"], destDB) {
				return config["destination_table"] + " table is not exists  in destination database " + databaseName, true

			}
		}

	}
	return message, error
}

//VerifySourceTableConfig function is used to verify the tabel level configurations
func VerifySourceTableConfig(client string, tableConfigs map[string]map[string]string, sourceDB *sql.DB) (message string, error bool) {
	message = "Configuration verification - ok"
	error = false
	databaseName, errorMessage := database.GetDatabaseName(sourceDB)

	if len(databaseName) == 0 && len(errorMessage) > 0 {
		message = errorMessage
		error = true
	} else {
		for archive, config := range tableConfigs {
			if !database.IsTableExists(config["source_table"], sourceDB) {
				return config["source_table"] + " table  is not exists in the source database " + databaseName, true

			} else {
				if len(config["raw_query"]) > 0 {
					_, err := sourceDB.Query(config["raw_query"])
					if err != nil {
						return config["raw_query"] + " invalid raw query for " + archive, true

					}

				}
				if len(config["condition"]) > 0 {
					query := "select count(1) as noofrows from " + config["source_table"] + " where " + config["condition"]
					if len(config["related_tables"]) > 0 {
						query = "select count(1) as noofrows from " + config["source_table"] + "," + config["related_tables"] + " where " + config["condition"]

					}
					_, conError := sourceDB.Query(query)

					if conError != nil {
						return config["condition"] + " invalid  condition on  " + config["source_table"] + " " + conError.Error(), true

					}
				}
			}

		}
	}

	return message, error
}

//DoArchive  function is used to do the archive process
func DoArchive(dbconfig map[string]map[string]string, tableconfig map[string]map[string]map[string]string) (affectedRows map[string]map[string]int, err error) {
	//start write log
	filename := settings.Logpath + "dbarchvie_" + time.Now().Format("2006-01-02") + ".csv"
	_, err = os.Stat(filename)
	if err != nil {
		head := "Start Time, Client Name, Tables Archived, End Time, Total Time\n"
		helper.WriteLog(filename, head)
	}
	var nooftables int
	for c, _ := range dbconfig {
		nooftables = nooftables + len(tableconfig[c])
	}
	// Wait group start here
	var twg sync.WaitGroup
	twg.Add(nooftables)
	tempAffectedRows := make(map[string]int)
	tempAffectedTables := make(map[string]map[string]int)
	for client, config := range dbconfig {
		//Go routine start
		go func(client string, config map[string]string) {

			clientTableConfig := tableconfig[client]

			for _, tableconfigurations := range clientTableConfig {
				//Go routine start here
				go func(tableconfigurations map[string]string) {

					if len(tableconfigurations["condition"]) > 0 {

						//Data archival start here
						startTime := time.Now()
						logdata := startTime.Format("2006-01-02 15:04:05") + ","
						logdata = logdata + client + ","
						sourceDB, _ := database.GetConnection(config["mysql_source_username"], config["mysql_source_password"], config["mysql_source_host"], config["mysql_source_port"], config["mysql_source_database"])
						//trascation for source database start here
						transaction, err := sourceDB.Begin()
						if err != nil {

						}
						destDB, _ := database.GetConnection(config["mysql_destination_username"], config["mysql_destination_password"], config["mysql_destination_host"], config["mysql_destination_port"], config["mysql_destination_database"])
						defer func() {
							sourceDB.Close()
							destDB.Close()
							twg.Done()
							//Wait group statu updated as completed here
						}()
						// if the source and destination servers are running on same instances i.e same port

						if config["mysql_source_host"] == config["mysql_destination_host"] && config["mysql_source_port"] == config["mysql_destination_port"] {
							if tableconfigurations["create_destination_table_if_not_exists"] == "yes" {
								destDB, _ := database.GetConnection(config["mysql_destination_username"], config["mysql_destination_password"], config["mysql_destination_host"], config["mysql_destination_port"], config["mysql_destination_database"])
								defer destDB.Close()

								createQuery := "create table IF NOT EXISTS " + tableconfigurations["destination_table"] + " like " + config["mysql_source_database"] + "." + tableconfigurations["source_table"]

								_, err := destDB.Exec(createQuery)
								if err != nil {
									fmt.Println(err.Error())
								}

							}
							//Get the no of rows from the source table
							countQuery := "select count(1) as noofrows from " + tableconfigurations["source_table"]
							rowRes, err := sourceDB.Query(countQuery)
							var noofrows int
							if err != nil {

							} else {
								for rowRes.Next() {
									rowRes.Scan(&noofrows)
									tempAffectedRows["rows_before_archival"] = noofrows
								}
							}
							destCountQuery := "select count(1) as noofrows from " + tableconfigurations["destination_table"]
							desRowRes, err := destDB.Query(destCountQuery)

							if err != nil {

							} else {
								for desRowRes.Next() {
									desRowRes.Scan(&noofrows)
									tempAffectedRows["destination_rows_before_archival"] = noofrows
								}
							}
							insertQuery := "insert into " + config["mysql_destination_database"] + "." + tableconfigurations["destination_table"] + " select * from " + tableconfigurations["source_table"] + " where " + tableconfigurations["condition"]
							if len(tableconfigurations["related_tables"]) > 0 {
								insertQuery = "insert into " + config["mysql_destination_database"] + "." + tableconfigurations["destination_table"] + " select " + tableconfigurations["source_table"] + ".* from " + tableconfigurations["source_table"] + "," + tableconfigurations["related_tables"] + " where " + tableconfigurations["condition"]

							}
							logdata = logdata + tableconfigurations["source_table"] + ","
							if insertRes, err := transaction.Exec(insertQuery); err != nil {
								transaction.Rollback()
							} else {
								noOfRowsAffected, _ := insertRes.RowsAffected()
								tempAffectedRows["rows_affected_archival"] = int(noOfRowsAffected)
								tempAffectedRows["rows_after_archival"] = tempAffectedRows["rows_before_archival"] - int(noOfRowsAffected)
								tempAffectedRows["destination_rows_after_archival"] = tempAffectedRows["destination_rows_before_archival"] + int(noOfRowsAffected)
							}
							deleteQuery := "delete from " + tableconfigurations["source_table"] + " where " + tableconfigurations["condition"]
							if len(tableconfigurations["related_tables"]) > 0 {
								deleteQuery = "delete " + tableconfigurations["source_table"] + ".* from " + tableconfigurations["source_table"] + "," + tableconfigurations["related_tables"] + " where " + tableconfigurations["condition"]
							}
							//trascation for source database end here
							if _, err := transaction.Exec(deleteQuery); err != nil {
								transaction.Rollback()
							}
						} else {
							// source and destination servers are running on different ports or instances

							rows, _ := sourceDB.Query("show create table " + tableconfigurations["source_table"])
							defer rows.Close()
							var (
								Create      string
								Table       string
								CreateQuery string
							)
							for rows.Next() {

								err := rows.Scan(&Table, &Create)
								if err != nil {
									fmt.Println(err.Error())
								} else {
									CreateQuery = Create
								}
							}
							//Create the destination table here
							CreateQuery = strings.Replace(CreateQuery, "`"+tableconfigurations["source_table"]+"`", " IF NOT EXISTS `"+tableconfigurations["destination_table"]+"`", 1)

							destDB.Exec(CreateQuery)
							//trascation for destionation database end here
							destTransaction, _ := destDB.Begin()
							// Select the data from source database
							selectQuery := " select * from " + tableconfigurations["source_table"] + " where " + tableconfigurations["condition"]
							if len(tableconfigurations["related_tables"]) > 0 {
								selectQuery = " select " + tableconfigurations["source_table"] + ".* from " + tableconfigurations["source_table"] + "," + tableconfigurations["related_tables"] + " where " + tableconfigurations["condition"]
							}
							selectRes, _ := sourceDB.Query(selectQuery)
							selectCols, _ := selectRes.Columns()
							insertColumns := strings.Join(selectCols, ",")
							insertValues := make([]string, len(selectCols)+1)
							insertTemp := strings.Join(insertValues, "?,")
							insertTemp = strings.Trim(insertTemp, ",")
							insertQuery := "insert into " + tableconfigurations["destination_table"] + "(" + insertColumns + ") values(" + insertTemp + ")"

							destTransaction.Prepare(insertQuery)
							vals := make([]interface{}, len(selectCols))
							for i, _ := range selectCols {
								vals[i] = new(sql.RawBytes)
							}
							tempAffectedRows["rows_affected_archival"] = len(vals)
							tempAffectedRows["rows_after_archival"] = tempAffectedRows["rows_before_archival"] - len(vals)
							for selectRes.Next() {
								err = selectRes.Scan(vals...)
								if err != nil {
									fmt.Println(err.Error())
								} else {
									_, er := destTransaction.Exec(insertQuery, vals...)
									if er != nil {
										fmt.Println(er.Error())
									}
								}

							}

							logdata = logdata + tableconfigurations["source_table"] + ","
							//trascation for destionation database end here
							destTransaction.Commit()
							deleteQuery := "delete from " + tableconfigurations["source_table"] + " where " + tableconfigurations["condition"]
							if len(tableconfigurations["related_tables"]) > 0 {
								deleteQuery = "delete " + tableconfigurations["source_table"] + ".* from " + tableconfigurations["source_table"] + "," + tableconfigurations["related_tables"] + " where " + tableconfigurations["condition"]
							}
							if _, err := sourceDB.Exec(deleteQuery); err != nil {

							}

						}
						//trascation for source database end here
						if err := transaction.Commit(); err != nil {

						}
						endTime := time.Now()
						logdata = logdata + endTime.Format("2006-01-02 15:04:05") + ","
						diff := endTime.Sub(startTime)

						logdata = logdata + strconv.FormatFloat(diff.Seconds(), 'f', 3, 64) + "\n"
						helper.WriteLog(filename, logdata)
						tempAffectedTables[tableconfigurations["source_table"]] = tempAffectedRows

					}

				}(tableconfigurations)
				//Go routine end here
			}

		}(client, config)
		//Go routine end here
	}
	twg.Wait()

	// Wait group end here
	return tempAffectedTables, nil

}

// SaveConfig is function is used to save the configuration of archival application of database
/* func SaveConfig(dbconfig map[string]map[string]string, tableconfig map[string]map[string]map[string]string) (err error) {
	//start write log
	for client, config := range dbconfig {

		defaultConfig.SetDatabaseConfig(client, config)
		clientTableConfig := tableconfig[client]
		for tablename, tableconfigurations := range clientTableConfig {
			defaultConfig.SetTableConfig(client, tablename, tableconfigurations)
		}
	}
	return nil
} */
