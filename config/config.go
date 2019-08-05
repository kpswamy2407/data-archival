package config

import (
	"errors"
)

//ArchiveConfig is variable holds all the configuraion details of all clients
var ArchiveConfig map[string]map[string]string

//ArchiveTableConfig is variable holds all the configuraion details of all tables and its conditions
var ArchiveTableConfig map[string]map[string]map[string]string

func init() {
	ArchiveConfig = make(map[string]map[string]string)
	ArchiveConfig["one"] = map[string]string{
		"mysql_username":         "root",
		"mysql_password":         "",
		"mysql_host":             "127.0.0.1",
		"mysql_source_port":      "3306",
		"mysql_destination_port": "3306",
		"source_database":        "one",
		"destination_database":   "one",
	}
	/* 	ArchiveConfig["two"] = map[string]string{
		"mysql_username":         "root",
		"mysql_password":         "",
		"mysql_host":             "127.0.0.1",
		"mysql_source_port":      "3306",
		"mysql_destination_port": "3306",
		"source_database":        "two",
		"destination_database":   "two_backup",
	} */
	ArchiveTableConfig = make(map[string]map[string]map[string]string)
	ArchiveTableConfig["one"] = map[string]map[string]string{
		"table1": map[string]string{
			"source_table":      "table1",
			"destination_table": "table1_backup",
			"raw_query":         "",
			"condition":         "id>=1",
			"related_tables":    "",
		},
		/* "table2": map[string]string{
			"source_table":      "table2",
			"destination_table": "table2_backup",
			"raw_query":         "",
			"condition":         "id>=1",
		}, */
	}
	/* ArchiveTableConfig["two"] = map[string]map[string]string{
		"table1": map[string]string{
			"source_table":      "table1",
			"destination_table": "table1",
			"raw_query":         "",
			"condition":         "id>=1",
		},
		"table2": map[string]string{
			"source_table":      "table2",
			"destination_table": "table3",
			"raw_query":         "",
			"condition":         "id>=1",
		},
	} */

}

//GetArchiveConfig function,used to get the database configuration details of client
func GetArchiveConfig(client string) (configInfo map[string]string, err error) {
	configInfo, ok := ArchiveConfig[client]
	if ok {
		return configInfo, nil
	}
	return nil, errors.New("No configuration found the client:" + client)
}

//GetTableConfig function,used to get the table configuration details of client
func GetTableConfig(client string) (tableConfigInfo map[string]map[string]string, err error) {
	tableConfigInfo, ok := ArchiveTableConfig[client]
	if ok {
		return tableConfigInfo, nil
	}
	return nil, errors.New("No configuration found the client:" + client)
}
