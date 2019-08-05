package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/kpswamy540/db.archive.system/settings"
	"gopkg.in/yaml.v2"

	"github.com/kpswamy540/db.archive.system/database"
	"github.com/kpswamy540/db.archive.system/repository"
)

//ArchiveController holds all the methods related to acrhive process
type ArchiveController struct {
}

// ArchiveManualRequest collects the request parameters for the ArchiveManual method.
type ArchiveManualRequest struct {
	DatabaseConfig map[string]map[string]string            `json:"database_config" yaml:"database_config"`
	TableConfig    map[string]map[string]map[string]string `json:"table_config" yaml:"table_config"`
}

// ArchiveManualResponse collects the response parameters for the ArchiveManual method.
type ArchiveManualResponse struct {
	StatusCode   int                       `json:"status_code"`
	Message      string                    `json:"message"`
	AffectedRows map[string]map[string]int `json:"affected_rows"`
}

//ArchiveManual function is used to do the archival proccess using manul input
func (a *ArchiveController) ArchiveManual(res http.ResponseWriter, req *http.Request) {

	var archiveManualRequest ArchiveManualRequest
	var archiveManualResponse ArchiveManualResponse
	json.NewDecoder(req.Body).Decode(&archiveManualRequest)

	var isError bool
	var verifyFlag bool
	verifyFlag = true
	isError = false
	//General configuration verfication start here
	if len(archiveManualRequest.DatabaseConfig) <= 0 {
		isError = true
		verifyFlag = false
		archiveManualResponse.StatusCode = 201
		archiveManualResponse.Message = "Invalid database configurations. Please check and try and again"
		json.NewEncoder(res).Encode(archiveManualResponse)

	}
	if len(archiveManualRequest.TableConfig) <= 0 {
		isError = true
		verifyFlag = false
		archiveManualResponse.StatusCode = 201
		archiveManualResponse.Message = "Invalid table configurations. Please check and try and again"
		json.NewEncoder(res).Encode(archiveManualResponse)
	}
	//General configuration verfication end here
	if isError == false {
		//Database  configuration verfication start here
		for client, dbConfig := range archiveManualRequest.DatabaseConfig {
			message, error := repository.VerifyGeneralConfig(client, dbConfig, archiveManualRequest.TableConfig[client])
			if error == true {
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = message
				json.NewEncoder(res).Encode(archiveManualResponse)
				verifyFlag = false
			} else {

				dvm, dbe := repository.VerifyDatabaseConfig(client, dbConfig, true)
				if dbe == true {
					archiveManualResponse.StatusCode = 201
					archiveManualResponse.Message = dvm
					json.NewEncoder(res).Encode(archiveManualResponse)
					verifyFlag = false
				} else {
					//base database verification end here
					// Table level configuration start here
					soureDB, _ := database.GetConnection(dbConfig["mysql_source_username"], dbConfig["mysql_source_password"], dbConfig["mysql_source_host"], dbConfig["mysql_source_port"], dbConfig["mysql_source_database"])
					stvm, stve := repository.VerifySourceTableConfig(client, archiveManualRequest.TableConfig[client], soureDB)
					defer soureDB.Close()
					if stve == true {
						archiveManualResponse.StatusCode = 201
						archiveManualResponse.Message = stvm
						json.NewEncoder(res).Encode(archiveManualResponse)
						verifyFlag = false
					} else {
						destDB, _ := database.GetConnection(dbConfig["mysql_destination_username"], dbConfig["mysql_destination_password"], dbConfig["mysql_destination_host"], dbConfig["mysql_destination_port"], dbConfig["mysql_destination_database"])
						defer destDB.Close()

						message, error = repository.VerifyDestinationTableConfig(client, archiveManualRequest.TableConfig[client], destDB)

						if error == true {
							archiveManualResponse.StatusCode = 201
							archiveManualResponse.Message = message
							json.NewEncoder(res).Encode(archiveManualResponse)
							verifyFlag = false
						}
					}
					// Table level configuration end here
				}
			}
		}
		// Data archival process start here
		if verifyFlag == true {
			affectedRows, err := repository.DoArchive(archiveManualRequest.DatabaseConfig, archiveManualRequest.TableConfig)
			if err != nil {
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = err.Error()
				json.NewEncoder(res).Encode(archiveManualResponse)
			}
			archiveManualResponse.StatusCode = 200
			archiveManualResponse.Message = "Process completed successfully!"
			archiveManualResponse.AffectedRows = affectedRows
			json.NewEncoder(res).Encode(archiveManualResponse)
		}
		// Data archival process end here
	}

}

// Test is used to test connection
func (a *ArchiveController) Test(res http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(res, "Hello, you've requested: %s\n", req.URL.Path)
}

// SaveConfig is functio is used to save the configuration for database
/* func (a *ArchiveController) SaveConfig(res http.ResponseWriter, req *http.Request) {
	var archiveManualRequest ArchiveManualRequest
	var archiveManualResponse ArchiveManualResponse
	json.NewDecoder(req.Body).Decode(&archiveManualRequest)

	var isError bool
	var verifyFlag bool
	verifyFlag = true
	isError = false
	if len(archiveManualRequest.DatabaseConfig) <= 0 {
		isError = true
		verifyFlag = false
		archiveManualResponse.StatusCode = 201
		archiveManualResponse.Message = "Invalid database configurations. Please check and try and again"
		json.NewEncoder(res).Encode(archiveManualResponse)

	}
	if len(archiveManualRequest.TableConfig) <= 0 {
		isError = true
		verifyFlag = false
		archiveManualResponse.StatusCode = 201
		archiveManualResponse.Message = "Invalid table configurations. Please check and try and again"
		json.NewEncoder(res).Encode(archiveManualResponse)
	}
	if isError == false {
		for client, dbConfig := range archiveManualRequest.DatabaseConfig {
			message, error := repository.VerifyGeneralConfig(client, dbConfig, archiveManualRequest.TableConfig[client])
			if error == true {
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = message
				json.NewEncoder(res).Encode(archiveManualResponse)
				verifyFlag = false
			} else {

				dvm, dbe := repository.VerifyDatabaseConfig(client, dbConfig, true)
				if dbe == true {
					archiveManualResponse.StatusCode = 201
					archiveManualResponse.Message = dvm
					json.NewEncoder(res).Encode(archiveManualResponse)
					verifyFlag = false
				} else {
					//base database verification completed
					soureDB, _ := database.GetConnection(dbConfig["mysql_source_username"], dbConfig["mysql_source_password"], dbConfig["mysql_source_host"], dbConfig["mysql_source_port"], dbConfig["mysql_source_database"])
					stvm, stve := repository.VerifySourceTableConfig(client, archiveManualRequest.TableConfig[client], soureDB)
					defer soureDB.Close()
					if stve == true {
						archiveManualResponse.StatusCode = 201
						archiveManualResponse.Message = stvm
						json.NewEncoder(res).Encode(archiveManualResponse)
						verifyFlag = false
					} else {
						destDB, _ := database.GetConnection(dbConfig["mysql_destination_username"], dbConfig["mysql_destination_password"], dbConfig["mysql_destination_host"], dbConfig["mysql_destination_port"], dbConfig["mysql_destination_database"])
						defer destDB.Close()

						message, error = repository.VerifyDestinationTableConfig(client, archiveManualRequest.TableConfig[client], destDB)

						if error == true {
							archiveManualResponse.StatusCode = 201
							archiveManualResponse.Message = message
							json.NewEncoder(res).Encode(archiveManualResponse)
							verifyFlag = false
						}
					}

				}
			}
		}
		if verifyFlag == true {
			err := repository.SaveConfig(archiveManualRequest.DatabaseConfig, archiveManualRequest.TableConfig)
			if err != nil {
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = err.Error()
				json.NewEncoder(res).Encode(archiveManualResponse)
			}
			fmt.Println(config.ArchiveConfig)
			fmt.Println(config.ArchiveTableConfig)
			archiveManualResponse.StatusCode = 200
			archiveManualResponse.Message = "Process completed successfully!"
			json.NewEncoder(res).Encode(archiveManualResponse)
		}
	}
} */

// UploadConfig function is used to upload the configuration files
func (a *ArchiveController) UploadConfig(res http.ResponseWriter, req *http.Request) {
	var archiveManualRequest ArchiveManualRequest
	var archiveManualResponse ArchiveManualResponse
	// file upload process start here
	req.ParseMultipartForm(32 << 20)
	file, handler, err := req.FormFile("file")
	if err != nil {
		archiveManualResponse.StatusCode = 201
		archiveManualResponse.Message = err.Error()
		json.NewEncoder(res).Encode(archiveManualResponse)

	} else {
		defer file.Close()
		byteValue, _ := ioutil.ReadAll(file)
		yamlParseErr := yaml.Unmarshal(byteValue, &archiveManualRequest)
		if yamlParseErr != nil {
			fmt.Println(yamlParseErr.Error())

		} else {
			fmt.Println(archiveManualRequest)
		}
		os.Exit(2)
		jsonParseErr := json.Unmarshal(byteValue, &archiveManualRequest)
		if jsonParseErr != nil {
			archiveManualResponse.StatusCode = 201
			archiveManualResponse.Message = err.Error()
			json.NewEncoder(res).Encode(archiveManualResponse)
		} else {
			err = ioutil.WriteFile(settings.ConfigUploadPath+handler.Filename, byteValue, 0666)
			if err != nil {
				fmt.Fprintf(res, "%v", err)
			}
			var isError bool
			var verifyFlag bool
			verifyFlag = true
			isError = false
			// file upload process end here and configuration verification start
			if len(archiveManualRequest.DatabaseConfig) <= 0 {
				isError = true
				verifyFlag = false
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = "Invalid database configurations. Please check and try and again"
				json.NewEncoder(res).Encode(archiveManualResponse)

			}
			if len(archiveManualRequest.TableConfig) <= 0 {
				isError = true
				verifyFlag = false
				archiveManualResponse.StatusCode = 201
				archiveManualResponse.Message = "Invalid table configurations. Please check and try and again"
				json.NewEncoder(res).Encode(archiveManualResponse)
			}
			if isError == false {
				for client, dbConfig := range archiveManualRequest.DatabaseConfig {
					message, error := repository.VerifyGeneralConfig(client, dbConfig, archiveManualRequest.TableConfig[client])
					if error == true {
						archiveManualResponse.StatusCode = 201
						archiveManualResponse.Message = message
						json.NewEncoder(res).Encode(archiveManualResponse)
						verifyFlag = false
					} else {

						dvm, dbe := repository.VerifyDatabaseConfig(client, dbConfig, true)
						if dbe == true {
							archiveManualResponse.StatusCode = 201
							archiveManualResponse.Message = dvm
							json.NewEncoder(res).Encode(archiveManualResponse)
							verifyFlag = false
						} else {
							//base database verification completed
							soureDB, _ := database.GetConnection(dbConfig["mysql_source_username"], dbConfig["mysql_source_password"], dbConfig["mysql_source_host"], dbConfig["mysql_source_port"], dbConfig["mysql_source_database"])
							stvm, stve := repository.VerifySourceTableConfig(client, archiveManualRequest.TableConfig[client], soureDB)
							defer soureDB.Close()
							if stve == true {
								archiveManualResponse.StatusCode = 201
								archiveManualResponse.Message = stvm
								json.NewEncoder(res).Encode(archiveManualResponse)
								verifyFlag = false
							} else {
								destDB, _ := database.GetConnection(dbConfig["mysql_destination_username"], dbConfig["mysql_destination_password"], dbConfig["mysql_destination_host"], dbConfig["mysql_destination_port"], dbConfig["mysql_destination_database"])
								defer destDB.Close()

								message, error = repository.VerifyDestinationTableConfig(client, archiveManualRequest.TableConfig[client], destDB)

								if error == true {
									archiveManualResponse.StatusCode = 201
									archiveManualResponse.Message = message
									json.NewEncoder(res).Encode(archiveManualResponse)
									verifyFlag = false
								}
							}

						}
					}
				}
				// data archival process start here
				if verifyFlag == true {
					affectedRows, err := repository.DoArchive(archiveManualRequest.DatabaseConfig, archiveManualRequest.TableConfig)
					if err != nil {
						archiveManualResponse.StatusCode = 201
						archiveManualResponse.Message = err.Error()
						json.NewEncoder(res).Encode(archiveManualResponse)
					}
					archiveManualResponse.StatusCode = 200
					archiveManualResponse.Message = "Process completed successfully!"
					archiveManualResponse.AffectedRows = affectedRows
					json.NewEncoder(res).Encode(archiveManualResponse)

				}
				// data archival process end here
			}
		}
	}

}
