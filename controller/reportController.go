package controller

import (
	"encoding/json"
	"net/http"
	"os"
	"sort"

	"github.com/gorilla/mux"
	"github.com/kpswamy540/db.archive.system/repository"
	"github.com/kpswamy540/db.archive.system/settings"
)

//ReportController holds all the methods related to reports and log files details
type ReportController struct {
}

// ReportRequest collects the request parameters for the Report list method.
type ReportRequest struct {
	FileName string `json:"file_name"`
}

// ReportResponse collects the response parameters for the rport method.
type ReportResponse struct {
	StatusCode int                     `json:"status_code"`
	Message    string                  `json:"message"`
	Logs       []repository.ReportFile `json:"logs"`
}

// GetLogs function is used to get the log file details such as createdate,file name and its size
func (r *ReportController) GetLogs(res http.ResponseWriter, req *http.Request) {
	var reportReponse ReportResponse
	logFiles, err := repository.GetReportLogs(0)
	if err != nil {
		reportReponse.StatusCode = 201
		reportReponse.Message = err.Error()
		reportReponse.Logs = nil
	} else {
		if len(logFiles) > 0 {
			sort.SliceStable(logFiles, func(i, j int) bool { return logFiles[i].Date > logFiles[j].Date })
			reportReponse.StatusCode = 200
			reportReponse.Message = "List of files"
			reportReponse.Logs = logFiles
		} else {
			reportReponse.StatusCode = 201
			reportReponse.Message = "No log files found"
			reportReponse.Logs = nil
		}
	}
	json.NewEncoder(res).Encode(reportReponse)
}

// GetRecentLogs function is used to get the log file details such as createdate,file name and its size of recent
func (r *ReportController) GetRecentLogs(res http.ResponseWriter, req *http.Request) {
	var reportReponse ReportResponse
	logFiles, err := repository.GetReportLogs(5)
	if err != nil {
		reportReponse.StatusCode = 201
		reportReponse.Message = err.Error()
		reportReponse.Logs = nil
	} else {
		if len(logFiles) > 0 {
			sort.SliceStable(logFiles, func(i, j int) bool { return logFiles[i].Date > logFiles[j].Date })
			reportReponse.StatusCode = 200
			reportReponse.Message = "List of files"
			reportReponse.Logs = logFiles
		} else {
			reportReponse.StatusCode = 201
			reportReponse.Message = "No log files found"
			reportReponse.Logs = nil
		}
	}
	json.NewEncoder(res).Encode(reportReponse)
}

//GetAllConfig is function, used to get all configuration files
func (r *ReportController) GetAllConfig(res http.ResponseWriter, req *http.Request) {
	var reportReponse ReportResponse
	logFiles, err := repository.GetAllConfig(0)
	if err != nil {
		reportReponse.StatusCode = 201
		reportReponse.Message = err.Error()
		reportReponse.Logs = nil
	} else {
		if len(logFiles) > 0 {
			sort.SliceStable(logFiles, func(i, j int) bool { return logFiles[i].Date > logFiles[j].Date })
			reportReponse.StatusCode = 200
			reportReponse.Message = "List of files"
			reportReponse.Logs = logFiles
		} else {
			reportReponse.StatusCode = 201
			reportReponse.Message = "No log files found"
			reportReponse.Logs = nil
		}
	}
	json.NewEncoder(res).Encode(reportReponse)
}

//DownloadLogFile used to donwload the file
func (r *ReportController) DownloadLogFile(res http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	filename := vars["filename"]
	filePath := settings.Logpath + filename
	f, err := os.Open(filePath)
	if err != nil {
	}
	defer f.Close()
	_, err = f.Stat()
	if err != nil {

	}
	res.Header().Set("Content-Disposition", "attachment; filename="+filename)
	http.ServeFile(res, req, filePath)
}
