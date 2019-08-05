package main

import (
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/kpswamy540/db.archive.system/controller"
)

//Application routes and server start here
func main() {
	archiveController := &controller.ArchiveController{}
	reportController := &controller.ReportController{}
	router := mux.NewRouter()
	router.HandleFunc("/test", archiveController.Test).Methods("GET")
	router.HandleFunc("/db/archive-manual", archiveController.ArchiveManual).Methods("POST", "OPTIONS")
	/* router.HandleFunc("/db/saveconfig", archiveController.SaveConfig).Methods("POST", "OPTIONS") */
	router.HandleFunc("/upload/config", archiveController.UploadConfig).Methods("POST", "OPTIONS")
	router.HandleFunc("/getallconfig", reportController.GetAllConfig).Methods("GET")
	router.HandleFunc("/reports/all", reportController.GetLogs).Methods("GET")
	router.HandleFunc("/reports/recent", reportController.GetLogs).Methods("GET")
	router.HandleFunc("/reports/download/{filename}", reportController.DownloadLogFile).Methods("GET")
	//Server port listening
	http.ListenAndServe(":8080", handlers.CORS(
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Content-Length"}),
		handlers.AllowedOrigins([]string{"*"}))(router))
}
