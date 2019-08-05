package repository

import (
	"os"
	"path/filepath"

	"github.com/kpswamy540/db.archive.system/helper"

	"github.com/kpswamy540/db.archive.system/settings"
)

// ReportFile is structure holds of report file log
type ReportFile struct {
	Date     string `json:"date"`
	FileName string `json:"file_name"`
	FileSize string `json:"file_size"`
}

// GetReportLogs is used to get the log files
func GetReportLogs(limit int) (logFiles []ReportFile, err error) {
	err = filepath.Walk(settings.Logpath,
		func(path string, info os.FileInfo, err error) error {
			if info.IsDir() == false {
				logFiles = append(logFiles, ReportFile{
					Date:     info.ModTime().Format("2006-01-02 15:04:05"),
					FileName: info.Name(),
					FileSize: helper.ConvertSizeIntoReadableFormat(uint64(info.Size())),
				})
				if limit != 0 && len(logFiles) == limit {
					return nil
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return logFiles, nil
}

//GetAllConfig functio is used to get the configuration files
func GetAllConfig(limit int) (configFiles []ReportFile, err error) {
	err = filepath.Walk(settings.ConfigUploadPath,
		func(path string, info os.FileInfo, err error) error {
			if info.IsDir() == false {
				configFiles = append(configFiles, ReportFile{
					Date:     info.ModTime().Format("2006-01-02 15:04:05"),
					FileName: info.Name(),
					FileSize: helper.ConvertSizeIntoReadableFormat(uint64(info.Size())),
				})
				if limit != 0 && len(configFiles) == limit {
					return nil
				}
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	return configFiles, nil
}
