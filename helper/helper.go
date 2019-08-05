package helper

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	// BYTE is constant represents the bytes
	BYTE = 1 << (10 * iota)
	// KILOBYTE is constant represents the kilobytesbytes
	KILOBYTE
	// MEGABYTE is constant represents the megabytes
	MEGABYTE
	// GIGABYTE is constant represents the gigabytes
	GIGABYTE
	// TERABYTE is constant represents the terabytes
	TERABYTE
)

//WriteLog is function is used to write the log, its takes the filename, data are parameters
func WriteLog(filename string, data string) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("creation error:" + err.Error())
	}
	if _, err := f.Write([]byte(data)); err != nil {
		fmt.Println("Write error:" + err.Error())
	}
	if err := f.Close(); err != nil {
		fmt.Println("Close error:" + err.Error())
	}
}

//ConvertSizeIntoReadableFormat is functio is used to the file size in readable fomrat
func ConvertSizeIntoReadableFormat(bytes uint64) string {
	unit := ""
	value := float64(bytes)

	switch {
	case bytes >= TERABYTE:
		unit = "TB"
		value = value / TERABYTE
	case bytes >= GIGABYTE:
		unit = "GB"
		value = value / GIGABYTE
	case bytes >= MEGABYTE:
		unit = "MB"
		value = value / MEGABYTE
	case bytes >= KILOBYTE:
		unit = "KB"
		value = value / KILOBYTE
	case bytes >= BYTE:
		unit = "B"
	case bytes == 0:
		return "0"
	}
	result := strconv.FormatFloat(value, 'f', 1, 64)
	result = strings.TrimSuffix(result, ".0")
	return result + unit
}
