package go_library

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

func (d *DbRows) ExportToCsv(csvConfig CsvWriterOption) (string, int64, error) {
	var final_error error

	d.csvConfig = &csvConfig

	var folder_path string = csvConfig.FolderPath
	var file_name_pattern string = csvConfig.FileNamePattern
	var max_row_per_file int64 = csvConfig.MaxRowPerFile
	var csv_delimiter string = csvConfig.CsvDelimiter
	var hide_row_header bool = csvConfig.HideRowHeader
	var first_row_header string = csvConfig.FirstRowHeader
	var checksum_file_name string = csvConfig.ChecksumFileName
	var checksum_content_pattern string = csvConfig.ChecksumContentPattern

	if strings.TrimSpace(folder_path) == "" {
		folder_path = os.Getenv("TEMP_FOLDER_PATH")
	}

	if file_name_pattern == "" {
		if max_row_per_file <= 0 {
			file_name_pattern = strings.Replace(GenUUID(), "-", "", -1) + ".csv"
		} else {
			file_name_pattern = strings.Replace(GenUUID(), "-", "", -1) + "-{00i}"
		}
	}

	fmt.Println("folder_path:", folder_path)

	var file *os.File
	var datawriter *bufio.Writer

	var ls_delimited_filename string = ""
	var file_name string = ""
	var itter_file int64 = 0

	re := regexp.MustCompile(`\r?\n`)

	column_info := d.ColumnInfo
	num_row, err := d.FetchStringRows(func(index_no int64, arrColumn []*DbColumnInfo, mapRow map[string]string) bool {
		var lb_continue bool = true
		var lb_new_file bool = false

		if index_no == 0 || (max_row_per_file > 0 && index_no%max_row_per_file == 0) {
			if index_no > 0 {
				datawriter.Flush()
				file.Close()

				if ls_delimited_filename != "" {
					ls_delimited_filename += "|"
				}

				ls_delimited_filename += file_name
			}

			itter_file++
			lb_new_file = true
			file_name = strings.Replace(file_name_pattern, "{i}", fmt.Sprintf("%d", itter_file), -1)
			file_name = strings.Replace(file_name, "{0i}", fmt.Sprintf("%02d", itter_file), -1)
			file_name = strings.Replace(file_name, "{00i}", fmt.Sprintf("%03d", itter_file), -1)
			file_name = strings.Replace(file_name, "{000i}", fmt.Sprintf("%04d", itter_file), -1)
			file_name = strings.Replace(file_name, "{0000i}", fmt.Sprintf("%05d", itter_file), -1)

			err := os.Remove(folder_path + file_name)
			_ = err

			file, err = os.OpenFile(folder_path+file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {

				final_error = err
				lb_continue = false

			} else {

				datawriter = bufio.NewWriter(file)

			}
		}

		// Write Header
		var ls_line string = ""
		if lb_new_file && !hide_row_header {

			if strings.TrimSpace(first_row_header) != "" && first_row_header != "nil" && first_row_header != "<nil>" {
				ls_line = first_row_header
			} else {
				for j, col := range arrColumn {
					if j > 0 {
						ls_line += csv_delimiter
					}
					ls_line += strings.Replace(col.ColumnName, csv_delimiter, "_", -1)
				}
			}

			_, err := datawriter.WriteString(ls_line + "\n")
			if err != nil {

				final_error = err
				lb_continue = false

			}

		}

		// Write Data
		ls_line = ""
		for j, col := range arrColumn {
			if j > 0 {
				ls_line += csv_delimiter
			}

			current_data_line := mapRow[col.ColumnName]
			current_data_line = re.ReplaceAllString(current_data_line, "")
			current_data_line = strings.Replace(current_data_line, string(10), " ", -1)
			current_data_line = strings.Replace(current_data_line, string(13), " ", -1)
			current_data_line = strings.Replace(current_data_line, csv_delimiter, " ", -1)

			ls_line += strings.TrimSpace(current_data_line)
		}

		_, err := datawriter.WriteString(ls_line + "\n")
		if err != nil {

			final_error = err
			lb_continue = false

		}

		return lb_continue
	})

	if err != nil {
		final_error = err
	} else {

		if num_row > 0 {
			datawriter.Flush()
			file.Close()

			if ls_delimited_filename != "" {
				ls_delimited_filename += "|"
			}

			ls_delimited_filename += file_name
		} else {

			file_name = strings.Replace(file_name_pattern, "{i}", fmt.Sprintf("%d", itter_file), -1)
			file_name = strings.Replace(file_name, "{0i}", fmt.Sprintf("%02d", itter_file), -1)
			file_name = strings.Replace(file_name, "{00i}", fmt.Sprintf("%03d", itter_file), -1)
			file_name = strings.Replace(file_name, "{000i}", fmt.Sprintf("%04d", itter_file), -1)
			file_name = strings.Replace(file_name, "{0000i}", fmt.Sprintf("%05d", itter_file), -1)

			_ = os.Remove(folder_path + file_name)
			file, err = os.OpenFile(folder_path+file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				final_error = err
			} else {

				datawriter = bufio.NewWriter(file)

				ls_line := ""
				for x, col := range column_info {
					if x > 0 {
						ls_line += csv_delimiter
					}
					ls_line += col.ColumnName
				}

				_, err = datawriter.WriteString(ls_line + "\n")

				datawriter.Flush()
				file.Close()

				ls_delimited_filename = file_name
			}
		}

		// Create Checksum File
		if checksum_file_name != "" {
			file_name = checksum_file_name
			_ = os.Remove(folder_path + file_name)
			file, err = os.OpenFile(folder_path+file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				final_error = err
			}
			datawriter = bufio.NewWriter(file)

			var ls_line string = checksum_content_pattern
			ls_line = strings.Replace(ls_line, "{numrow}", fmt.Sprintf("%d", num_row), -1)

			_, err = datawriter.WriteString(ls_line)
			if err != nil {
				final_error = err
			}

			datawriter.Flush()
			file.Close()
		}
	}

	_ = column_info

	if final_error != nil {
		return "", num_row, final_error
	} else {
		return ls_delimited_filename, num_row, err
	}
}
