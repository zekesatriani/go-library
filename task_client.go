package go_library

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ----- >> Base Task Client
type TaskClient struct {
	IsConnected bool
	base_url    string
	proxy       []string
}

type TcDbClient struct {
	IsConnected  bool
	taskClient   *TaskClient
	dbcon_type   string
	dbcon_host   string
	dbcon_port   string
	dbcon_dbname string
	dbcon_user   string
	dbcon_pass   string
}

type TcFtpClient struct {
	ftpClient *FtpClient
	// IsConnected bool
	taskClient *TaskClient
	// address     string
	// port        string
	// userName    string
	// password    string
	// sftp        bool
}

// ----- >> Init Task Client
func NewTaskClient(url string, proxy []string) (TaskClient, error) {
	taskClient := TaskClient{}
	var final_err error

	taskClient.IsConnected = false
	taskClient.base_url = url
	if proxy != nil {
		taskClient.proxy = proxy
	} else {
		taskClient.proxy = []string{}
	}

	// if len(taskClient.proxy) == 0 {
	_, err := taskClient.exec("/system/info", nil)
	if err != nil {
		final_err = err
	} else {
		taskClient.IsConnected = true
	}

	return taskClient, final_err
}

func (d *TaskClient) exec(url string, data map[string]string) (string, error) {
	var final_result string
	var final_err error

	App := App{}

	// --- >> Init Rest Client
	restClient := NewRestClient()

	var ls_base_url string = d.base_url
	var ls_url = url
	map_data := data
	if map_data == nil {
		map_data = map[string]string{}
	}

	if len(d.proxy) > 0 {
		var ls_delimited_proxy string = ""
		for i, this_url := range d.proxy {
			if i == 0 {
				ls_base_url = strings.TrimSpace(this_url)
			} else if i > 0 && strings.TrimSpace(this_url) != "" {
				if ls_delimited_proxy != "" {
					ls_delimited_proxy += "|"
				}
				ls_delimited_proxy += strings.TrimSpace(this_url)
			}
		}
		map_data["proxy_url"] = ls_delimited_proxy
		map_data["base_url"] = d.base_url
		map_data["path_url"] = ls_url
		ls_url = "/proxy"
	}

	// Var_dump("map_data1:", map_data)

	var token = ""
	var ls_data = ""
	jsonData, err := json.Marshal(map_data)
	if err != nil {
		final_err = err
	} else {
		ls_data = Encrypt(string(jsonData))
		token = Encrypt(MD5(ls_data) + "|" + fmt.Sprintf("%d", time.Now().UTC().UnixNano()))
	}

	// --- >> Set Header
	app_token := os.Getenv("APP-TOKEN")
	if app_token == "" {
		app_token = os.Getenv("APP_TOKEN")
	}
	restClient.SetHeader("app-token", app_token)
	restClient.SetHeader("token", token)

	// --- >> Hit end point : POST
	var result_post string = ""
	if url == "/file/download" {
		result_post, err = restClient.PostDownload(ls_base_url+"/api"+ls_url, "json", map[string]interface{}{"data": ls_data}, map_data["download_path"])
	} else {
		result_post, err = restClient.Post(ls_base_url+"/api"+ls_url, "json", map[string]interface{}{"data": ls_data})
	}
	if err != nil {
		final_err = err
	} else {
		// fmt.Println("result_post ["+url+"] : ", result_post)
		if result_post == "DOWNLOAD-OK" {
			final_result = result_post
		} else if result_post != "" {
			err := json.Unmarshal([]byte(result_post), &App.Response)
			if err != nil {
				final_err = err
				// fmt.Println("result_post ["+url+"] : ", result_post)
			} else if App.Response.Status == "fail" {
				final_err = errors.New(App.Response.Message)
			} else if fmt.Sprintf("%T", App.Response.Data) == "string" && App.Response.Data.(string) != "" {
				final_result = Decrypt(App.Response.Data.(string))
			} else {
				final_result = App.Response.Message
			}
		} else {
			final_result = result_post
		}
	}

	return final_result, final_err
}

// ----- >> DB Operation
func (d *TaskClient) NewDbClient(dbcon_type string, dbcon_host string, dbcon_port string, dbcon_dbname string, dbcon_user string, dbcon_pass string) (TcDbClient, error) {
	dbClient := TcDbClient{}
	var final_err error

	dbClient.IsConnected = false
	dbClient.taskClient = d
	dbClient.dbcon_type = dbcon_type
	dbClient.dbcon_host = dbcon_host
	dbClient.dbcon_port = dbcon_port
	dbClient.dbcon_dbname = dbcon_dbname
	dbClient.dbcon_user = dbcon_user
	dbClient.dbcon_pass = dbcon_pass

	_, err := d.exec("/db/connect", map[string]string{
		"dbcon_type":   dbcon_type,
		"dbcon_host":   dbcon_host,
		"dbcon_port":   dbcon_port,
		"dbcon_dbname": dbcon_dbname,
		"dbcon_user":   dbcon_user,
		"dbcon_pass":   dbcon_pass,
	})
	if err != nil {
		final_err = err
	} else {
		dbClient.IsConnected = true
	}

	return dbClient, final_err
}

func (d *TcDbClient) GetDatabases() ([]string, error) {
	var final_result []string
	var final_err error

	res, err := d.taskClient.exec("/db/get_databases", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) GetTables() ([]*DbTableInfo, error) {
	var final_result []*DbTableInfo
	var final_err error

	res, err := d.taskClient.exec("/db/get_tables", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) GetColumns(dbName string, tableName string) ([]*DbColumnInfo, error) {
	var final_result []*DbColumnInfo
	var final_err error

	res, err := d.taskClient.exec("/db/get_table_columns", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": dbName,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"table_name":   tableName,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) Query(asSQL string) ([]map[string]string, error) {
	var final_result []map[string]string
	var final_err error

	res, err := d.taskClient.exec("/db/query", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"sql":          asSQL,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) GetData(asSQL string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/db/get_data", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"sql":          asSQL,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) Paging(asSQL string, aiPage int64, aiPageSize int64) (DataPaging, error) {
	var final_result DataPaging
	var final_err error

	res, err := d.taskClient.exec("/db/paging", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"sql":          asSQL,
		"page":         fmt.Sprintf("%d", aiPage),
		"rows":         fmt.Sprintf("%d", aiPageSize),
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

// type ExcelClientWriterOption struct {
// 	FileNamePattern       string
// 	MaxRowPerFile         int64
// 	NumberTypeColumn      []string
// 	UseRowNo              bool
// 	FirstRowHeader        []string
// 	FirstRowHeaderMapping map[string]string
// }

func (d *TcDbClient) DbToExcel(sql string, opt ...ExcelClientWriterOption) (string, error) {
	var final_result string
	var final_err error

	option := ExcelClientWriterOption{MaxRowPerFile: 1000000}
	for _, item := range opt {
		option = item
		break
	}

	var delimited_first_row_header string = ""
	if option.FirstRowHeader != nil {
		for i, val := range option.FirstRowHeader {
			if i > 0 {
				delimited_first_row_header += "|"
			}
			delimited_first_row_header += fmt.Sprintf("%v", val)
		}
	}

	var delimited_first_row_header_mapping string = ""
	if delimited_first_row_header == "" && option.FirstRowHeaderMapping != nil {
		for key, val := range option.FirstRowHeaderMapping {
			if delimited_first_row_header_mapping != "" {
				delimited_first_row_header_mapping += "|"
			}
			delimited_first_row_header_mapping += key + "=" + val
		}
	}

	var use_row_no string = "0"
	if option.UseRowNo {
		use_row_no = "1"
	}

	res, err := d.taskClient.exec("/db/db_to_excel", map[string]string{
		"dbcon_type":               d.dbcon_type,
		"dbcon_host":               d.dbcon_host,
		"dbcon_port":               d.dbcon_port,
		"dbcon_dbname":             d.dbcon_dbname,
		"dbcon_user":               d.dbcon_user,
		"dbcon_pass":               d.dbcon_pass,
		"sql":                      sql,
		"file_name_pattern":        option.FileNamePattern,
		"max_row_per_file":         fmt.Sprintf("%d", option.MaxRowPerFile),
		"number_type_column":       strings.Join(option.NumberTypeColumn, ","),
		"use_row_no":               use_row_no,
		"first_row_header":         delimited_first_row_header,
		"first_row_header_mapping": delimited_first_row_header_mapping,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

// type CsvWriterOption struct {
// 	FileNamePattern  string
// 	MaxRowPerFile    int64
// 	NumberTypeColumn []string
// 	CsvDelimiter     string
// 	HideRowHeader    bool
// 	FirstRowHeader   string
// }

func (d *TcDbClient) DbToCsv(sql string, opt ...CsvWriterOption) (string, error) {
	var final_result string
	var final_err error

	option := CsvWriterOption{MaxRowPerFile: 0, HideRowHeader: false}
	for _, item := range opt {
		option = item
		break
	}

	res, err := d.taskClient.exec("/db/db_to_csv", map[string]string{
		"dbcon_type":        d.dbcon_type,
		"dbcon_host":        d.dbcon_host,
		"dbcon_port":        d.dbcon_port,
		"dbcon_dbname":      d.dbcon_dbname,
		"dbcon_user":        d.dbcon_user,
		"dbcon_pass":        d.dbcon_pass,
		"sql":               sql,
		"file_name_pattern": option.FileNamePattern,
		"max_row_per_file":  fmt.Sprintf("%d", option.MaxRowPerFile),
		"csv_delimiter":     option.CsvDelimiter,
		"hide_row_header":   strings.Replace(fmt.Sprintf("%v", option.HideRowHeader), "<nil>", "", -1),
		"first_row_header":  option.FirstRowHeader,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

func (d *TcDbClient) BulkImport(filename string, destination_table string, row_data_at int64) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/db/bulk_import", map[string]string{
		"dbcon_type":        d.dbcon_type,
		"dbcon_host":        d.dbcon_host,
		"dbcon_port":        d.dbcon_port,
		"dbcon_dbname":      d.dbcon_dbname,
		"dbcon_user":        d.dbcon_user,
		"dbcon_pass":        d.dbcon_pass,
		"filename":          filename,
		"destination_table": destination_table,
		"row_data_at":       fmt.Sprintf("%d", row_data_at),
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) Backup(filename string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/db/backup", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"filename":     filename,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) Restore(filename string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/db/restore", map[string]string{
		"dbcon_type":   d.dbcon_type,
		"dbcon_host":   d.dbcon_host,
		"dbcon_port":   d.dbcon_port,
		"dbcon_dbname": d.dbcon_dbname,
		"dbcon_user":   d.dbcon_user,
		"dbcon_pass":   d.dbcon_pass,
		"filename":     filename,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) TaskList() (map[string]interface{}, error) {
	var final_result map[string]interface{}
	var final_err error

	res, err := d.taskClient.exec("/db/task_list", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) TaskStatus(id string) (map[string]string, error) {
	var final_result map[string]string
	var final_err error

	res, err := d.taskClient.exec("/db/task_status", map[string]string{
		"id": id,
	})
	if err != nil {
		final_err = err
	} else {
		var res_map map[string]interface{}
		err := json.Unmarshal([]byte(res), &res_map)
		if err != nil {
			final_err = err
		} else {
			final_result = map[string]string{}
			for key, value := range res_map {
				xType := fmt.Sprintf("%T", value)
				if xType == "float" || xType == "float32" || xType == "float64" {
					float_value := value.(float64)
					if float_value-math.Floor(float_value) != 0 {
						final_result[key] = fmt.Sprintf("%.5f", value)
					} else {
						final_result[key] = fmt.Sprintf("%.0f", value)
					}
				} else if xType == "int" || xType == "int32" || xType == "int64" {
					final_result[key] = fmt.Sprintf("%d", value)
				} else {
					final_result[key] = fmt.Sprintf("%v", value)
				}
			}
		}
	}

	return final_result, final_err
}

func (d *TcDbClient) TaskAbort(id string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/db/task_abort", map[string]string{
		"id": id,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

// ----- >> FTP Operation
func (d *TaskClient) NewFtpClient(address string, port int, userName string, password string, sftp ...bool) (TcFtpClient, error) {
	tcFtpClient := TcFtpClient{
		ftpClient: &FtpClient{
			isConnected: false,
			Address:     address,
			Port:        port,
			UserName:    userName,
			Password:    password,
			Sftp:        false,
		},
		taskClient: d,
	}
	var final_err error

	if sftp != nil && len(sftp) > 0 {
		(*tcFtpClient.ftpClient).Sftp = sftp[0]
	}

	_, err := d.exec("/ftp/connect", map[string]string{
		"host":     address,
		"port":     fmt.Sprintf("%d", port),
		"user":     userName,
		"password": password,
		"sftp":     strings.Replace(fmt.Sprintf("%v", (*tcFtpClient.ftpClient).Sftp), "<nil>", "", -1),
	})
	if err != nil {
		final_err = err
	} else {
		(*tcFtpClient.ftpClient).isConnected = true
	}

	return tcFtpClient, final_err
}

func (d *TcFtpClient) Download(ftp_source string, local_destination string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/ftp/download", map[string]string{
		"host":              (*d.ftpClient).Address,
		"port":              fmt.Sprintf("%d", (*d.ftpClient).Port),
		"user":              (*d.ftpClient).UserName,
		"password":          (*d.ftpClient).Password,
		"sftp":              strings.Replace(fmt.Sprintf("%v", (*d.ftpClient).Sftp), "<nil>", "", -1),
		"ftp_source":        ftp_source,
		"local_destination": local_destination,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TcFtpClient) Upload(local_source string, ftp_destination string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.taskClient.exec("/ftp/upload", map[string]string{
		"host":            (*d.ftpClient).Address,
		"port":            fmt.Sprintf("%d", (*d.ftpClient).Port),
		"user":            (*d.ftpClient).UserName,
		"password":        (*d.ftpClient).Password,
		"sftp":            strings.Replace(fmt.Sprintf("%v", (*d.ftpClient).Sftp), "<nil>", "", -1),
		"local_source":    local_source,
		"ftp_destination": ftp_destination,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TcFtpClient) TaskList() (map[string]map[string]map[string]string, error) {
	var final_result map[string]map[string]map[string]string
	var final_err error

	res, err := d.taskClient.exec("/ftp/task_list", map[string]string{
		"host":     (*d.ftpClient).Address,
		"port":     fmt.Sprintf("%d", (*d.ftpClient).Port),
		"user":     (*d.ftpClient).UserName,
		"password": (*d.ftpClient).Password,
		"sftp":     strings.Replace(fmt.Sprintf("%v", (*d.ftpClient).Sftp), "<nil>", "", -1),
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TcFtpClient) TaskStatus(id string) (map[string]string, error) {
	var final_result map[string]string
	var final_err error

	res, err := d.taskClient.exec("/ftp/task_status", map[string]string{
		"host":     (*d.ftpClient).Address,
		"port":     fmt.Sprintf("%d", (*d.ftpClient).Port),
		"user":     (*d.ftpClient).UserName,
		"password": (*d.ftpClient).Password,
		"sftp":     strings.Replace(fmt.Sprintf("%v", (*d.ftpClient).Sftp), "<nil>", "", -1),
		"id":       id,
	})
	if err != nil {
		final_err = err
	} else {
		var res_map map[string]interface{}
		err := json.Unmarshal([]byte(res), &res_map)
		if err != nil {
			final_err = err
		} else {
			final_result = map[string]string{}
			for key, value := range res_map {
				xType := fmt.Sprintf("%T", value)
				if xType == "float" || xType == "float32" || xType == "float64" {
					float_value := value.(float64)
					if float_value-math.Floor(float_value) != 0 {
						final_result[key] = fmt.Sprintf("%.5f", value)
					} else {
						final_result[key] = fmt.Sprintf("%.0f", value)
					}
				} else if xType == "int" || xType == "int32" || xType == "int64" {
					final_result[key] = fmt.Sprintf("%d", value)
				} else {
					final_result[key] = fmt.Sprintf("%v", value)
				}
			}
		}
	}

	return final_result, final_err
}

// ----- >> File Operation
func (d *TaskClient) FileExists(filename string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/file/exists", map[string]string{
		"filename": filename,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileRename(filename string, new_filename string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/file/rename", map[string]string{
		"filename":     filename,
		"new_filename": new_filename,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileDelete(filename string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/file/delete", map[string]string{
		"filename": filename,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileCompress(filename []string, delete_after_compress bool) (string, error) {
	var final_result string
	var final_err error

	var ls_delete_after_compress string = "0"
	if delete_after_compress {
		ls_delete_after_compress = "1"
	}
	res, err := d.exec("/file/compress", map[string]string{
		"filename":              strings.Join(filename[:], "|"),
		"delete_after_compress": ls_delete_after_compress,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileExtract(filename string, delete_after_extract bool) (string, error) {
	var final_result string
	var final_err error

	var ls_delete_after_extract string = "0"
	if delete_after_extract {
		ls_delete_after_extract = "1"
	}
	res, err := d.exec("/file/extract", map[string]string{
		"filename":             filename,
		"delete_after_extract": ls_delete_after_extract,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileDownload(filename string, download_path string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/file/download", map[string]string{
		"filename":      filename,
		"download_path": download_path,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileUpload(file_path string) (string, error) {
	var final_result string
	var final_err error

	// Check File Exists
	fi, err := os.Stat(file_path)
	if errors.Is(err, os.ErrNotExist) {
		final_err = errors.New("File Not Found")
	} else if err != nil {
		final_err = err
	} else {

		// Setting Chunk Size (MB)
		var part_size int64 = 1024 // MB

		total_size := fi.Size()
		var total_part int64 = int64(math.Ceil(float64(total_size) / float64(part_size*1024)))
		_ = total_part

		// Open File
		file, err := os.Open(file_path)
		defer file.Close()
		if err != nil {
			final_err = err
		} else {

			// Set New Filename
			var file_ext string = strings.Replace(strings.ToLower(filepath.Ext(file_path)), ".", "", -1)
			var new_file_name = GenUUID() + "." + file_ext

			var index_part int64 = 0
			for index_part = 0; index_part < total_part; index_part++ {

				// Read Chunked File
				partSize := int(math.Ceil((math.Min(float64(part_size*1024), float64(total_size-int64(uint64(index_part)*uint64(part_size*1024)))))))
				if partSize <= 0 {
					break
				}
				partBuffer := make([]byte, partSize)
				file.Read(partBuffer)

				// Send File via rest
				res, err := d.fileUploadHttpRequest(d.base_url+"/api/file/upload", file_path, new_file_name, partBuffer, index_part, total_part)
				if err != nil {
					final_err = err
					break
				} else {

					var objmap map[string]interface{}
					err := json.Unmarshal([]byte(res), &objmap)

					if err != nil {
						final_err = err
						break
					} else {
						var ls_status string = ""
						var ls_message string = ""

						if _, ok := objmap["status"]; ok {
							ls_status = fmt.Sprintf("%v", objmap["status"])
						}

						if _, ok := objmap["message"]; ok {
							ls_message = fmt.Sprintf("%v", objmap["message"])
						}

						if ls_status != "success" {
							if ls_message != "" {
								final_err = errors.New("Err rest response : " + ls_message)
							} else {
								final_err = errors.New("Err rest upload")
							}
							break
						}
					}
					_ = res
					// fmt.Println("res ["+fmt.Sprintf("%d", index_part)+"] :", res)
				}

			}

			final_result = new_file_name
		}
	}

	return final_result, final_err
}

func (d *TaskClient) fileUploadHttpRequest(url string, file_path string, new_file_name string, partBuffer []byte, index_part int64, total_part int64) (string, error) {
	var final_result string
	var final_err error

	client := &http.Client{
		Timeout: time.Second * 10,
	}
	// New multipart writer.
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	fw, err := writer.CreateFormField("index_part")
	if err != nil {
	}
	_, err = io.Copy(fw, strings.NewReader(fmt.Sprintf("%d", index_part)))
	if err != nil {
		return "", err
	}

	fw, err = writer.CreateFormField("total_part")
	if err != nil {
	}
	_, err = io.Copy(fw, strings.NewReader(fmt.Sprintf("%d", total_part)))
	if err != nil {
		return "", err
	}

	fw, err = writer.CreateFormField("name")
	if err != nil {
	}
	_, err = io.Copy(fw, strings.NewReader(new_file_name))
	if err != nil {
		return "", err
	}

	fw, err = writer.CreateFormFile("file", file_path)
	if err != nil {
	}
	fw.Write(partBuffer)

	// Close multipart writer.
	writer.Close()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body.Bytes()))
	if err != nil {
		return "", err
	}

	var token = ""
	var ls_data = ""
	map_data := make(map[string]string)

	jsonData, err := json.Marshal(map_data)
	if err != nil {
		final_err = err
	} else {
		ls_data = Encrypt(string(jsonData))
		token = Encrypt(MD5(ls_data) + "|" + fmt.Sprintf("%d", time.Now().UTC().UnixNano()))

		// --- >> Set Header
		app_token := os.Getenv("APP-TOKEN")
		if app_token == "" {
			app_token = os.Getenv("APP_TOKEN")
		}
		req.Header.Set("app-token", app_token)
		req.Header.Set("token", token)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, _ := client.Do(req)
		if res.StatusCode != http.StatusOK {
			final_err = errors.New(fmt.Sprintf("Request failed with response code: %d", res.StatusCode))
		} else {
			defer res.Body.Close()

			body, err := io.ReadAll(res.Body)
			if err != nil {
				// fmt.Println(err)
				final_err = err
			}
			final_result = string(body)
			// fmt.Println(final_result)
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileTaskList() (map[string]map[string]map[string]interface{}, error) {
	var final_result map[string]map[string]map[string]interface{}
	var final_err error

	res, err := d.exec("/file/task_list", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TaskClient) FileTaskStatus(id string) (map[string]string, error) {
	var final_result map[string]string
	var final_err error

	res, err := d.exec("/file/task_status", map[string]string{
		"id": id,
	})
	if err != nil {
		final_err = err
	} else {
		var res_map map[string]interface{}
		err := json.Unmarshal([]byte(res), &res_map)
		if err != nil {
			final_err = err
		} else {
			final_result = map[string]string{}
			for key, value := range res_map {
				xType := fmt.Sprintf("%T", value)
				if xType == "float" || xType == "float32" || xType == "float64" {
					float_value := value.(float64)
					if float_value-math.Floor(float_value) != 0 {
						final_result[key] = fmt.Sprintf("%.5f", value)
					} else {
						final_result[key] = fmt.Sprintf("%.0f", value)
					}
				} else if xType == "int" || xType == "int32" || xType == "int64" {
					final_result[key] = fmt.Sprintf("%d", value)
				} else {
					final_result[key] = fmt.Sprintf("%v", value)
				}
			}
		}
	}

	return final_result, final_err
}

// "exists",
// "images",
// "download",
// "upload",

// ----- >> Rest
type RestClient struct {
	IsConnected bool
	taskClient  *TaskClient
	header      map[string]string
}

func (d *TaskClient) NewRestClient() RestClient {
	restClient := RestClient{}

	restClient.taskClient = d
	restClient.header = make(map[string]string)

	return restClient
}

func (d *RestClient) SetHeader(key string, value string) {
	d.header[key] = value
}

func (d *RestClient) SetHeaders(header map[string]string) {
	d.header = header
}

func (d *RestClient) RemoveHeader(key string) {
	var _, isExist = d.header[key]
	if isExist {
		delete(d.header, key)
	}
}

func (d *RestClient) ClearHeaders() {
	d.header = make(map[string]string)
}

func (d *RestClient) GetHeader(key string) (string, bool) {
	var result, isExist = d.header[key]
	return result, isExist
}

func (d *RestClient) GetHeaders() map[string]string {
	return d.header
}

func (d *RestClient) Get(rest_url string, data map[string]string) (string, error) {
	var final_result string
	var final_err error

	if data != nil {
		values := url.Values{}
		for key, val := range data {
			if strings.TrimSpace(key) != "" && strings.TrimSpace(val) != "" {
				values.Add(key, val)
			}
		}
		query := values.Encode()

		if strings.Contains(rest_url, "?") {
			rest_url += "&" + query
		} else {
			rest_url += "?" + query
		}
	}

	var json_headers string = ""
	if len(d.header) > 0 {
		jsonStr, err := json.Marshal(d.header)
		if err != nil {
			final_err = err
		} else {
			json_headers = string(jsonStr)
		}
	}

	res, err := d.taskClient.exec("/rest/get", map[string]string{
		"url":       rest_url,
		"method":    "GET",
		"form_type": "",
		"headers":   json_headers,
		"body":      "",
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *RestClient) Post(rest_url string, form_type string, data map[string]interface{}) (string, error) {
	var final_result string
	var final_err error

	var json_headers string = ""
	if len(d.header) > 0 {
		jsonStr, err := json.Marshal(d.header)
		if err != nil {
			final_err = err
		} else {
			json_headers = string(jsonStr)
		}
	}

	var json_data string = ""
	if data != nil && len(data) > 0 {
		jsonStr, err := json.Marshal(data)
		if err != nil {
			final_err = err
		} else {
			json_data = string(jsonStr)
		}
	}

	res, err := d.taskClient.exec("/rest/post", map[string]string{
		"url":       rest_url,
		"method":    "POST",
		"form_type": form_type,
		"headers":   json_headers,
		"body":      json_data,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *RestClient) Put(rest_url string, form_type string, data map[string]interface{}) (string, error) {
	var final_result string
	var final_err error

	var json_headers string = ""
	if len(d.header) > 0 {
		jsonStr, err := json.Marshal(d.header)
		if err != nil {
			final_err = err
		} else {
			json_headers = string(jsonStr)
		}
	}

	var json_data string = ""
	if data != nil && len(data) > 0 {
		jsonStr, err := json.Marshal(data)
		if err != nil {
			final_err = err
		} else {
			json_data = string(jsonStr)
		}
	}

	res, err := d.taskClient.exec("/rest/put", map[string]string{
		"url":       rest_url,
		"method":    "PUT",
		"form_type": form_type,
		"headers":   json_headers,
		"body":      json_data,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *RestClient) Delete(rest_url string, data map[string]string) (string, error) {
	var final_result string
	var final_err error

	if data != nil {
		values := url.Values{}
		for key, val := range data {
			if strings.TrimSpace(key) != "" && strings.TrimSpace(val) != "" {
				values.Add(key, val)
			}
		}
		query := values.Encode()

		if strings.Contains(rest_url, "?") {
			rest_url += "&" + query
		} else {
			rest_url += "?" + query
		}
	}

	var json_headers string = ""
	if len(d.header) > 0 {
		jsonStr, err := json.Marshal(d.header)
		if err != nil {
			final_err = err
		} else {
			json_headers = string(jsonStr)
		}
	}

	res, err := d.taskClient.exec("/rest/delete", map[string]string{
		"url":       rest_url,
		"method":    "DELETE",
		"form_type": "",
		"headers":   json_headers,
		"body":      "",
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

// ----- >> OS Service
func (d *TaskClient) ServiceStart(service_name string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/services/start", map[string]string{
		"service_name": service_name,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) ServiceStop(service_name string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/services/stop", map[string]string{
		"service_name": service_name,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

// ----- >> System Info
func (d *TaskClient) SystemInfo() (map[string]interface{}, error) {
	var final_result map[string]interface{}
	var final_err error

	res, err := d.exec("/system/info", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TaskClient) SystemMemory() (map[string]interface{}, error) {
	var final_result map[string]interface{}
	var final_err error

	res, err := d.exec("/system/memory", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

func (d *TaskClient) SystemCheckDomain(domain string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/system/check_domain", map[string]string{
		"domain": domain,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) SystemNetConnect(host string, arr_port []int) (map[int64]bool, error) {
	var final_result map[int64]bool
	var final_err error

	delimited_port := ""
	for _, port := range arr_port {
		if delimited_port != "" {
			delimited_port += ","
		}
		delimited_port += fmt.Sprintf("%d", port)
	}

	res, err := d.exec("/system/net_connect", map[string]string{
		"ip_address": host,
		"port":       delimited_port,
	})
	if err != nil {
		final_err = err
	} else {
		err := json.Unmarshal([]byte(res), &final_result)
		if err != nil {
			final_err = err
		}
	}

	return final_result, final_err
}

// ----- >> Git
func (d *TaskClient) GitPull(path string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/git/pull", map[string]string{
		"path": path,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) GitStatus(path string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/git/status", map[string]string{
		"path": path,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) GitBranch(path string) (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/git/branch", map[string]string{
		"path": path,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) GitVersion() (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/git/version", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

// ----- >> Go
func (d *TaskClient) GoBuild(path string, output ...string) (string, error) {
	var final_result string
	var final_err error

	var ls_output string = ""
	if output != nil {
		for i, val := range output {
			if i == 0 {
				ls_output = val
				break
			}
		}
	}

	res, err := d.exec("/go/build", map[string]string{
		"path":   path,
		"output": ls_output,
	})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func (d *TaskClient) GoVersion() (string, error) {
	var final_result string
	var final_err error

	res, err := d.exec("/go/version", map[string]string{})
	if err != nil {
		final_err = err
	} else {
		final_result = string(res)
		if len(final_result) > 1 {
			first1 := string(final_result[0:1])
			last1 := string(final_result[len(final_result)-1:])
			if first1 == "\"" && last1 == "\"" {
				final_result = final_result[1 : len(final_result)-1]
			}
		}
	}

	return final_result, final_err
}

func Var_dump(expression ...interface{}) {
	fmt.Println(fmt.Sprintf("%#v", expression))
}
