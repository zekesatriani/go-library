package go_library

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"reflect"

	// "fmt"
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ----- >> Base Rest Client
type Rest struct {
	maxIdleConns        int
	maxIdleConnsPerHost int
	idleConnTimeout     int64
	timeout             int64
	header              map[string]string
	insecureSkipVerify  bool
}

// ----- >> Init Rest
func NewRestClient() Rest {
	restClient := Rest{}

	restClient.maxIdleConns = 20
	restClient.maxIdleConnsPerHost = 20
	restClient.maxIdleConns = 30
	restClient.timeout = 30
	restClient.header = make(map[string]string)
	restClient.insecureSkipVerify = true

	return restClient
}

// ----- >> Header Request Modification
func (this *Rest) SetHeader(key string, value string) {
	this.header[key] = value
}

func (this *Rest) SetHeaders(header map[string]string) {
	this.header = header
}

func (this *Rest) RemoveHeader(key string) {
	var _, isExist = this.header[key]
	if isExist {
		delete(this.header, key)
	}
}

func (this *Rest) ClearHeaders() {
	this.header = make(map[string]string)
}

func (this *Rest) GetHeader(key string) (string, bool) {
	var result, isExist = this.header[key]
	return result, isExist
}

func (this *Rest) GetHeaders() map[string]string {
	return this.header
}

// ----- >> Timeout Configuration
func (this *Rest) SetMaxIdleConns(maxIdleConns int) {
	this.maxIdleConns = maxIdleConns
}

func (this *Rest) SetMaxIdleConnsPerHost(maxIdleConnsPerHost int) {
	this.maxIdleConnsPerHost = maxIdleConnsPerHost
}

func (this *Rest) SetIdleConnTimeout(idleConnTimeout int64) {
	this.idleConnTimeout = idleConnTimeout
}

func (this *Rest) SetTimeout(timeout int64) {
	this.timeout = timeout
}

func (this *Rest) SetSSLInsecureSkipVerify(value bool) {
	this.insecureSkipVerify = value
}

// ----- >> Execute Rest Client
func (this *Rest) Get(rest_url string, data map[string]string) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("GET", rest_url, data, "", nil, "")

	return final_result, final_err
}

func (this *Rest) GetDownload(rest_url string, form_type string, data map[string]string, output_path string) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("GET", rest_url, data, "", nil, output_path)

	return final_result, final_err
}

func (this *Rest) Post(rest_url string, form_type string, data map[string]interface{}) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("POST", rest_url, nil, form_type, data, "")

	return final_result, final_err
}

func (this *Rest) PostDownload(rest_url string, form_type string, data map[string]interface{}, output_path string) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("POST", rest_url, nil, form_type, data, output_path)

	return final_result, final_err
}

func (this *Rest) Put(rest_url string, form_type string, data map[string]interface{}) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("PUT", rest_url, nil, form_type, data, "")

	return final_result, final_err
}

func (this *Rest) Delete(rest_url string, data map[string]string) (string, error) {
	var final_result string
	var final_err error

	final_result, final_err = this.Curl("DELETE", rest_url, data, "", nil, "")

	return final_result, final_err
}

func (this *Rest) Curl(rest_method string, rest_url string, data_url map[string]string, form_type string, data_body map[string]interface{}, output_path string) (string, error) {
	var final_result string
	var final_err error

	var li_timeout int64 = this.timeout
	if li_timeout < 120 && output_path != "" {
		li_timeout = 7200
	}
	res, err := this.CurlBinary(rest_method, rest_url, data_url, form_type, data_body, li_timeout)
	if err != nil {
		final_err = err
	} else {
		defer res.Body.Close()
		if output_path != "" {

			// Create the file
			out, err := os.Create(output_path)
			if err != nil {
				final_err = err
			}
			defer out.Close()

			// Check server response
			if res.StatusCode != http.StatusOK {
				final_err = errors.New(fmt.Sprintf("bad status: %s", res.Status))
			}

			_, err = io.Copy(out, res.Body)
			if err != nil {
				final_err = err
			} else {
				final_result = "DOWNLOAD-OK"

				// Open File
				f, err := os.Open(output_path)
				if err != nil {
					final_err = err
				} else {
					reader := bufio.NewReader(f)
					line, _, err := reader.ReadLine()
					if err == nil && IsStringJSON(string(line)) {
						// fmt.Println("line:", string(line))
						final_result = string(line)
					}
				}
				defer f.Close()

			}

		} else {

			body, err := io.ReadAll(res.Body)
			if err != nil {
				final_err = err
			} else {
				final_result = string(body)
			}

		}
	}

	return final_result, final_err
}

func (this *Rest) CurlBinary(rest_method string, rest_url string, data_url map[string]string, form_type string, data_body map[string]interface{}, timeout int64) (*http.Response, error) {
	var final_result *http.Response
	var final_err error

	// Init URL Param
	if data_url != nil {
		values := url.Values{}
		for key, val := range data_url {
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

	// Init HttpClient
	tr := &http.Transport{
		MaxIdleConns:        this.maxIdleConns,
		MaxIdleConnsPerHost: this.maxIdleConnsPerHost,
		IdleConnTimeout:     time.Duration(this.idleConnTimeout) * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: this.insecureSkipVerify},
	}

	var li_timeout int64 = this.timeout
	if timeout > li_timeout {
		li_timeout = timeout
	}
	client := &http.Client{Timeout: time.Second * time.Duration(li_timeout), Transport: tr}

	// Define Content-Type
	this.RemoveHeader("Content-Type")
	if strings.ToUpper(rest_method) == "POST" || strings.ToUpper(rest_method) == "PUT" {

		if form_type == "x-www-form-urlencoded" {
			form_type = "application/x-www-form-urlencoded"
		} else if form_type == "form-data" {
			form_type = "multipart/form-data"
		} else if form_type == "json" {
			form_type = "application/json"
		}

		if form_type != "application/x-www-form-urlencoded" && form_type != "multipart/form-data" && form_type != "application/json" {
			form_type = "application/json"
		}

		this.SetHeader("Content-Type", form_type)
	}

	// Post Body Data
	var payload io.Reader
	if data_body != nil {

		if form_type == "application/x-www-form-urlencoded" {

			values := url.Values{}
			for key, value := range data_body {
				val := fmt.Sprintf("%v", value)
				val_type := fmt.Sprintf("%v", reflect.TypeOf(value))

				if val_type == "time.Time" {
					val = value.(time.Time).Format("2006-01-02 15:04:05")
				}

				if strings.TrimSpace(key) != "" && strings.TrimSpace(val) != "" {
					values.Add(key, val)
				}
			}
			query := values.Encode()
			payload = strings.NewReader(query)

		} else if form_type == "multipart/form-data" {

			payload_buff := &bytes.Buffer{}
			writer := multipart.NewWriter(payload_buff)
			for key, value := range data_body {
				val := fmt.Sprintf("%v", value)
				val_type := fmt.Sprintf("%v", reflect.TypeOf(value))

				if val_type == "time.Time" {
					val = value.(time.Time).Format("2006-01-02 15:04:05")
				}

				if strings.TrimSpace(key) != "" && strings.TrimSpace(val) != "" {
					_ = writer.WriteField(key, val)
				}
			}
			err := writer.Close()
			if err != nil {
				final_err = err
			} else {
				payload = payload_buff
			}

		} else {

			jsonString, err := json.Marshal(data_body)
			if err != nil {
				final_err = err
			} else {

				var jsonStr = []byte(jsonString)
				payload = bytes.NewBuffer(jsonStr)
			}

		}
	}

	if final_err == nil {
		req, err := http.NewRequest(rest_method, rest_url, payload)

		if err != nil {
			final_err = err
		} else {

			// Http Request Header
			for key, val := range this.header {
				if strings.TrimSpace(val) != "" {
					req.Header.Add(key, val)
				}
			}

			res, err := client.Do(req)
			if err != nil {
				final_err = err
			} else {
				final_result = res
			}
		}
	}

	return final_result, final_err
}

func (this *Rest) GetFileContentType(out *os.File) (string, error) {

	// Only the first 512 bytes are used to sniff the content type.
	buffer := make([]byte, 512)

	_, err := out.Read(buffer)
	if err != nil {
		return "", err
	}

	// Use the net/http package's handy DectectContentType function. Always returns a valid
	// content-type by returning "application/octet-stream" if no others seemed to match.
	contentType := http.DetectContentType(buffer)

	return contentType, nil
}
