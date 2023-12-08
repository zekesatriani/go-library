package go_library

import (
	"archive/zip"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/smtp"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	cryptomd5 "crypto/md5"

	"github.com/google/uuid"
	email "github.com/jordan-wright/email"
)

func IsFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func IsInteger(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func IsStringJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

func IsLetterOrSpace(s string) bool {
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && r != ' ' {
			return false
		}
	}
	return true
}

func ToFloat64(s string, default_value float64) float64 {
	var lnValue float64 = default_value

	li_index_comma := strings.Index(s, ",")
	li_count_comma := strings.Count(s, ",")

	li_index_dot := strings.Index(s, ".")
	li_count_dot := strings.Count(s, ".")

	if li_index_comma >= 0 && li_index_dot > li_index_comma {
		// s = " 2,033,075.01 "
		s = strings.Replace(s, ",", "", -1)
	} else if li_count_comma > 1 && li_count_dot == 0 {
		// s = " 2,033,075 "
		s = strings.Replace(s, ",", "", -1)
	} else if li_index_dot >= 0 && li_index_comma > li_index_dot {
		// s = " 2.033.075,01 "
		s = strings.Replace(strings.Replace(s, ".", "", -1), ",", ".", -1)
	} else if li_count_dot > 1 && li_count_comma == 0 {
		// s = " 2.033.075 "
		s = strings.Replace(s, ".", "", -1)
	} else if li_index_comma >= 0 {
		// s = " 2033075,01 "
		s = strings.Replace(s, ",", ".", -1)
	}

	tmp, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err == nil {
		lnValue = tmp
	}
	return lnValue
}

func ToInt64(s string, default_value int64) int64 {
	var liValue int64 = default_value
	tmp, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		liValue = tmp
	}
	return liValue
}

func ToDate(s string) time.Time {
	var ldtValue time.Time

	re_dmy_slash := regexp.MustCompile("(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\d\\d)")
	re_dmy_min := regexp.MustCompile("(0?[1-9]|[12][0-9]|3[01])-(0?[1-9]|1[012])-((19|20)\\d\\d)")
	re_ymd := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	if re_dmy_slash.MatchString(s) {
		layout := "02/01/2006"
		str := s
		tmp, err := time.Parse(layout, str)
		if err == nil {
			ldtValue = tmp
		}
	} else if re_dmy_min.MatchString(s) {
		layout := "02-01-2006"
		str := s
		tmp, err := time.Parse(layout, str)
		if err == nil {
			ldtValue = tmp
		}
	} else if re_ymd.MatchString(s) {
		layout := "2006-01-02"
		str := s
		tmp, err := time.Parse(layout, str)
		if err == nil {
			ldtValue = tmp
		}
	}

	return ldtValue
}

func MD5(s string) string {
	data := []byte(s)
	return fmt.Sprintf("%x", cryptomd5.Sum(data))
}

func GetFieldString(e interface{}, field string) string {
	r := reflect.ValueOf(e)
	f := reflect.Indirect(r).FieldByName(field)
	return f.String()
}

func GetFieldInteger(e interface{}, field string) int {
	r := reflect.ValueOf(e)
	f := reflect.Indirect(r).FieldByName(field)
	return int(f.Int())
}

func InStringSlice(val string, slice []string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func InInt64Slice(val int64, slice []int64) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func GenUUID() string {
	return uuid.New().String()
}

func GetIpFromDomain(domain string) string {
	var ls_ip_address string = ""
	fmt.Println("-----------------")
	fmt.Println(domain)
	ips, _ := net.LookupIP(domain)
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			if ls_ip_address == "" {
				ls_ip_address = ipv4.String()
			}
			fmt.Println("IPv4: ", ipv4)
		}
	}
	return ls_ip_address
}

func GetIpOutbound() (ip string, err error) {
	var clientIP string = ""
	var final_error error

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		final_error = err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	clientIP = fmt.Sprintf("%v", localAddr.IP)

	return clientIP, final_error
}

func CheckNetConnect(host string, ports []string) map[int64]bool {
	result := map[int64]bool{}

	for _, port := range ports {
		var li_port = ToInt64(strings.TrimSpace(port), 0)
		if li_port > 0 && li_port <= 65353 {
			timeout := time.Second
			conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
			if err != nil {
				result[li_port] = false
			}
			if conn != nil {
				result[li_port] = true
				conn.Close()
			}
		}
	}

	return result
}

func SendMail(param map[string]interface{}) error {
	var result_error error

	sender_host, _ := param["sender_host"]
	sender_port, _ := param["sender_port"]
	sender_name, _ := param["sender_name"]
	sender_email, _ := param["sender_email"]
	sender_password, _ := param["sender_password"]
	sender_tls, _ := param["sender_tls"]
	sender_insecure_skip_verify, _ := param["sender_insecure_skip_verify"]

	arr_to, _ := param["to"]
	arr_cc, _ := param["cc"]
	arr_bcc, _ := param["bcc"]
	arr_attachment, _ := param["attachment"]

	ls_subject, _ := param["subject"]
	ls_message_text, _ := param["message"]
	ls_message_html, _ := param["message_html"]

	e := email.NewEmail()

	var ls_sender_host string = ""
	var ls_sender_port string = ""
	var ls_sender_email string = ""
	var ls_sender_password string = ""
	var ls_sender_name string = ""
	var ls_sender_tls string = ""
	var ls_sender_insecure_skip_verify bool = true

	if sender_host != nil && strings.TrimSpace(sender_host.(string)) != "" {
		ls_sender_host = sender_host.(string)
	}

	if sender_port != nil && strings.TrimSpace(sender_port.(string)) != "" {
		ls_sender_port = sender_port.(string)
	}

	if sender_email != nil && strings.TrimSpace(sender_email.(string)) != "" {
		ls_sender_email = sender_email.(string)
	}

	if sender_name != nil && strings.TrimSpace(sender_name.(string)) != "" {
		ls_sender_name = sender_name.(string)
	}

	if sender_password != nil && strings.TrimSpace(sender_password.(string)) != "" {
		ls_sender_password = sender_password.(string)
	} else {
		ls_sender_password = "U2FsdGVkX19yCR2w7jntHnP9fHrCC+WDcwHDgyq/Njg="
	}

	if sender_tls != nil && strings.TrimSpace(sender_tls.(string)) != "" {
		ls_sender_tls = sender_tls.(string)
	} else {
		ls_sender_tls = "tls"
	}

	if sender_insecure_skip_verify != nil && strings.TrimSpace(sender_insecure_skip_verify.(string)) != "" && strings.TrimSpace(sender_insecure_skip_verify.(string)) != "true" {
		ls_sender_insecure_skip_verify = false
	} else {
		ls_sender_insecure_skip_verify = true
	}

	e.From = (ls_sender_name + " <" + ls_sender_email + ">")

	if arr_to != nil && len(arr_to.([]string)) > 0 {
		var arr_tmp []string = []string{}
		for _, val := range arr_to.([]string) {
			if strings.TrimSpace(val) != "" {
				arr_tmp = append(arr_tmp, val)
			}
		}
		e.To = arr_tmp
	}

	if arr_cc != nil && len(arr_cc.([]string)) > 0 {
		var arr_tmp []string = []string{}
		for _, val := range arr_cc.([]string) {
			if strings.TrimSpace(val) != "" {
				arr_tmp = append(arr_tmp, val)
			}
		}
		e.Cc = arr_tmp
	}

	if arr_bcc != nil && len(arr_bcc.([]string)) > 0 {
		var arr_tmp []string = []string{}
		for _, val := range arr_bcc.([]string) {
			if strings.TrimSpace(val) != "" {
				arr_tmp = append(arr_tmp, val)
			}
		}
		e.Bcc = arr_tmp
	}

	if arr_attachment != nil && len(arr_attachment.([]string)) > 0 {
		for _, val := range arr_attachment.([]string) {
			if strings.TrimSpace(val) != "" {
				e.AttachFile(val)
			}
		}

	}

	e.Subject = ls_subject.(string)

	if ls_message_text != nil && strings.TrimSpace(ls_message_text.(string)) != "" {
		e.Text = []byte(ls_message_text.(string))
	}

	if ls_message_html != nil && strings.TrimSpace(ls_message_html.(string)) != "" {
		e.HTML = []byte(ls_message_html.(string))
	}

	/// TLS config
	tlsconfig := &tls.Config{
		InsecureSkipVerify: ls_sender_insecure_skip_verify,
		ServerName:         ls_sender_host,
	}
	_ = tlsconfig

	if ls_sender_tls == "tls" {
		result_error = e.SendWithTLS(ls_sender_host+":"+ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host), tlsconfig)
	} else if ls_sender_tls == "starttls" {
		result_error = e.SendWithStartTLS(ls_sender_host+":"+ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host), tlsconfig)
	} else {
		result_error = e.Send(ls_sender_host+":"+ls_sender_port, smtp.PlainAuth("", ls_sender_email, Decrypt(ls_sender_password), ls_sender_host))
	}
	// fmt.Println("result_error", result_error)

	return result_error

}

func Unzip(src, dest string) error {
	archive, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer archive.Close()

	for _, f := range archive.File {
		filePath := filepath.Join(dest, f.Name)
		// fmt.Println("unzipping file ", filePath)

		if !strings.HasPrefix(filePath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return errors.New("invalid file path")

		}
		if f.FileInfo().IsDir() {
			// fmt.Println("creating directory...")
			os.MkdirAll(filePath, os.ModePerm)
			continue
		}

		if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
			return err
		}

		dstFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		fileInArchive, err := f.Open()
		if err != nil {
			return err
		}

		if _, err := io.Copy(dstFile, fileInArchive); err != nil {
			return err
		}

		dstFile.Close()
		fileInArchive.Close()
	}

	return nil
}

func Unrar(src, dest string) error {
	if _, err := os.Stat(src); errors.Is(err, os.ErrNotExist) {
		return err
	} else if _, err := os.Stat(dest); errors.Is(err, os.ErrNotExist) {
		return err
	} else if runtime.GOOS == "windows" {
		src_dir := filepath.Dir(src)
		src_file := filepath.Base(src)

		dest_dir := dest
		// fmt.Println("src_dir: ", src_dir)
		// fmt.Println("src_file: ", src_file)
		// fmt.Println("dest_dir: ", dest_dir)
		if src_dir != dest_dir {
			// fmt.Println("copy: ", src, " : ", dest+"/"+src_file)
			CopyFile(src, dest+"/"+src_file)
		}

		cmd := exec.Command("rar", "e", src_file)
		cmd.Dir = dest_dir

		if output, err := cmd.CombinedOutput(); err != nil {
			os.Remove(dest + "/" + src_file)
			return err
		} else {
			os.Remove(dest + "/" + src_file)
			if output != nil {
				fmt.Println(string(output))
			}
			return nil
		}
	}
	return errors.New("Unrar Fail")
}

func RemoveDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	err = os.Remove(dir)
	if err != nil {
		return err
	}
	return nil
}

func CopyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func SliceExists(slice interface{}, item interface{}) bool {
	s := reflect.ValueOf(slice)

	if s.Kind() != reflect.Slice {
		return false
		// panic("SliceExists() given a non-slice type")
	}

	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == item {
			return true
		}
	}

	return false
}

func exportReflectValue(
	field_value reflect.Value, indent string,
	loop_detector map[uintptr]bool) string {
	defer func() { recover() }()
	var_type := field_value.Kind().String()
	inside_indent := indent + "  "
	switch field_value.Kind() {
	case reflect.Bool:
		return fmt.Sprintf("%s(%t)", var_type, field_value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%s(%d)", var_type, field_value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Uintptr:
		return fmt.Sprintf("%s(%d)", var_type, field_value.Uint())
	case reflect.Float32:
		return fmt.Sprintf("%s(%g)", var_type, float32(field_value.Float()))
	case reflect.Float64:
		return fmt.Sprintf("%s(%g)", var_type, field_value.Float())
	case reflect.Complex64:
		return fmt.Sprintf("%s%g", var_type, complex64(field_value.Complex()))
	case reflect.Complex128:
		return fmt.Sprintf("%s%g", var_type, field_value.Complex())
	case reflect.Ptr:
		if field_value.IsNil() {
			return fmt.Sprintf("(%s)nil", field_value.Type().String())
		}
		pointer := field_value.Pointer()
		if _, present := loop_detector[pointer]; present {
			return "<infinite loop is detected>"
		}
		loop_detector[pointer] = true
		defer delete(loop_detector, field_value.Pointer())
		return "&" + exportReflectValue(field_value.Elem(), indent, loop_detector)
	case reflect.Array, reflect.Slice:
		output := fmt.Sprintf("%s{", field_value.Type())
		if field_value.Len() > 0 {
			output += "\n"
			for i := 0; i < field_value.Len(); i++ {
				output += inside_indent
				// output += exportReflectValue(field_value.Index(i), inside_indent)
				output += exportReflectValue(
					field_value.Index(i), inside_indent, loop_detector)
				output += ",\n"
			}
			output += indent
		}
		output += "}"
		return output
	case reflect.Map:
		output := fmt.Sprintf("%s{", field_value.Type())
		keys := field_value.MapKeys()
		if len(keys) > 0 {
			output += "\n"
			for _, key := range keys {
				output += inside_indent
				output += exportReflectValue(key, inside_indent, loop_detector)
				output += ": "
				output += exportReflectValue(
					field_value.MapIndex(key), inside_indent, loop_detector)
				output += ",\n"
			}
			output += indent
		}
		output += "}"
		return output
	case reflect.String:
		return fmt.Sprintf("%s(%#v)", var_type, field_value.String())
	case reflect.UnsafePointer:
		return fmt.Sprintf("unsafe.Pointer(%#v)", field_value.Pointer())
	case reflect.Struct:
		output := fmt.Sprintf("%s{\n", field_value.Type())
		for i := 0; i < field_value.NumField(); i++ {
			output += inside_indent + field_value.Type().Field(i).Name + ": "
			output += exportReflectValue(
				field_value.Field(i), inside_indent, loop_detector)
			output += ","
			if field_value.Type().Field(i).Tag != "" {
				output += fmt.Sprintf("  // Tag: %#v", field_value.Type().Field(i).Tag)
			}
			output += "\n"
		}
		output += indent + "}"
		return output
	case reflect.Interface:
		return exportReflectValue(
			reflect.ValueOf(field_value.Interface()), indent, loop_detector)
	case reflect.Chan:
		return fmt.Sprintf("(%s)%#v", field_value.Type(), field_value.Pointer())
	case reflect.Invalid:
		return "<invalid>"
	default:
		return "<" + var_type + " is not supported>"
	}
}

func VarDumpToString(data interface{}) string {
	return exportReflectValue(reflect.ValueOf(data), "", map[uintptr]bool{})
}

func VarDump(data interface{}) {
	fmt.Println(VarDumpToString(data))
}
