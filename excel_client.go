package go_library

import (
	"archive/zip"
	"bufio"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// ----- >> Base Excel Client
type ExcelClient struct {
	file_name  string
	file_path  string
	sheet_name string

	boldStyle int

	curr_file          *excelize.File
	curr_writer        *excelize.StreamWriter
	curr_reader        *excelize.Rows
	curr_reader_option ExcelClientReaderOption
	curr_writer_option ExcelClientWriterOption

	write_is_ready        bool
	write_temp_folder     string
	write_file_name       []string
	write_headers         [][]interface{}
	write_itter_row       int64
	write_max_row_perfile int64
	write_curr_file       int64
	write_curr_row        int64

	read_is_ready        bool
	read_file_path       string
	read_sheets          []string
	read_curr_sheet      int64
	read_curr_row        int64
	read_count_empty_row int64

	base_path string
}

type ExcelClientReaderOption struct {
	Password    string
	RowHeaderAt int64
	RowDataAt   int64
}

type ExcelClientWriterOption struct {
	FileName              string
	FilePath              string
	FileNamePattern       string
	SheetName             string
	MaxRowPerFile         int64
	ColWidth              []float64
	NumberTypeColumn      []string
	UseRowNo              bool
	FirstRowHeader        []interface{}
	FirstRowHeaderMapping map[string]string

	OutputFileSkipCompression bool
}

var arr_alphabets []string = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ", "BA", "BB", "BC", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BK", "BL", "BM", "BN", "BO", "BP", "BQ", "BR", "BS", "BT", "BU", "BV", "BW", "BX", "BY", "BZ", "CA", "CB", "CC", "CD", "CE", "CF", "CG", "CH", "CI", "CJ", "CK", "CL", "CM", "CN", "CO", "CP", "CQ", "CR", "CS", "CT", "CU", "CV", "CW", "CX", "CY", "CZ", "DA", "DB", "DC", "DD", "DE", "DF", "DG", "DH", "DI", "DJ", "DK", "DL", "DM", "DN", "DO", "DP", "DQ", "DR", "DS", "DT", "DU", "DV", "DW", "DX", "DY", "DZ", "EA", "EB", "EC", "ED", "EE", "EF", "EG", "EH", "EI", "EJ", "EK", "EL", "EM", "EN", "EO", "EP", "EQ", "ER", "ES", "ET", "EU", "EV", "EW", "EX", "EY", "EZ", "FA", "FB", "FC", "FD", "FE", "FF", "FG", "FH", "FI", "FJ", "FK", "FL", "FM", "FN", "FO", "FP", "FQ", "FR", "FS", "FT", "FU", "FV", "FW", "FX", "FY", "FZ", "GA", "GB", "GC", "GD", "GE", "GF", "GG", "GH", "GI", "GJ", "GK", "GL", "GM", "GN", "GO", "GP", "GQ", "GR", "GS", "GT", "GU", "GV", "GW", "GX", "GY", "GZ", "HA", "HB", "HC", "HD", "HE", "HF", "HG", "HH", "HI", "HJ", "HK", "HL", "HM", "HN", "HO", "HP", "HQ", "HR", "HS", "HT", "HU", "HV", "HW", "HX", "HY", "HZ", "IA", "IB", "IC", "ID", "IE", "IF", "IG", "IH", "II", "IJ", "IK", "IL", "IM", "IN", "IO", "IP", "IQ", "IR", "IS", "IT", "IU", "IV", "IW", "IX", "IY", "IZ", "JA", "JB", "JC", "JD", "JE", "JF", "JG", "JH", "JI", "JJ", "JK", "JL", "JM", "JN", "JO", "JP", "JQ", "JR", "JS", "JT", "JU", "JV", "JW", "JX", "JY", "JZ", "KA", "KB", "KC", "KD", "KE", "KF", "KG", "KH", "KI", "KJ", "KK", "KL", "KM", "KN", "KO", "KP", "KQ", "KR", "KS", "KT", "KU", "KV", "KW", "KX", "KY", "KZ", "LA", "LB", "LC", "LD", "LE", "LF", "LG", "LH", "LI", "LJ", "LK", "LL", "LM", "LN", "LO", "LP", "LQ", "LR", "LS", "LT", "LU", "LV", "LW", "LX", "LY", "LZ", "MA", "MB", "MC", "MD", "ME", "MF", "MG", "MH", "MI", "MJ", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NB", "NC", "ND", "NE", "NF", "NG", "NH", "NI", "NJ", "NK", "NL", "NM", "NN", "NO", "NP", "NQ", "NR", "NS", "NT", "NU", "NV", "NW", "NX", "NY", "NZ", "OA", "OB", "OC", "OD", "OE", "OF", "OG", "OH", "OI", "OJ", "OK", "OL", "OM", "ON", "OO", "OP", "OQ", "OR", "OS", "OT", "OU", "OV", "OW", "OX", "OY", "OZ", "PA", "PB", "PC", "PD", "PE", "PF", "PG", "PH", "PI", "PJ", "PK", "PL", "PM", "PN", "PO", "PP", "PQ", "PR", "PS", "PT", "PU", "PV", "PW", "PX", "PY", "PZ", "QA", "QB", "QC", "QD", "QE", "QF", "QG", "QH", "QI", "QJ", "QK", "QL", "QM", "QN", "QO", "QP", "QQ", "QR", "QS", "QT", "QU", "QV", "QW", "QX", "QY", "QZ", "RA", "RB", "RC", "RD", "RE", "RF", "RG", "RH", "RI", "RJ", "RK", "RL", "RM", "RN", "RO", "RP", "RQ", "RR", "RS", "RT", "RU", "RV", "RW", "RX", "RY", "RZ", "SA", "SB", "SC", "SD", "SE", "SF", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SP", "SQ", "SR", "SS", "ST", "SU", "SV", "SW", "SX", "SY", "SZ", "TA", "TB", "TC", "TD", "TE", "TF", "TG", "TH", "TI", "TJ", "TK", "TL", "TM", "TN", "TO", "TP", "TQ", "TR", "TS", "TT", "TU", "TV", "TW", "TX", "TY", "TZ", "UA", "UB", "UC", "UD", "UE", "UF", "UG", "UH", "UI", "UJ", "UK", "UL", "UM", "UN", "UO", "UP", "UQ", "UR", "US", "UT", "UU", "UV", "UW", "UX", "UY", "UZ", "VA", "VB", "VC", "VD", "VE", "VF", "VG", "VH", "VI", "VJ", "VK", "VL", "VM", "VN", "VO", "VP", "VQ", "VR", "VS", "VT", "VU", "VV", "VW", "VX", "VY", "VZ", "WA", "WB", "WC", "WD", "WE", "WF", "WG", "WH", "WI", "WJ", "WK", "WL", "WM", "WN", "WO", "WP", "WQ", "WR", "WS", "WT", "WU", "WV", "WW", "WX", "WY", "WZ", "XA", "XB", "XC", "XD", "XE", "XF", "XG", "XH", "XI", "XJ", "XK", "XL", "XM", "XN", "XO", "XP", "XQ", "XR", "XS", "XT", "XU", "XV", "XW", "XX", "XY", "XZ", "YA", "YB", "YC", "YD", "YE", "YF", "YG", "YH", "YI", "YJ", "YK", "YL", "YM", "YN", "YO", "YP", "YQ", "YR", "YS", "YT", "YU", "YV", "YW", "YX", "YY", "YZ", "ZA", "ZB", "ZC", "ZD", "ZE", "ZF", "ZG", "ZH", "ZI", "ZJ", "ZK", "ZL", "ZM", "ZN", "ZO", "ZP", "ZQ", "ZR", "ZS", "ZT", "ZU", "ZV", "ZW", "ZX", "ZY", "ZZ"}

// ----- >> Init Excel Client
func NewExcelClient() ExcelClient {
	excelClient := ExcelClient{}

	excelClient.curr_reader_option = ExcelClientReaderOption{}
	excelClient.curr_writer_option = ExcelClientWriterOption{SheetName: "Sheet1", MaxRowPerFile: 1000000}
	excelClient.write_is_ready = false
	excelClient.read_is_ready = false

	return excelClient
}

// ----- >> Create Excel
func (d *ExcelClient) Create(file_name string, file_path string, opt ...ExcelClientWriterOption) error {
	var final_err error

	d.curr_writer_option = ExcelClientWriterOption{SheetName: "Sheet1", MaxRowPerFile: 1000000}
	for _, item := range opt {
		d.curr_writer_option = item
		break
	}

	// Create(file_name string, file_path string, sheet_name string, max_row_perfile int64) error {
	// var final_err error

	d.write_is_ready = false

	d.write_temp_folder = GenUUID()
	RemoveDirectory(file_path + d.write_temp_folder)

	err := os.Mkdir(file_path+d.write_temp_folder, 0755)
	if err != nil {
		final_err = err
	} else {
		d.base_path = file_path + d.write_temp_folder

		d.file_name = file_name
		d.file_path = file_path
		if d.curr_writer_option.SheetName == "" {
			d.sheet_name = "Sheet1"
		} else {
			d.sheet_name = d.curr_writer_option.SheetName
		}
		if d.curr_writer_option.MaxRowPerFile < 1 {
			d.curr_writer_option.MaxRowPerFile = 1000000
		}
		d.write_max_row_perfile = d.curr_writer_option.MaxRowPerFile
		d.write_headers = [][]interface{}{}

		final_err = d.create_file()

		d.write_itter_row = 1
		d.write_curr_file = 1
	}

	return final_err
}

func (d *ExcelClient) create_file() error {
	var final_err error

	d.curr_file = excelize.NewFile()
	streamWriter, err := d.curr_file.NewStreamWriter(d.sheet_name)
	if err != nil {
		final_err = err
	} else {
		d.curr_writer = streamWriter
		d.write_is_ready = true
		d.write_curr_row = 1

		if d.sheet_name != "Sheet1" {
			d.curr_file.SetSheetName("Sheet1", d.sheet_name)
		}

		d.boldStyle, _ = d.curr_file.NewStyle(&excelize.Style{
			Font: &excelize.Font{Bold: true},
			Border: []excelize.Border{
				{Type: "left", Color: "000000", Style: 1},
				{Type: "right", Color: "000000", Style: 1},
				{Type: "top", Color: "000000", Style: 1},
				{Type: "bottom", Color: "000000", Style: 1},
			},
		})

		if len(d.curr_writer_option.ColWidth) > 0 {
			for i, col_width := range d.curr_writer_option.ColWidth {
				// fmt.Println("Set col_width : ", d.sheet_name, " : ", i, " : ", arr_alphabets[i], " : ", col_width)
				if col_width > 5 {
					err := d.curr_file.SetColWidth(d.sheet_name, arr_alphabets[i], arr_alphabets[i], col_width)
					if err != nil {
						final_err = err
						break
					}
				} else {
					err := d.curr_file.SetColWidth(d.sheet_name, arr_alphabets[i], arr_alphabets[i], 15)
					if err != nil {
						final_err = err
						break
					}
				}
			}
		}
	}

	return final_err
}

// ----- >> Add Excel Row Header
func (d *ExcelClient) AddRowHeader(row []interface{}) error {
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("File is not ready to write")
	} else {
		d.write_headers = append(d.write_headers, row)

		// row_header := make([]interface{}, len(row))
		// for j, title := range row {
		// 	row_header[j] = excelize.Cell{StyleID: d.boldStyle, Value: title}
		// }

		// var ls_row string = fmt.Sprintf("%v", (d.write_curr_row))
		// err := d.curr_writer.SetRow("A"+ls_row, row_header)
		// if err != nil {
		// 	final_err = err
		// }

		// d.write_curr_row++
		final_err = d.write_row(row, true)
	}

	return final_err
}

// ----- >> Add Excel Row Data
func (d *ExcelClient) AddRowData(row []interface{}) error {
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("File is not ready to write")
	} else {

		// Create File when more than max row per-file
		if d.write_itter_row > 1 && ((d.write_itter_row-1)%d.write_max_row_perfile == 0) {
			var ls_file_name_part string = d.file_name

			if d.curr_writer_option.FileNamePattern != "" {
				ls_file_name_part = strings.Replace(d.curr_writer_option.FileNamePattern, "{i}", fmt.Sprintf("%d", d.write_curr_file), -1) + ".xlsx"
				ls_file_name_part = strings.Replace(ls_file_name_part, "{0i}", fmt.Sprintf("%02d", d.write_curr_file), -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{00i}", fmt.Sprintf("%03d", d.write_curr_file), -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{000i}", fmt.Sprintf("%04d", d.write_curr_file), -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{0000i}", fmt.Sprintf("%05d", d.write_curr_file), -1)
			} else {
				ls_file_name_part = ls_file_name_part + "-Part" + fmt.Sprintf("%03d", d.write_curr_file) + ".xlsx"
			}
			err := d.save_file(ls_file_name_part)
			if err != nil {
				final_err = err
				d.write_is_ready = false
			} else {
				d.write_file_name = append(d.write_file_name, ls_file_name_part)

				d.write_curr_row = 1
				d.write_curr_file++

				d.create_file()
			}
		}

		// Write Header
		if final_err == nil && d.write_curr_row == 1 && len(d.write_headers) > 0 {
			for _, row := range d.write_headers {
				final_err = d.write_row(row, true)
				if final_err != nil {
					d.write_is_ready = false
					break
				}
			}

		}

		final_err = d.write_row(row, false)
		if final_err != nil {
			d.write_is_ready = false
		}

		d.write_itter_row++
	}

	return final_err
}

// ----- >> Import From CSV to Excel
type CSVClientReaderOption struct {
	RowHeaderAt            int64
	RowDataAt              int64
	Delimiter              rune
	NumberColumName        []string
	NumberColumIndex       []int64
	OnImportProcessEachRow func(index_no int64, data []string, err error) (bool, []string)
}

func (d *ExcelClient) ImportFromCSV(file_path string, opt ...CSVClientReaderOption) (int64, error) {
	var total_row int64 = 0
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("File is not ready to write")
	} else {

		csv_reader_option := CSVClientReaderOption{}
		for _, item := range opt {
			csv_reader_option = item
			break
		}

		// Define Column Name with Number type
		var arr_NumberColumnName []string = []string{}
		if csv_reader_option.NumberColumName != nil {
			for _, val := range csv_reader_option.NumberColumName {
				_, lb_found := InStringSlice(val, arr_NumberColumnName)
				if !lb_found {
					arr_NumberColumnName = append(arr_NumberColumnName, strings.TrimSpace(strings.ToLower(val)))
				}
			}
		}

		// Define Column Index with Number Type
		var arr_NumberColumnIndex []int64 = []int64{}
		if csv_reader_option.NumberColumIndex != nil {
			for _, val := range csv_reader_option.NumberColumIndex {
				_, lb_found := InInt64Slice(val, arr_NumberColumnIndex)
				if !lb_found {
					arr_NumberColumnIndex = append(arr_NumberColumnIndex, val)
				}
			}
		}

		if _, err := os.Stat(file_path); errors.Is(err, os.ErrNotExist) {
			final_err = errors.New("File [" + file_path + "] Not Found")
		} else {
			// open file
			f, err := os.Open(file_path)
			if err != nil {
				final_err = err
			} else {
				// Set Default
				if csv_reader_option.Delimiter == 0 {
					reader := bufio.NewReader(f)
					line, _, err := reader.ReadLine()
					if err != nil {
						final_err = err
					} else {
						if strings.Contains(string(line), "|") {
							csv_reader_option.Delimiter = '|'
						} else if strings.Contains(string(line), ";") {
							csv_reader_option.Delimiter = ';'
						} else if strings.Contains(string(line), "\t") {
							csv_reader_option.Delimiter = '\t'
						} else if strings.Contains(string(line), ",") {
							csv_reader_option.Delimiter = ','
						}
					}
					f.Close()

					f, err = os.Open(file_path)
					if err != nil {
						final_err = err
					}

				}
				// if csv_reader_option.RowHeaderAt == 0 {
				// 	csv_reader_option.RowHeaderAt = 1
				// }
				if csv_reader_option.RowDataAt == 0 || csv_reader_option.RowDataAt < csv_reader_option.RowHeaderAt {
					csv_reader_option.RowDataAt = csv_reader_option.RowHeaderAt + 1
				}
			}

			// close the file at the end of the program
			defer f.Close()

			if final_err == nil {
				// read csv values using csv.Reader
				csvReader := csv.NewReader(f)
				csvReader.Comma = csv_reader_option.Delimiter

				if csv_reader_option.RowHeaderAt == 0 && len(d.write_headers) > 0 && len(d.write_headers[0]) > 0 {
					for itter_col, val := range d.write_headers[0] {
						_, lb_found := InStringSlice(strings.TrimSpace(strings.ToLower(fmt.Sprintf("%v", val))), arr_NumberColumnName)
						if lb_found {

							_, lb_found = InInt64Slice(int64(itter_col), arr_NumberColumnIndex)
							if !lb_found {
								arr_NumberColumnIndex = append(arr_NumberColumnIndex, int64(itter_col))
							}

						}
					}
				}

				var itter_row int64 = 0
				var itter_no int64 = 0
				for {
					rec, err := csvReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						final_err = err
						break
					}

					// Row Header : Loop Column
					if (itter_row+1) == csv_reader_option.RowHeaderAt && len(rec) > 0 && len(arr_NumberColumnName) > 0 {

						var new_row []interface{} = []interface{}{}
						for itter_col, val := range rec {
							_, lb_found := InStringSlice(strings.TrimSpace(strings.ToLower(val)), arr_NumberColumnName)
							if lb_found {

								_, lb_found = InInt64Slice(int64(itter_col), arr_NumberColumnIndex)
								if !lb_found {
									arr_NumberColumnIndex = append(arr_NumberColumnIndex, int64(itter_col))
								}

							}
							new_row = append(new_row, val)
						}

						err = d.AddRowHeader(new_row)
						if err != nil {
							final_err = err
							break
						}
					}

					// Row Data : Loop Column
					// fmt.Println("itter_row : ", itter_row, " : ", csv_reader_option.RowDataAt)
					// fmt.Printf("%+v\n", rec)
					if (itter_row+1) >= csv_reader_option.RowDataAt && len(rec) > 0 {
						// fmt.Printf("%+v\n", rec)

						var lb_continue bool = true
						var data []string = []string{}
						if csv_reader_option.OnImportProcessEachRow != nil {
							lb_continue, data = csv_reader_option.OnImportProcessEachRow(itter_no, rec, err)
						} else {
							data = rec
						}

						if !lb_continue {
							final_err = errors.New("Process Canceled")
							RemoveDirectory(d.file_path + "/" + d.write_temp_folder)
							break
						}

						var new_row []interface{} = []interface{}{}
						for itter_col, val := range data {
							var cell_val interface{} = val
							if len(arr_NumberColumnIndex) > 0 && strings.TrimSpace(val) != "" {
								_, lb_found := InInt64Slice(int64(itter_col), arr_NumberColumnIndex)
								if lb_found {
									cell_val = ToFloat64(val, 0)
								}
							}
							new_row = append(new_row, cell_val)
						}
						err = d.AddRowData(new_row)
						if err != nil {
							final_err = err
							break
						}

						itter_no++
					}

					itter_row++
				}

				total_row = itter_row
			}
		}
	}

	return total_row, final_err
}

func (d *ExcelClient) ImportFromRowStream(rows *sql.Rows, f_process_each_row func(index_no int64, column []string, data []interface{}) (bool, []interface{})) (int64, error) {
	var total_row int64 = 0
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("File is not ready to write")
	} else {

		columns, _ := rows.Columns()
		count := len(columns)
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)

		var itter_row int64 = 0
		for rows.Next() {

			for i, _ := range columns {
				valuePtrs[i] = &values[i]
			}
			rows.Scan(valuePtrs...)

			var row_header []interface{} = []interface{}{}
			var row_excel []interface{} = []interface{}{}

			if d.curr_writer_option.UseRowNo {
				var ls_header string = "RowNo"
				if d.curr_writer_option.FirstRowHeaderMapping != nil {
					var value, isExist = d.curr_writer_option.FirstRowHeaderMapping[ls_header]
					if isExist && strings.TrimSpace(value) != "" {
						ls_header = value
					}
				}
				row_header = append(row_header, ls_header)
			}

			// fmt.Println(" >> --------------------- >> ")
			for i, col := range columns {
				var v interface{}
				val := values[i]
				val_type := fmt.Sprintf("%v", reflect.TypeOf(val))

				if itter_row == 0 {
					var ls_header string = col
					if d.curr_writer_option.FirstRowHeaderMapping != nil {
						var value, isExist = d.curr_writer_option.FirstRowHeaderMapping[ls_header]
						if isExist && strings.TrimSpace(value) != "" {
							ls_header = value
						}
					}
					row_header = append(row_header, ls_header)
				}

				// fmt.Println("----------")
				// fmt.Println("col:", col)
				// fmt.Println("val:", val)
				// fmt.Println("val_type:", val_type)

				// switch v := val.(type) {
				// case int:
				// 	fmt.Println(col, ": int:", v)
				// case int8:
				// 	fmt.Println(col, ":int8:", v)
				// case int16:
				// 	fmt.Println(col, ":int16:", v)
				// case int32:
				// 	fmt.Println(col, ":int32:", v)
				// case int64:
				// 	fmt.Println(col, ":int64:", v)
				// case float64:
				// 	fmt.Println(col, "float64:", v)
				// default:
				// 	fmt.Println(col, "unknown", reflect.TypeOf(val))
				// }

				if val_type == "time.Time" {
					v = val.(time.Time).Format("2006-01-02 15:04:05")
				} else {
					b, ok := val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
				}

				if v != "" && v != "<nil>" {
					var lb_number_column = false
					if d.curr_writer_option.NumberTypeColumn != nil {
						_, lb_number_column = InStringSlice(col, d.curr_writer_option.NumberTypeColumn)
					}

					if val_type == "int64" {
						// fmt.Println("debug1a:", v, ":", fmt.Sprintf("%v", v))
						v = ToInt64(strings.Replace(fmt.Sprintf("%v", v), ",", ".", -1), 0)
						// fmt.Println("debug1b:", v)
					} else if val_type == "[]uint8" || val_type == "float64" || lb_number_column {
						// fmt.Println("debug2a:", v, ":", fmt.Sprintf("%v", v))
						v = ToFloat64(strings.Replace(fmt.Sprintf("%v", v), ",", ".", -1), 0)
						// fmt.Println("debug2b:", v)
					}
				}

				row_excel = append(row_excel, v)
			}

			if itter_row == 0 {
				if d.curr_writer_option.FirstRowHeader != nil && len(d.curr_writer_option.FirstRowHeader) > 0 {
					final_err = d.AddRowHeader(d.curr_writer_option.FirstRowHeader)
				} else {
					final_err = d.AddRowHeader(row_header)
				}

				if final_err != nil {
					RemoveDirectory(d.file_path + "/" + d.write_temp_folder)
					break
				}
			}

			lb_continue, data := f_process_each_row(itter_row, columns, row_excel)

			if !lb_continue {
				final_err = errors.New("Process Canceled")
				RemoveDirectory(d.file_path + "/" + d.write_temp_folder)
				break
			}

			if d.curr_writer_option.UseRowNo {
				data = append([]interface{}{(itter_row + 1)}, data...)
			}
			err := d.AddRowData(data)
			if err != nil {
				final_err = err
				break
			}

			itter_row++
		}

		rows.Close()

		total_row = itter_row
	}

	return total_row, final_err
}

// ----- >> Write Stream Excel Row
func (d *ExcelClient) write_row(row []interface{}, is_bold bool) error {
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("File is not ready to write")
	} else {
		row_data := make([]interface{}, len(row))
		for j, title := range row {
			if is_bold {
				row_data[j] = excelize.Cell{StyleID: d.boldStyle, Value: title}
			} else {
				row_data[j] = excelize.Cell{Value: title}
			}
		}

		var ls_row string = fmt.Sprintf("%v", (d.write_curr_row))
		err := d.curr_writer.SetRow("A"+ls_row, row_data)
		if err != nil {
			final_err = err
			d.write_is_ready = false
		} else {
			d.write_curr_row++
		}
	}

	return final_err
}

// ----- >> Save Excel
func (d *ExcelClient) Save() (string, error) {
	var final_filename string
	var final_err error

	if !d.write_is_ready {
		final_err = errors.New("file is not ready to write")
	} else if d.write_curr_file == 1 && d.write_itter_row == 1 {
		final_err = errors.New("no data written")
	} else {
		//fmt.Println("Debug Save : write_curr_row : ", d.write_curr_row)
		if d.write_curr_row > 1 {
			var ls_file_name_part string = d.file_name

			if d.write_curr_file > 1 {
				if d.curr_writer_option.FileNamePattern != "" {
					ls_file_name_part = strings.Replace(d.curr_writer_option.FileNamePattern, "{i}", fmt.Sprintf("%d", d.write_curr_file), -1) + ".xlsx"
					ls_file_name_part = strings.Replace(ls_file_name_part, "{0i}", fmt.Sprintf("%02d", d.write_curr_file), -1)
					ls_file_name_part = strings.Replace(ls_file_name_part, "{00i}", fmt.Sprintf("%03d", d.write_curr_file), -1)
					ls_file_name_part = strings.Replace(ls_file_name_part, "{000i}", fmt.Sprintf("%04d", d.write_curr_file), -1)
					ls_file_name_part = strings.Replace(ls_file_name_part, "{0000i}", fmt.Sprintf("%05d", d.write_curr_file), -1)
				} else {
					ls_file_name_part = ls_file_name_part + "-Part" + fmt.Sprintf("%03d", d.write_curr_file) + ".xlsx"
				}
			} else if d.curr_writer_option.FileNamePattern != "" {
				ls_file_name_part = strings.Replace(d.curr_writer_option.FileNamePattern, "{i}", fmt.Sprintf("%03d", d.write_curr_file), -1) + ".xlsx"
				ls_file_name_part = strings.Replace(ls_file_name_part, "{0i}", "", -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{00i}", "", -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{000i}", "", -1)
				ls_file_name_part = strings.Replace(ls_file_name_part, "{0000i}", "", -1)
			}

			err := d.save_file(ls_file_name_part)
			if err != nil {
				final_err = err
				d.write_is_ready = false
			} else {
				d.write_file_name = append(d.write_file_name, ls_file_name_part)
			}
		}

		if d.curr_writer_option.OutputFileSkipCompression {
			final_filename = strings.Join(d.write_file_name, "|")
		} else {
			// Remove if exists
			if _, err := os.Stat(d.file_path + d.file_name + `.zip`); err == nil {
				os.Remove(d.file_path + d.file_name + `.zip`)
			}

			// Get a Buffer to Write To
			outFile, err := os.Create(d.file_path + d.file_name + `.zip`)
			if err != nil {
				final_err = err
			}
			defer outFile.Close()

			if final_err == nil {
				// Create a new zip archive.
				w := zip.NewWriter(outFile)

				// Add some files to the archive.
				err = addFilesToZipFile(w, d.file_path+d.write_temp_folder+"/", "")
				if err != nil {
					final_err = err
					fmt.Println(err)
				}

				// Make sure to check the error on Close.
				err = w.Close()
				if err != nil {
					fmt.Println(err)
				}
			}

			RemoveDirectory(d.file_path + "/" + d.write_temp_folder)
			os.Remove(d.file_path + "/" + d.write_temp_folder)

			final_filename = d.file_name + `.zip`
		}

	}

	return final_filename, final_err
}

func (d *ExcelClient) save_file(file_name string) error {
	var final_err error

	if !d.write_is_ready && d.write_itter_row <= 1 {
		final_err = errors.New("file is not ready to write")
	} else {
		if err := d.curr_writer.Flush(); err != nil {
			final_err = err
			d.write_is_ready = false
		} else {
			var ls_file_output_path string = d.file_path + d.write_temp_folder + "/" + file_name
			if err := d.curr_file.SaveAs(ls_file_output_path); err != nil {
				final_err = err
				d.write_is_ready = false
			} else {
				d.Close()
			}
		}
	}

	return final_err
}

func (d *ExcelClient) Read(file_path string, opt ...ExcelClientReaderOption) ([]string, error) {
	var final_err error

	d.curr_reader_option = ExcelClientReaderOption{}
	for _, item := range opt {
		d.curr_reader_option = item
		break
	}

	d.read_is_ready = false
	if _, err := os.Stat(file_path); errors.Is(err, os.ErrNotExist) {
		final_err = errors.New("File [" + file_path + "] Not Found")
	} else {
		d.read_file_path = file_path
		res, err := excelize.OpenFile(file_path, excelize.Options{Password: d.curr_reader_option.Password})
		if err != nil {
			final_err = err
		} else {
			d.curr_file = res
			d.read_sheets = []string{}

			for _, sheet_name := range d.curr_file.GetSheetMap() {
				d.read_sheets = append(d.read_sheets, sheet_name)
			}

			d.read_is_ready = true
			d.read_curr_sheet = 0
			d.read_curr_row = 0
			d.read_count_empty_row = 0
		}
	}

	return d.read_sheets, final_err
}

func (d *ExcelClient) FetchRow() (string, int64, []string, error) {
	var sheet_name string
	var final_result []string = []string{}
	var final_err error

	if !d.read_is_ready {
		final_err = errors.New("file is not ready to read")
	} else if int(d.read_curr_sheet) >= len(d.read_sheets) {
		final_err = errors.New("EOF")
		d.curr_reader.Close()
		d.Close()
	} else if d.read_count_empty_row > 5 {

		if int(d.read_curr_sheet)+1 >= len(d.read_sheets) {
			final_err = errors.New("EOF")
			d.curr_reader.Close()
			d.Close()
		} else {
			d.read_curr_sheet++
			d.read_curr_row = 0
			d.read_count_empty_row = 0
			sheet_name, d.read_curr_row, final_result, final_err = d.FetchRow()
		}

	} else {
		sheet_name = d.read_sheets[d.read_curr_sheet]

		// First Load Sheet
		if int(d.read_curr_sheet) < len(d.read_sheets) {
			if d.read_curr_row == 0 {
				rows, err := d.curr_file.Rows(sheet_name)
				if err != nil {
					final_err = err
				} else {
					d.curr_reader = rows
				}
			}
		} else {
			final_err = errors.New("Sheet index [" + fmt.Sprintf("%d", d.read_curr_sheet) + "] overflow of [" + fmt.Sprintf("%d", len(d.read_sheets)) + "] sheet length")
			d.read_is_ready = false
		}

		if d.read_is_ready {
			if d.curr_reader.Next() {
				if d.curr_reader.Error() != nil {

					if int(d.read_curr_sheet)+1 >= len(d.read_sheets) {
						final_err = errors.New("EOF")
						d.curr_reader.Close()
						d.Close()
					} else {
						d.read_curr_sheet++
						d.read_curr_row = 0
						d.read_count_empty_row = 0
						sheet_name, d.read_curr_row, final_result, final_err = d.FetchRow()
					}

				} else {

					row, err := d.curr_reader.Columns()
					if err != nil {
						final_err = err
						d.read_is_ready = false
					} else {
						final_result = row
					}
				}

				if len(final_result) == 0 {
					d.read_count_empty_row++
				}

				d.read_curr_row++
			} else {
				if int(d.read_curr_sheet)+1 >= len(d.read_sheets) {
					final_err = errors.New("EOF")
					d.curr_reader.Close()
					d.Close()
				} else {
					d.read_curr_sheet++
					d.read_curr_row = 0
					d.read_count_empty_row = 0
					sheet_name, d.read_curr_row, final_result, final_err = d.FetchRow()
				}
			}
		}
	}

	return sheet_name, d.read_curr_row, final_result, final_err
}

func (d *ExcelClient) Close() error {
	var final_err error

	if d.curr_file != nil {
		final_err = d.curr_file.Close()
		if final_err == nil {
			d.curr_file = nil
		}
	}

	return final_err
}

func (d *ExcelClient) GetBasePath() string {
	return d.base_path
}

func addFilesToZipFile(w *zip.Writer, basePath, baseInZip string) error {
	var final_err error

	// Open the Directory
	files, err := os.ReadDir(basePath)
	if err != nil {
		final_err = err
		fmt.Println(err)
	}

	for _, file := range files {
		// fmt.Println(basePath + file.Name())
		if !file.IsDir() {
			dat, err := os.ReadFile(basePath + file.Name())
			if err != nil {
				final_err = err
				fmt.Println(err)
			}

			// Add some files to the archive.
			f, err := w.Create(baseInZip + file.Name())
			if err != nil {
				final_err = err
				fmt.Println(err)
			}
			_, err = f.Write(dat)
			if err != nil {
				fmt.Println(err)
			}
		} else if file.IsDir() {

			// Recurse
			newBase := basePath + file.Name() + "/"
			fmt.Println("Recursing and Adding SubDir: " + file.Name())
			fmt.Println("Recursing and Adding SubDir: " + newBase)

			err = addFilesToZipFile(w, newBase, baseInZip+file.Name()+"/")
			if err != nil {
				final_err = err
			}
		}
	}

	return final_err
}
