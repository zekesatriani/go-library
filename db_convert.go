package go_library

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func f_columnType2DbColumnInfo(param []*sql.ColumnType) []*DbColumnInfo {
	result := []*DbColumnInfo{}
	for _, row := range param {

		fieldType := "string"
		dbFieldType := strings.ToLower(row.DatabaseTypeName())

		if _, stringFound := InStringSlice(dbFieldType, []string{"bigint", "int", "integer", "mediumint", "smallint", "tinyint"}); stringFound {
			fieldType = "int"
		} else if _, stringFound := InStringSlice(dbFieldType, []string{"decimal", "double", "double precision", "float", "money", "numeric", "real"}); stringFound {
			fieldType = "number"
		} else if _, stringFound := InStringSlice(dbFieldType, []string{"date"}); stringFound {
			fieldType = "date"
		} else if _, stringFound := InStringSlice(dbFieldType, []string{"time"}); stringFound {
			fieldType = "time"
		} else if _, stringFound := InStringSlice(dbFieldType, []string{"datetime", "datetime2", "datetimeoffset", "timestamp"}); stringFound {
			fieldType = "datetime"
		}

		charLength, _ := row.Length()
		precision, scale, _ := row.DecimalSize()
		if charLength == 0 && precision > 0 {
			charLength = precision
		}

		isRequired := false
		nullable, _ := row.Nullable()
		if !nullable {
			isRequired = true
		}

		result = append(result, &DbColumnInfo{
			ColumnName:   row.Name(),
			FieldType:    fieldType,
			DatabaseType: dbFieldType,
			ColumnLength: charLength,
			Precision:    scale,
			IsRequired:   isRequired,
			IsPrimaryKey: false,
			// Sequence:     "",
		})
	}
	return result
}

func f_rowToMapString(rows *sql.Rows, fields []string, tzApp *time.Location, tzDb *time.Location) (map[string]string, error) {
	var scanResults = make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var s sql.NullString
		scanResults[i] = &s
	}

	if err := rows.Scan(scanResults...); err != nil {
		return nil, err
	}

	result := make(map[string]string, len(fields))
	for i, key := range fields {
		s := scanResults[i].(*sql.NullString)
		if s.String == "" {
			result[key] = ""
			continue
		}

		// switch scanResults[i].(type) {
		// case time.Time:
		// 	r, err := convert.String2Time(s.String, tzDb, tzApp)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	result[key] = r.Format("2006-01-02 15:04:05")
		// 	break
		// default:
		// 	result[key] = s.String
		// }
		result[key] = s.String

	}
	return result, nil
}

func f_rowToMapInterface(rows *sql.Rows, fields []string, tzApp *time.Location, tzDb *time.Location) (map[string]interface{}, error) {
	count := len(fields)

	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	seqCol := 0
	for i := range fields {
		valuePtrs[i] = &values[i]
		seqCol++
	}
	err := rows.Scan(valuePtrs...)
	if err != nil {
		fmt.Println("error scan")
		return nil, err
	}
	result := make(map[string]interface{}, seqCol)

	for i, col := range fields {
		rst, err := f_interface2Interface(tzDb, tzApp, values[i])
		if err != nil {
			fmt.Println("error interface2Interface")
			return nil, err
		}
		result[col] = rst
	}
	return result, nil
}

func f_interface2Interface(originalLocation *time.Location, convertedLocation *time.Location, v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch vv := v.(type) {
	case *int64, *int32, *int8, *string, *float32, *float64, *bool, int64, int32, int8, string, float32, float64, bool, []byte:
		if fmt.Sprintf("%s", reflect.TypeOf(v)) == "[]uint8" || fmt.Sprintf("%s", reflect.TypeOf(v)) == "[]uint32" || fmt.Sprintf("%s", reflect.TypeOf(v)) == "[]uint64" {
			vv, _ = strconv.ParseFloat(fmt.Sprintf("%s", v), 64)
		}
		return vv, nil
	case *[]byte:
		if len(*vv) > 0 {
			return *vv, nil
		}
		return nil, nil
	case *time.Time:
		if vv != nil {
			dtStr := vv.In(originalLocation).Format("2006-01-02 15:04:05")
			return time.ParseInLocation("2006-01-02 15:04:05", dtStr, convertedLocation)
			//dtStr := vv.In(userLocation).Format("2006-01-02 15:04:05")
			//return vv.In(userLocation), nil
			//return vv.In(userLocation).Format("2006-01-02 15:04:05"), nil
		}
		return "", nil
	case time.Time:
		dtStr := vv.In(originalLocation).Format("2006-01-02 15:04:05")
		return time.ParseInLocation("2006-01-02 15:04:05", dtStr, convertedLocation)
		//return vv.In(userLocation), nil
		//return vv.In(userLocation).Format("2006-01-02 15:04:05"), nil
	default:
		return "", fmt.Errorf("convert assign string unsupported type: %#v %T", vv, vv)
	}
}

func f_splitSqlKeyword(sql string) map[string]string {
	var arr_keyword_result map[string][]string = map[string][]string{
		"SELECT":   []string{},
		"FROM":     []string{},
		"WHERE":    []string{},
		"GROUP BY": []string{},
		"ORDER BY": []string{},
	}

	// fmt.Println("sql : ", sql)

	var tmp = ""
	var itterAfterReset int64 = 0
	var itterBracket int64 = 0
	var word = ""
	var arrWord []string
	var stCutword bool = false
	var prevKeywordPosition string = ""
	var keywordPosition string = ""

	var prevChar string = ""
	for _, runeOfWord := range sql {
		var char string = string(runeOfWord)

		// fmt.Println("--------")
		// fmt.Println("pos : ", pos)
		// fmt.Println("char : ", string(char))
		// fmt.Println("char : ", char)

		if char == "(" {
			itterBracket++
		} else if char == ")" {
			itterBracket--
		}

		if char == " " || char == "\t" || char == "\n" {
			if itterAfterReset == 0 {
				word = tmp

			}
			itterAfterReset++
			tmp = ""
			stCutword = true
		} else {
			word = ""
			tmp += char
			itterAfterReset = 0
			stCutword = false
		}

		if strings.TrimSpace(word) != "" && stCutword && !(prevChar == " " || prevChar == "\t" || prevChar == "\n") {

			// fmt.Println("--------")
			// fmt.Println("word:", word)
			// if len(arrWord) > 1 {
			// 	fmt.Println("prev word:", arrWord[len(arrWord)-1])
			// }
			// fmt.Println("keywordPosition:", keywordPosition)

			if strings.ToUpper(word) == "SELECT" && itterBracket == 0 {
				prevKeywordPosition = keywordPosition
				keywordPosition = "SELECT"
			} else if strings.ToUpper(word) == "FROM" && itterBracket == 0 {
				prevKeywordPosition = keywordPosition
				keywordPosition = "FROM"
			} else if strings.ToUpper(word) == "WHERE" && itterBracket == 0 {
				prevKeywordPosition = keywordPosition
				keywordPosition = "WHERE"
			} else if strings.ToUpper(word) == "BY" && strings.ToUpper(arrWord[len(arrWord)-1]) == "GROUP" && itterBracket == 0 {
				prevKeywordPosition = keywordPosition
				keywordPosition = "GROUP BY"

				if len(arr_keyword_result[prevKeywordPosition]) > 0 {
					arr_keyword_result[prevKeywordPosition] = arr_keyword_result[prevKeywordPosition][:len(arr_keyword_result[prevKeywordPosition])-1]
				}

			} else if strings.ToUpper(word) == "BY" && strings.ToUpper(arrWord[len(arrWord)-1]) == "ORDER" && itterBracket == 0 {
				prevKeywordPosition = keywordPosition
				keywordPosition = "ORDER BY"

				if len(arr_keyword_result[prevKeywordPosition]) > 0 {
					arr_keyword_result[prevKeywordPosition] = arr_keyword_result[prevKeywordPosition][:len(arr_keyword_result[prevKeywordPosition])-1]
				}
			} else {

				arr_keyword_result[keywordPosition] = append(arr_keyword_result[keywordPosition], word)

			}

			arrWord = append(arrWord, word)
		}

		prevChar = char
	}

	if tmp != "" {

		word = tmp
		// fmt.Println("--------")
		// fmt.Println("word:", word)
		// if len(arrWord) > 1 {
		// 	fmt.Println("prev word:", arrWord[len(arrWord)-1])
		// }
		// fmt.Println("keywordPosition:", keywordPosition)

		arr_keyword_result[keywordPosition] = append(arr_keyword_result[keywordPosition], word)

		arrWord = append(arrWord, word)
	}

	return map[string]string{
		"SELECT":   strings.Join(arr_keyword_result["SELECT"], " "),
		"FROM":     strings.Join(arr_keyword_result["FROM"], " "),
		"WHERE":    strings.Join(arr_keyword_result["WHERE"], " "),
		"GROUP BY": strings.Join(arr_keyword_result["GROUP BY"], " "),
		"ORDER BY": strings.Join(arr_keyword_result["ORDER BY"], " "),
	}
}
