package go_library

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (d *DbClient) GetWhere(table string, condition string, param ...interface{}) (map[string]string, error) {
	ls_sql := "SELECT * FROM "
	i := strings.Index(table, ".")
	if i >= -1 {
		ls_sql += table
	} else {
		ls_sql += d.bracketObjectOpen + table + d.bracketObjectClose
	}
	ls_sql += " WHERE " + condition

	res, _, err := d.FirstRowString(ls_sql, param...)
	return res, err
}

func (d *DbClient) Insert(table string, data map[string]interface{}, suffixQueryArgs ...string) (int64, error) {

	var lsSQL = "INSERT INTO " + d.bracketObjectOpen + table + d.bracketObjectClose
	var lsField = ""
	var lsValue = ""
	var args []interface{}

	var ls_template_value string = "?"
	if d.dbType == "postgres" {
		ls_template_value = "${i}"
	} else if d.dbType == "oracle" {
		ls_template_value = ":{i}"
	}

	var i int = 1
	for key, val := range data {
		if lsField != "" {
			lsField += ","
		}
		lsField += d.bracketObjectOpen + key + d.bracketObjectClose

		if lsValue != "" {
			lsValue += ","
		}
		lsValue += strings.Replace(ls_template_value, "{i}", fmt.Sprintf("%d", i), -1)

		var valToDb interface{}
		switch val.(type) {
		case time.Time:
			valToDb = val.(time.Time).In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			break
		case string:
			resultTime, err := time.Parse("2006-01-02T15:04:05.9999999-07:00", val.(string))
			if err != nil {
				valToDb = val
			} else {
				valToDb = resultTime.In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			}
			break
		default:
			valToDb = val
		}

		args = append(args, valToDb)
		i++
	}

	lsSQL += "(" + lsField + ") VALUES (" + lsValue + ")"

	strSuffixQuery := ""
	for _, suffixQuery := range suffixQueryArgs {
		if len(strings.TrimSpace(suffixQuery)) > 0 {
			strSuffixQuery += " ;" + suffixQuery
		}
	}

	if len(strSuffixQuery) > 0 {
		rows, err := d.db.Query(lsSQL+strSuffixQuery, args...)
		if err != nil {

		}
		defer func() {
			_ = rows.Close()
		}()

		var lastInsertId int64
		for rows.Next() {
			_ = rows.Scan(&lastInsertId)
		}
		return lastInsertId, err
	} else {
		_, err := d.db.Exec(lsSQL, args...)
		return -1, err
	}
}

func (d *DbClient) Update(table string, data map[string]interface{}, criteria map[string]interface{}) (sql.Result, error) {
	var finalResult sql.Result
	var finalErr error
	var lsSQL = "UPDATE " + d.bracketObjectOpen + table + d.bracketObjectClose
	var lsField = ""
	var lsCriteria = ""
	var args []interface{}

	var ls_template_value string = "?"
	if d.dbType == "postgres" {
		ls_template_value = "${i}"
	} else if d.dbType == "oracle" {
		ls_template_value = ":{i}"
	}

	var i int = 1
	for key, val := range data {
		if lsField != "" {
			lsField += ", "
		}
		lsField += d.bracketObjectOpen + key + d.bracketObjectClose + " = " + strings.Replace(ls_template_value, "{i}", fmt.Sprintf("%d", i), -1)

		var valToDb interface{}
		switch val.(type) {
		case time.Time:
			valToDb = val.(time.Time).In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			break
		case string:
			resultTime, err := time.Parse("2006-01-02T15:04:05.9999999-07:00", val.(string))
			if err != nil {
				valToDb = val
			} else {
				valToDb = resultTime.In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			}
			break
		default:
			valToDb = val
		}

		args = append(args, valToDb)
		i++
	}

	for key, val := range criteria {
		if lsCriteria != "" {
			lsCriteria += " AND "
		}
		lsCriteria += d.bracketObjectOpen + key + d.bracketObjectClose + " = " + strings.Replace(ls_template_value, "{i}", fmt.Sprintf("%d", i), -1)

		var valToDb interface{}
		switch val.(type) {
		case time.Time:
			valToDb = val.(time.Time).In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			break
		case string:
			resultTime, err := time.Parse("2006-01-02T15:04:05.9999999-07:00", val.(string))
			if err != nil {
				valToDb = val
			} else {
				valToDb = resultTime.In(d.tzDbLocation).Format("2006-01-02 15:04:05")
			}
			break
		default:
			valToDb = val
		}

		args = append(args, valToDb)
		i++
	}

	lsSQL += " SET " + lsField + " WHERE " + lsCriteria
	// fmt.Println("lsSQL:update", lsSQL, args)
	//args = append([]interface{}{ lsSQL }, args...)
	res, err := d.db.Exec(lsSQL, args...)
	if err != nil {
		// log.Fatal("Error sql statement: ", err, lsSQL)
		//fmt.Println("Error sql statement: ", err, lsSQL)
		finalErr = err
	} else {
		finalResult = res
	}

	return finalResult, finalErr
}

func (d *DbClient) Delete(table string, criteria map[string]interface{}) (sql.Result, error) {
	var finalResult sql.Result
	var finalErr error
	var lsSQL = "DELETE FROM " + d.bracketObjectOpen + table + d.bracketObjectClose
	var lsCriteria = ""
	var args []interface{}

	var ls_template_value string = "?"
	if d.dbType == "postgres" {
		ls_template_value = "${i}"
	} else if d.dbType == "oracle" {
		ls_template_value = ":{i}"
	}

	var i int = 1
	for key, val := range criteria {
		if lsCriteria != "" {
			lsCriteria += " AND "
		}
		lsCriteria += d.bracketObjectOpen + key + d.bracketObjectClose + " = " + strings.Replace(ls_template_value, "{i}", fmt.Sprintf("%d", i), -1)

		args = append(args, val)
		i++
	}

	lsSQL += " WHERE " + lsCriteria
	// fmt.Println("lsSQL:delete", lsSQL, args)
	//args = append([]interface{}{ lsSQL }, args...)
	res, err := d.db.Exec(lsSQL, args...)
	if err != nil {
		// log.Fatal("Error sql statement: ", err, lsSQL)
		//fmt.Println("Error sql statement: ", err, lsSQL)
		finalErr = err
	} else {
		finalResult = res
	}

	return finalResult, finalErr
}
