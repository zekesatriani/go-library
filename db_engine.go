package go_library

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/prestodb/presto-go-client/presto"
	_ "github.com/sijms/go-ora/v2"
)

type DbClient struct {
	db     *sql.DB
	errCon error

	dbType            string
	dbHost            string
	dbPort            int
	dbUser            string
	dbPass            string
	dbName            string
	dbDefaultSchema   string
	dbPostgresSslMode string
	tzAppLocation     *time.Location
	tzDbLocation      *time.Location

	connString string

	hasSchema            bool
	bracketObjectOpen    string
	bracketObjectClose   string
	dateFormatToChar     string
	datetimeFormatToChar string

	maxOpenConns    int
	maxIdleConns    int
	connMaxIdleTime time.Duration
	connMaxLifetime time.Duration
}

type dbClientTask struct {
	TaskId   string
	TaskType string
	Status   string
	Sql      string

	TimeStart    int64
	TimeFinish   int64
	TimeDuration float64

	ResultSql   sql.Result
	ResultData  interface{}
	ResultError error

	CtxExec       context.Context
	CtxCancelFunc context.CancelFunc
}

var (
	dbCurrentTask map[string]dbClientTask
	dbTaskHistory []dbClientTask
)

func NewDbclient(param DbClientOption) (*DbClient, error) {

	if strings.TrimSpace(param.DbType) == "" {
		return nil, errors.New("Invalid DB-Type [" + param.DbType + "]")
	}

	if param.AppTimeZone == nil {
		param.AppTimeZone, _ = time.LoadLocation("Local")
	}

	if param.DbTimeZone == nil {
		param.DbTimeZone, _ = time.LoadLocation("Local")
	}

	if strings.TrimSpace(param.PostgresSslMode) == "" {
		param.PostgresSslMode = "disable"
	}

	connString := ""
	hasSchema := false

	bracketObjectOpen := ""
	bracketObjectClose := ""
	dateFormatToChar := ""
	datetimeFormatToChar := ""

	if param.DbType == "mysql" {
		connString = fmt.Sprintf("%s:%s@(%s:%s)/%s?charset=utf8&allowAllFiles=true", param.User, param.Password, param.Host, fmt.Sprintf("%d", param.Port), param.Database)
		hasSchema = false
		bracketObjectOpen = "`"
		bracketObjectClose = "`"
		dateFormatToChar = "date_format({input},'%Y-%m-%d')"
		datetimeFormatToChar = "date_format({input},'%Y-%m-%d %H:%i:%s')"
	} else if param.DbType == "mssql" {
		connString = fmt.Sprintf("driver={SQL Server};server=%s;port=%s;user id=%s;password=%s;database=%s;", param.Host, fmt.Sprintf("%d", param.Port), param.User, param.Password, param.Database)

		if param.DefaultSchema == "" {
			param.DefaultSchema = "dbo"
		}

		hasSchema = true
		bracketObjectOpen = "["
		bracketObjectClose = "]"
		dateFormatToChar = "CONVERT(VARCHAR(10),{input},20)"
		datetimeFormatToChar = "CONVERT(VARCHAR(19),{input},20)"
	} else if param.DbType == "postgres" {
		// connString = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", param.User, param.Password, param.Host, param.Database)
		ls_timezone := param.DbTimeZone.String()
		if ls_timezone == "" || ls_timezone == "Local" {
			connString = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", param.Host, fmt.Sprintf("%d", param.Port), param.User, param.Password, param.Database, param.PostgresSslMode)
		} else {
			connString = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=%s", param.Host, fmt.Sprintf("%d", param.Port), param.User, param.Password, param.Database, param.PostgresSslMode, ls_timezone)
		}

		if param.DefaultSchema == "" {
			param.DefaultSchema = "public"
		}

		hasSchema = true

		bracketObjectOpen = `"`
		bracketObjectClose = `"`
		dateFormatToChar = "to_char({input},'YYYY-MM-DD')"
		datetimeFormatToChar = "to_char({input},'YYYY-MM-DD HH24:MI:SS')"
	} else if param.DbType == "presto" {
		connString = fmt.Sprintf("https://%s:%s@%s:%s?catalog=default&schema=%s", param.User, param.Password, param.Host, fmt.Sprintf("%d", param.Port), param.Database)
		hasSchema = true
		// bracketObjectOpen = "`"
		// bracketObjectClose = "`"
		// dateFormatToChar = "date_format({input},'%d/%m/%Y')"
		// datetimeFormatToChar = "date_format({input},'%d/%m/%Y %H:%i:%s')"
	} else if param.DbType == "oracle" {
		connString = fmt.Sprintf("oracle://%s:%s@%s:%s/%s", param.User, param.Password, param.Host, fmt.Sprintf("%d", param.Port), param.Database)
		hasSchema = false
		bracketObjectOpen = `"`
		bracketObjectClose = `"`
		dateFormatToChar = "to_char({input},'YYYY-MM-DD')"
		datetimeFormatToChar = "to_char({input},'YYYY-MM-DD HH24:MI:SS')"
	} else if param.DbType == "sqllite" {
		connString = param.Database
		bracketObjectOpen = `"`
		bracketObjectClose = `"`
		dateFormatToChar = "{input}"
		datetimeFormatToChar = "{input}"
	} else if param.DbType == "odbc" {
		connString = fmt.Sprintf("DSN=%s;Uid=%s;Pwd=%s;", param.Host, param.User, param.Password)
	}

	final_result := &DbClient{
		dbType:               param.DbType,
		dbHost:               param.Host,
		dbPort:               param.Port,
		dbUser:               param.User,
		dbPass:               param.Password,
		dbName:               param.Database,
		dbDefaultSchema:      param.DefaultSchema,
		dbPostgresSslMode:    param.PostgresSslMode,
		tzAppLocation:        param.AppTimeZone,
		tzDbLocation:         param.DbTimeZone,
		connString:           connString,
		hasSchema:            hasSchema,
		bracketObjectOpen:    bracketObjectOpen,
		bracketObjectClose:   bracketObjectClose,
		dateFormatToChar:     dateFormatToChar,
		datetimeFormatToChar: datetimeFormatToChar,
	}

	err := final_result.Connect()
	if err != nil {
		return nil, err
	} else {
		return final_result, nil
	}

}

func (d *DbClient) GetConfig() DbClientOption {
	return DbClientOption{
		DbType:        d.dbType,
		Host:          d.dbHost,
		Port:          d.dbPort,
		User:          d.dbUser,
		Password:      d.dbPass,
		Database:      d.dbName,
		DefaultSchema: d.dbDefaultSchema,
		AppTimeZone:   d.tzAppLocation,
		DbTimeZone:    d.tzDbLocation,
	}
}

func (d *DbClient) Connect() error {
	d.db, d.errCon = sql.Open(d.dbType, d.connString)
	return d.errCon
}

func (d *DbClient) Close() error {
	return d.db.Close()
}

func (d *DbClient) SetMaxOpenConns(n int) {
	d.maxOpenConns = n
	d.db.SetMaxOpenConns(n)
}

func (d *DbClient) SetMaxIdleConns(n int) {
	d.maxIdleConns = n
	d.db.SetMaxIdleConns(n)
}

func (d *DbClient) SetConnMaxIdleTime(duration time.Duration) {
	d.connMaxIdleTime = duration
	d.db.SetConnMaxIdleTime(duration)
}

func (d *DbClient) SetConnMaxLifetime(duration time.Duration) {
	d.connMaxLifetime = duration
	d.db.SetConnMaxLifetime(duration)
}

func (d *DbClient) ExecAsync(ctx context.Context, sql string, args ...any) string {
	if err := d.db.Ping(); err != nil {
		d.Connect()
	}
	task_id := GenUUID()
	myClientTask := dbClientTask{
		TaskId:    task_id,
		TaskType:  "exec",
		Status:    "processing",
		Sql:       sql,
		TimeStart: time.Now().UTC().Unix(),
		// executionDone: "",
		// executionCtx:  "",
		// cancelFunc:    "",
	}
	dbCurrentTask[task_id] = myClientTask
	myClientTask.CtxExec, myClientTask.CtxCancelFunc = context.WithCancel(ctx)

	go func(myClientTask *dbClientTask) {
		myClientTask.ResultSql, myClientTask.ResultError = d.db.ExecContext(myClientTask.CtxExec, sql, args...)
		myClientTask.TimeFinish = time.Now().UTC().Unix()
		duration := time.Since(time.Unix(myClientTask.TimeStart, 0))
		myClientTask.TimeDuration = float64(duration.Milliseconds()) / 1000

		if myClientTask.ResultError != nil {
			myClientTask.Status = "error"
		} else {
			myClientTask.Status = "success"
		}

		dbTaskHistory = append(dbTaskHistory, *myClientTask)
		delete(dbCurrentTask, myClientTask.TaskId)
	}(&myClientTask)
	return task_id
}

func (d *DbClient) Exec(sql string, args ...any) (sql.Result, error) {
	return d.ExecContext(context.Background(), sql, args...)
}

func (d *DbClient) ExecContext(ctx context.Context, sql string, args ...any) (sql.Result, error) {
	if err := d.db.Ping(); err != nil {
		err = d.Connect()
		if err != nil {
			return nil, err
		}
	}

	if d.dbType == "postgres" && strings.Contains(sql, "?") {
		arr_temp := strings.Split(sql, "?")
		sql = ""
		var i int = 1
		for itter, val := range arr_temp {
			if itter > 0 {
				sql += "$" + fmt.Sprintf("%d", i)
				i++
			}
			sql += val
		}
	}

	return d.db.ExecContext(ctx, sql, args...)
}

func (d *DbClient) Query(sql string, args ...any) (*sql.Rows, error) {
	return d.QueryContext(context.Background(), sql, args...)
}

func (d *DbClient) QueryContext(ctx context.Context, sql string, args ...any) (*sql.Rows, error) {
	if err := d.db.Ping(); err != nil {
		err = d.Connect()
		if err != nil {
			return nil, err
		}
	}
	return d.db.QueryContext(ctx, sql, args...)
}

func (d *DbClient) QueryRows(sql string, args ...any) (*DbRows, error) {
	return d.QueryRowsContext(context.Background(), sql, args...)
}

func (d *DbClient) QueryRowsContext(ctx context.Context, sql string, args ...any) (*DbRows, error) {
	if err := d.db.Ping(); err != nil {
		err = d.Connect()
		if err != nil {
			return nil, err
		}
	}

	start_timer_sql := time.Now()

	if d.dbType == "oracle" {

		if strings.Contains(sql, "?") {
			arr_temp := strings.Split(sql, "?")
			sql = ""
			var i int = 1
			for itter, val := range arr_temp {
				if itter > 0 {
					sql += ":" + fmt.Sprintf("%d", i)
					i++
				}
				sql += val
			}
		}

		stmt, err := d.db.PrepareContext(ctx, sql)
		// defer stmt.CLose()
		if err != nil {
			return nil, err
		} else {
			// suppose we have 2 params one time.Time and other is double
			rows, err := stmt.Query(args...)
			// defer rows.Close()
			if err != nil {
				return nil, err
			} else {
				return newDbRows(d, rows, start_timer_sql, nil)
			}
		}

	} else {

		if d.dbType == "postgres" && strings.Contains(sql, "?") {
			arr_temp := strings.Split(sql, "?")
			sql = ""
			var i int = 1
			for itter, val := range arr_temp {
				if itter > 0 {
					sql += "$" + fmt.Sprintf("%d", i)
					i++
				}
				sql += val
			}
		}

		rows, err := d.db.QueryContext(ctx, sql, args...)
		return newDbRows(d, rows, start_timer_sql, err)

	}
}

func (d *DbClient) FindAll(sql string, args ...any) ([]map[string]interface{}, []*DbColumnInfo, error) {
	return d.FindAllContext(context.Background(), sql, args...)
}

func (d *DbClient) FindAllContext(ctx context.Context, sql string, args ...any) ([]map[string]interface{}, []*DbColumnInfo, error) {
	var final_result []map[string]interface{} = []map[string]interface{}{}
	var column_info []*DbColumnInfo
	var final_error error

	rows_session, err := d.QueryRowsContext(ctx, sql, args...)
	if err != nil {
		final_error = err
	} else {
		if rows_session.ErrColumnType != nil {
			final_error = rows_session.ErrColumnType
		} else {
			column_info = rows_session.ColumnInfo
			num_row, err := rows_session.FetchInterfaceRows(func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]interface{}) bool {
				lb_continue := true

				final_result = append(final_result, mapRow)

				return lb_continue
			})
			if err != nil {
				final_error = err
			} else {
				_ = num_row
			}
		}
	}

	return final_result, column_info, final_error
}

func (d *DbClient) FindAllString(sql string, args ...any) ([]map[string]string, []*DbColumnInfo, error) {
	return d.FindAllStringContext(context.Background(), sql, args...)
}

func (d *DbClient) FindAllStringContext(ctx context.Context, sql string, args ...any) ([]map[string]string, []*DbColumnInfo, error) {
	var final_result []map[string]string = []map[string]string{}
	var column_info []*DbColumnInfo
	var final_error error

	rows_session, err := d.QueryRowsContext(ctx, sql, args...)
	if err != nil {
		final_error = err
	} else {
		if rows_session.ErrColumnType != nil {
			final_error = rows_session.ErrColumnType
		} else {
			column_info = rows_session.ColumnInfo
			num_row, err := rows_session.FetchStringRows(func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]string) bool {
				lb_continue := true

				final_result = append(final_result, mapRow)

				return lb_continue
			})
			if err != nil {
				final_error = err
			} else {
				_ = num_row
			}
		}
	}

	return final_result, column_info, final_error
}

func (d *DbClient) FirstRow(sql string, args ...any) (map[string]interface{}, []*DbColumnInfo, error) {
	return d.FirstRowContext(context.Background(), sql, args...)
}

func (d *DbClient) FirstRowContext(ctx context.Context, sql string, args ...any) (map[string]interface{}, []*DbColumnInfo, error) {
	var final_result []map[string]interface{} = []map[string]interface{}{}
	var column_info []*DbColumnInfo
	var final_error error

	rows_session, err := d.QueryRowsContext(ctx, sql, args...)
	if err != nil {
		final_error = err
	} else {
		if rows_session.ErrColumnType != nil {
			final_error = rows_session.ErrColumnType
		} else {
			column_info = rows_session.ColumnInfo
			num_row, err := rows_session.FetchInterfaceRows(func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]interface{}) bool {
				final_result = append(final_result, mapRow)
				return false
			})
			if err != nil {
				final_error = err
			} else {
				_ = num_row
			}
		}
	}

	if final_error != nil {
		return nil, nil, final_error
	} else if len(final_result) <= 0 {
		return nil, column_info, err
	} else {
		return final_result[0], column_info, err
	}
}

func (d *DbClient) FirstRowString(sql string, args ...any) (map[string]string, []*DbColumnInfo, error) {
	return d.FirstRowStringtContext(context.Background(), sql, args...)
}

func (d *DbClient) FirstRowStringtContext(ctx context.Context, sql string, args ...any) (map[string]string, []*DbColumnInfo, error) {
	var final_result []map[string]string = []map[string]string{}
	var column_info []*DbColumnInfo
	var final_error error

	rows_session, err := d.QueryRowsContext(ctx, sql, args...)
	if err != nil {
		final_error = err
	} else {
		if rows_session.ErrColumnType != nil {
			final_error = rows_session.ErrColumnType
		} else {
			column_info = rows_session.ColumnInfo
			num_row, err := rows_session.FetchStringRows(func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]string) bool {
				final_result = append(final_result, mapRow)
				return false
			})
			if err != nil {
				final_error = err
			} else {
				_ = num_row
			}
		}
	}

	if final_error != nil {
		return nil, nil, final_error
	} else if len(final_result) <= 0 {
		return nil, column_info, err
	} else {
		return final_result[0], column_info, err
	}
}

func (d *DbClient) FirstValue(sql string, args ...any) (interface{}, error) {
	return d.FirstValueContext(context.Background(), sql, args...)
}

func (d *DbClient) FirstValueContext(ctx context.Context, sql string, args ...any) (interface{}, error) {
	res, col, err := d.FirstRowContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	} else if len(res) == 0 {
		return nil, err
	} else {
		return res[col[0].ColumnName], err
	}
}

func (d *DbClient) FirstValueString(sql string, args ...any) (string, error) {
	return d.FirstValueStringContext(context.Background(), sql, args...)
}

func (d *DbClient) FirstValueStringContext(ctx context.Context, sql string, args ...any) (string, error) {
	res, col, err := d.FirstRowStringtContext(ctx, sql, args...)
	if err != nil {
		return "", err
	} else if len(res) == 0 {
		return "", err
	} else {
		return res[col[0].ColumnName], err
	}
}

func (d *DbClient) Paging(aiPage int64, aiPageSize int64, sql string, args ...any) (*DataPaging, error) {
	return d.PagingContext(context.Background(), aiPage, aiPageSize, sql, args...)
}

func (d *DbClient) PagingContext(ctx context.Context, aiPage int64, aiPageSize int64, sql string, args ...any) (*DataPaging, error) {
	var paging DataPaging
	var final_error error

	arrKeywordQuery := f_splitSqlKeyword(sql)
	// fmt.Println("SQL Part : ")
	// VarDump(sql_part)

	// ----- >> Count Total Rows
	var lsSqlTotal = "SELECT COUNT(1) jml FROM " + arrKeywordQuery["FROM"] + " "
	if arrKeywordQuery["WHERE"] != "" {
		lsSqlTotal += " WHERE " + arrKeywordQuery["WHERE"]
	}
	paging.SqlCount = lsSqlTotal

	var liTotal int64 = 0
	start_timer_sql := time.Now()
	lsTotal, err := d.FirstValueStringContext(ctx, lsSqlTotal, args...)
	end_timer_sql := time.Now()
	diff := end_timer_sql.Sub(start_timer_sql)

	paging.SqlCountTimeStart = start_timer_sql.Unix()
	paging.SqlCountTimeDuration = float64(diff.Microseconds()) / float64(1000000)
	paging.SqlCountTimeEnd = end_timer_sql.Unix()

	if err != nil {
		fmt.Println("Paging Count:", err.Error())
		return &paging, err
	} else if len(lsTotal) > 0 {
		liTotal, _ = strconv.ParseInt(lsTotal, 10, 64)
	}

	paging.Total = liTotal
	paging.RowPerPage = aiPageSize

	var lnCurrentPage = aiPage
	if aiPageSize < 1 {
		aiPageSize = 1
	}

	paging.NumPage = int64(math.Ceil(float64(paging.Total) / float64(aiPageSize)))
	if lnCurrentPage > paging.NumPage {
		lnCurrentPage = paging.NumPage
	}
	if lnCurrentPage < 1 {
		lnCurrentPage = 1
	}
	paging.Page = lnCurrentPage

	// ----- >> Set Start & End Rows
	// var liStart = ((lnCurrentPage - 1) * aiPageSize) + 1
	// var liEnd = liStart + aiPageSize - 1

	// ----- >> SQL Paging
	var lsSqlWithPaging = ""

	if d.dbType == "mysql" {

		// --- MySQL

		lsSqlWithPaging = `SELECT	` + arrKeywordQuery["SELECT"] + " \n" + `FROM	` + arrKeywordQuery["FROM"]
		if strings.TrimSpace(arrKeywordQuery["WHERE"]) != "" {
			lsSqlWithPaging += "\n" + ` WHERE ` + arrKeywordQuery["WHERE"]
		}
		if strings.TrimSpace(arrKeywordQuery["ORDER BY"]) != "" {
			lsSqlWithPaging += " \n" + `ORDER BY ` + arrKeywordQuery["ORDER BY"]
		}
		lsSqlWithPaging += " \n" + `LIMIT ` + strconv.FormatInt((lnCurrentPage-1)*aiPageSize, 10) + `, ` + strconv.FormatInt(aiPageSize, 10)

	} else if d.dbType == "postgres" {

		// --- PostgreSQL / ODBC

		lsSqlWithPaging = `SELECT	` + arrKeywordQuery["SELECT"] + " \n" + `FROM	` + arrKeywordQuery["FROM"]
		if strings.TrimSpace(arrKeywordQuery["WHERE"]) != "" {
			lsSqlWithPaging += "\n" + ` WHERE ` + arrKeywordQuery["WHERE"]
		}
		if strings.TrimSpace(arrKeywordQuery["ORDER BY"]) != "" {
			lsSqlWithPaging += " \n" + `ORDER BY ` + arrKeywordQuery["ORDER BY"]
		}
		lsSqlWithPaging += " \n" + `LIMIT ` + strconv.FormatInt(aiPageSize, 10) + ` OFFSET ` + strconv.FormatInt((lnCurrentPage-1)*aiPageSize, 10)

	} else if d.dbType == "mssql" {

		// --- SQL Server

		offset := (aiPage - 1) * aiPageSize
		lsOffset := strconv.Itoa(int(offset))
		lsPageSize := strconv.Itoa(int(aiPageSize))
		lsSqlWithPaging = `SELECT	` + arrKeywordQuery["SELECT"] + `
							  FROM	` + arrKeywordQuery["FROM"] + `
							  WHERE	` + arrKeywordQuery["WHERE"] + `
							  ORDER BY ` + arrKeywordQuery["ORDER BY"] + `
							  OFFSET ` + lsOffset + ` ROWS FETCH NEXT ` + lsPageSize + ` ROWS ONLY OPTION (RECOMPILE)`

	} else if d.dbType == "oracle" {

		// --- ORACLE

		offset := (aiPage - 1) * aiPageSize
		lsOffset := strconv.Itoa(int(offset))
		lsPageSize := strconv.Itoa(int(aiPageSize))
		lsSqlWithPaging = `	SELECT	` + arrKeywordQuery["SELECT"] + `
							FROM	` + arrKeywordQuery["FROM"] + `
							WHERE	` + arrKeywordQuery["WHERE"] + `
							ORDER BY ` + arrKeywordQuery["ORDER BY"] + `
							OFFSET ` + lsOffset + ` ROWS FETCH NEXT ` + lsPageSize + ` ROWS ONLY `

	} /*else if d.pagingSequenceColumn != "" {

		var lsOffset = strconv.FormatInt(((lnCurrentPage-1)*aiPageSize)+1, 10)
		var lsLimit = strconv.FormatInt(aiPageSize, 10)
		lsSqlWithPaging = `	SELECT	` + arrKeywordQuery["SELECT"] + `
								FROM	` + arrKeywordQuery["FROM"] + ` `
		if strings.TrimSpace(arrKeywordQuery["WHERE"]) != "" {

			lsSqlWithPaging += "\n" + `	WHERE ` + arrKeywordQuery["WHERE"] + `
													AND	` + d.pagingSequenceColumn + ` >= ` + lsOffset + `
													AND ` + d.pagingSequenceColumn + ` <= ` + lsOffset + ` + ` + lsLimit + ` - 1
								ORDER BY ` + d.pagingSequenceColumn

		} else {

			lsSqlWithPaging += "\n" + ` WHERE ` + d.pagingSequenceColumn + ` >= ` + lsOffset + `
													AND ` + d.pagingSequenceColumn + ` <= ` + lsOffset + ` + ` + lsLimit + ` - 1
								ORDER BY ` + d.pagingSequenceColumn

		}

	}*/

	// Get data with Paging
	paging.SqlPaging = lsSqlWithPaging
	start_timer_sql = time.Now()
	rows, _, err := d.FindAllContext(ctx, lsSqlWithPaging, args...)
	end_timer_sql = time.Now()
	diff = end_timer_sql.Sub(start_timer_sql)

	paging.SqlPagingTimeStart = start_timer_sql.Unix()
	paging.SqlPagingTimeDuration = float64(diff.Microseconds()) / float64(1000000)
	paging.SqlPagingTimeEnd = end_timer_sql.Unix()

	if err == nil {
		if len(rows) > 0 {
			paging.Rows = rows
		} else {
			paging.Rows = []string{}
		}
	} else {
		paging.Rows = []string{}
		final_error = err
	}

	return &paging, final_error
}

func (d *DbClient) DbfToChar(as_input string) string {
	return strings.Replace(d.dateFormatToChar, "{input}", as_input, -1)
}

func (d *DbClient) DbfToLongChar(as_input string) string {
	return strings.Replace(d.datetimeFormatToChar, "{input}", as_input, -1)
}
