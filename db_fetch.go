package go_library

import (
	"database/sql"
	"time"
)

type DbRows struct {
	db        *DbClient
	rows      *sql.Rows
	csvConfig *CsvWriterOption

	columnTypes   []*sql.ColumnType
	ColumnInfo    []*DbColumnInfo
	ErrColumnType error

	sqlDateTimeStart time.Time
	SqlTimeStart     int64
	SqlTimeEnd       int64
	SqlTimeDuration  float64

	IndexRow   int64
	FetchSpeed int64
}

func newDbRows(db *DbClient, rows *sql.Rows, start_timer_sql time.Time, err error) (*DbRows, error) {
	if err != nil {
		return nil, err
	} else {
		end_timer_sql := time.Now()
		diff := end_timer_sql.Sub(start_timer_sql)
		li_sql_load_time := float64(diff.Microseconds()) / float64(1000000)
		columnTypes, errColumnTypes := rows.ColumnTypes()
		return &DbRows{
			db:            db,
			rows:          rows,
			columnTypes:   columnTypes,
			ColumnInfo:    f_columnType2DbColumnInfo(columnTypes),
			ErrColumnType: errColumnTypes,

			sqlDateTimeStart: start_timer_sql,
			SqlTimeStart:     start_timer_sql.Unix(),
			SqlTimeEnd:       end_timer_sql.Unix(),
			SqlTimeDuration:  li_sql_load_time,

			IndexRow: 0,
		}, err
	}
}

func (d *DbRows) FetchStringRows(processRowFunc func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]string) bool) (int64, error) {
	var rows *sql.Rows = d.rows
	var idx int64 = 0

	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	if d.ErrColumnType != nil {
		return idx, d.ErrColumnType
	}

	fields, err := rows.Columns()
	if err != nil {
		return idx, err
	}

	for rows.Next() {
		result, err := f_rowToMapString(rows, fields, d.db.tzAppLocation, d.db.tzDbLocation)
		if err != nil {
			return idx, err
		}

		runNext := processRowFunc(idx, d.ColumnInfo, result)
		idx++

		d.IndexRow = idx

		if idx%5000 == 0 {
			duration := time.Since(d.sqlDateTimeStart)
			d.FetchSpeed = int64(float64(idx) / duration.Seconds())
		}

		if !runNext {
			break
		}
	}

	duration := time.Since(d.sqlDateTimeStart)
	d.FetchSpeed = int64(float64(idx) / duration.Seconds())

	if err := rows.Err(); err != nil {
		return idx, err
	}

	return idx, nil
}

func (d *DbRows) FetchInterfaceRows(processRowFunc func(idx int64, arrColumn []*DbColumnInfo, mapRow map[string]interface{}) bool) (int64, error) {
	var rows *sql.Rows = d.rows
	var idx int64 = 0

	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	if d.ErrColumnType != nil {
		return idx, d.ErrColumnType
	}

	fields, err := rows.Columns()
	if err != nil {
		return idx, err
	}

	for rows.Next() {
		result, err := f_rowToMapInterface(rows, fields, d.db.tzAppLocation, d.db.tzDbLocation)
		if err != nil {
			return idx, err
		}
		runNext := processRowFunc(idx, d.ColumnInfo, result)
		idx++

		if idx%5000 == 0 {
			duration := time.Since(d.sqlDateTimeStart)
			d.FetchSpeed = int64(float64(idx) / duration.Seconds())
		}

		d.IndexRow = idx
		if !runNext {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return idx, err
	}

	return idx, nil
}
