package go_library

import "time"

type DbClientOption struct {
	DbType          string
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	DefaultSchema   string
	PostgresSslMode string
	AppTimeZone     *time.Location
	DbTimeZone      *time.Location
}

type DbDatabaseInfo struct {
	DatabaseName  string
	CollationName string
	CreatedTime   string
}

type DbSchemaInfo struct {
	DatabaseName string
	SchemaName   string
	SchemaOwner  string
}

type DbTableInfo struct {
	TableSchema    string
	TableName      string
	TableType      string
	TablespaceName string
	CreateDate     string
	ModifyDate     string
	NumRows        int64
	TotalSpaceKb   float64
}

type DbViewInfo struct {
	TableSchema    string
	TableName      string
	ViewDefinition string
	Definer        string
}

type DbColumnInfo struct {
	// TableName    string
	ColumnName   string
	FieldType    string
	DatabaseType string
	ColumnLength int64
	Precision    int64
	IsRequired   bool
	IsPrimaryKey bool
	Sequence     int64
}

type CsvWriterOption struct {
	FolderPath             string
	FileNamePattern        string
	MaxRowPerFile          int64
	CsvDelimiter           string
	HideRowHeader          bool
	FirstRowHeader         string
	ChecksumFileName       string
	ChecksumContentPattern string
}

type DataPaging struct {
	Page       int64       `json:"page"`
	NumPage    int64       `json:"num_page"`
	Total      int64       `json:"total"`
	RowPerPage int64       `json:"row_per_page"`
	Rows       interface{} `json:"rows"`

	SqlCount             string  `json:"-"`
	SqlCountTimeStart    int64   `json:"-"`
	SqlCountTimeEnd      int64   `json:"-"`
	SqlCountTimeDuration float64 `json:"-"`

	SqlPaging             string  `json:"-"`
	SqlPagingTimeStart    int64   `json:"-"`
	SqlPagingTimeEnd      int64   `json:"-"`
	SqlPagingTimeDuration float64 `json:"-"`
}
