package go_library

import (
	"errors"
	"strings"
)

func (d *DbClient) GetDatabases(filter ...string) ([]*DbDatabaseInfo, error) {
	var final_result []*DbDatabaseInfo = []*DbDatabaseInfo{}
	var final_error error

	ls_filter := ""
	if len(filter) > 0 {
		ls_filter = strings.Replace(strings.TrimSpace(filter[0]), `'`, `''`, -1)
	}

	var ls_sql string = ""
	if d.dbType == "mysql" {

		ls_sql = `	SELECT	x.schema_name db_name, x.default_collation_name collation_name, y.create_date
					FROM	information_schema.schemata x
							LEFT JOIN (select table_schema, min(create_time) create_date from information_schema.` + "`tables`" + ` GROUP BY table_schema) y ON y.table_schema = x.schema_name `
		if ls_filter != "" {
			ls_sql += `	WHERE x.schema_name LIKE '` + ls_filter + `' `
		}
		ls_sql += `	ORDER BY db_name `

	} else if d.dbType == "mssql" {

		ls_sql = `	SELECT	name [db_name], collation_name, create_date 
					FROM	master.sys.databases `
		if ls_filter != "" {
			ls_sql += `	WHERE name LIKE '` + ls_filter + `' `
		}
		ls_sql += `	ORDER BY [db_name]`

	} else if d.dbType == "postgres" {

		ls_sql = `	SELECT x.datname db_name, x.datcollate collation_name, '' create_date
					FROM	pg_database x `
		if ls_filter != "" {
			ls_sql += `	WHERE x.datname LIKE '` + ls_filter + `' `
		}
		ls_sql += `	ORDER BY x.db_name`

	} else if d.dbType == "presto" {

		ls_sql = `SHOW CATALOGS`

	} else if d.dbType == "oracle" {

		ls_sql = `	SELECT	USERNAME DB_NAME, DEFAULT_COLLATION COLLATION_NAME, TO_CHAR(CREATED,'YYYY-MM-DD HH24:MI:SS') CREATE_DATE
					FROM	DBA_USERS `
		if ls_filter != "" {
			ls_sql += `	WHERE UPPER(USERNAME) LIKE '` + strings.ToUpper(ls_filter) + `' `
		}
		ls_sql += `	ORDER BY DB_NAME`

	} else if d.dbType == "sqllite" {

		final_result = append(final_result, &DbDatabaseInfo{
			DatabaseName: d.dbName,
		})

	} else if d.dbType == "odbc" {

		final_result = append(final_result, &DbDatabaseInfo{
			DatabaseName: d.dbName,
		})

	}

	if ls_sql != "" {
		res, cols, err := d.FindAllString(ls_sql)
		if err != nil {
			final_error = err
		} else {
			for _, row := range res {
				databaseName := ""
				collationName := ""
				createdTime := ""
				for _, col := range cols {
					if strings.ToLower(col.ColumnName) == "db_name" {
						databaseName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "collation_name" {
						collationName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "create_date" {
						createdTime = row[col.ColumnName]
					}
				}
				final_result = append(final_result, &DbDatabaseInfo{
					DatabaseName:  databaseName,
					CollationName: collationName,
					CreatedTime:   createdTime,
				})
			}
		}
	}

	return final_result, final_error
}

func (d *DbClient) GetSchemas(databaseName ...string) ([]*DbSchemaInfo, error) {
	var final_result []*DbSchemaInfo = []*DbSchemaInfo{}
	var final_error error

	db_name := ""
	if len(databaseName) > 0 && strings.TrimSpace(databaseName[0]) != "" {
		db_name = strings.Replace(databaseName[0], `'`, `''`, -1)
	} else {
		db_name = d.dbName
	}

	var ls_sql string = ""
	if d.dbType == "mssql" {

		ls_sql = `	SELECT	CATALOG_NAME db_name, SCHEMA_NAME schema_name, SCHEMA_OWNER schema_owner
					FROM	[` + db_name + `].INFORMATION_SCHEMA.SCHEMATA
					WHERE	CATALOG_NAME = '` + db_name + `'
					ORDER BY (CASE WHEN SCHEMA_NAME ='dbo' THEN 1 ELSE 2 END)`

	} else if d.dbType == "postgres" {

		ls_sql = `	SELECT	catalog_name db_name, schema_name, schema_owner
					FROM	"` + db_name + `".information_schema.schemata
					WHERE	catalog_name = '` + db_name + `'
					ORDER BY (CASE WHEN schema_name ='public' THEN 1 ELSE 2 END) `

	} else if d.dbType == "presto" {

		ls_sql = `	SELECT	catalog_name db_name, schema_name, '' schema_owner
					FROM	"polardb_bnc".information_schema.schemata
					ORDER BY db_name`

	}

	if strings.TrimSpace(ls_sql) != "" {
		res, cols, err := d.FindAllString(ls_sql)
		if err != nil {
			final_error = err
		} else {
			for _, row := range res {
				databaseName := ""
				schema_name := ""
				schema_owner := ""
				for _, col := range cols {
					if strings.ToLower(col.ColumnName) == "db_name" {
						databaseName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "schema_name" {
						schema_name = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "schema_owner" {
						schema_owner = row[col.ColumnName]
					}

				}
				final_result = append(final_result, &DbSchemaInfo{
					DatabaseName: databaseName,
					SchemaName:   schema_name,
					SchemaOwner:  schema_owner,
				})
			}
		}
	}

	return final_result, final_error
}

func (d *DbClient) GetTables(databaseName ...string) ([]*DbTableInfo, error) {
	var final_result []*DbTableInfo = []*DbTableInfo{}
	var final_error error

	db_name := ""
	if len(databaseName) > 0 && strings.TrimSpace(databaseName[0]) != "" {
		db_name = strings.Replace(databaseName[0], `'`, `''`, -1)
	} else {
		db_name = d.dbName
	}

	schema_name := ""
	if len(databaseName) > 1 && strings.TrimSpace(databaseName[1]) != "" {
		schema_name = strings.Replace(databaseName[1], `'`, `''`, -1)
	} else {
		schema_name = d.dbDefaultSchema
	}

	var ls_sql string = ""
	if d.dbType == "mysql" {

		ls_sql = `	SELECT	table_schema db_name, 
							'' table_schema,
							table_name, 
							table_type,
							create_time create_date, 
							update_time modify_date, 
							table_rows num_rows,
							round(((data_length + index_length) / 1024), 2) total_space_kb
					FROM	information_schema.tables 
					WHERE	table_schema = '` + db_name + `'
					ORDER BY table_type, table_name `

	} else if d.dbType == "mssql" {

		ls_sql = `	SELECT	x.table_catalog db_name, x.table_schema table_schema, x.table_name, x.table_type, y.create_date, y.modify_date, y.num_rows, y.total_space_kb, y.used_space_kb, y.unused_space_kb
					FROM	[` + db_name + `].information_schema.tables x WITH(NOLOCK)
							LEFT JOIN (
								SELECT 
									s.Name AS schema_name,
									t.NAME AS table_name,
									min(t.create_date) create_date,
									min(t.modify_date) modify_date,
									p.rows num_rows,
									SUM(a.total_pages) * 8 AS total_space_kb,
									SUM(a.used_pages) * 8 AS used_space_kb, 
									(SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS unused_space_kb
								FROM 
									[` + db_name + `].sys.tables t
								INNER JOIN      
									[` + db_name + `].sys.indexes i ON t.OBJECT_ID = i.object_id
								INNER JOIN 
									[` + db_name + `].sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
								INNER JOIN 
									[` + db_name + `].sys.allocation_units a ON p.partition_id = a.container_id
								LEFT OUTER JOIN 
									[` + db_name + `].sys.schemas s ON t.schema_id = s.schema_id
								WHERE 
									t.NAME NOT LIKE 'dt%' 
									AND t.is_ms_shipped = 0
									AND i.OBJECT_ID > 255 
								GROUP BY 
									t.Name, s.Name, p.Rows
							) y ON y.schema_name = x.table_schema AND y.table_name = x.table_name `
		if strings.TrimSpace(schema_name) != "" {
			ls_sql += `	WHERE x.table_schema = '` + schema_name + `' `
		}
		ls_sql += `	ORDER BY x.table_type, (CASE WHEN table_schema = 'dbo' THEN 1 ELSE 2 END), table_name`

	} else if d.dbType == "postgres" {

		ls_sql = `	SELECT	table_catalog db_name, 
							table_schema,
							table_name, 
							table_type,
							null create_date, 
							null modify_date, 
							COALESCE((
								SELECT reltuples::bigint AS estimate
								FROM   pg_class
								WHERE  oid = (table_schema||'.'||table_name)::regclass
							),0) num_rows,
							round((pg_total_relation_size(table_schema||'.'||table_name) / 1024), 2) total_space_kb
					FROM	information_schema.tables
					WHERE	table_catalog = '` + db_name + `' `
		if strings.TrimSpace(schema_name) != "" {
			ls_sql += `		AND table_schema = '` + schema_name + `' `
		}
		ls_sql += `	ORDER BY table_type, (CASE WHEN table_schema = 'public' THEN 1 ELSE 2 END), table_name`

	} else if d.dbType == "presto" {

		ls_sql = `	SELECT	"` + db_name + `" db_name, table_catalog db_name, table_schema, table_name, table_type
					FROM	"` + db_name + `".information_schema.tables
					WHERE	table_schema  = '` + schema_name + `'
					ORDER BY db_name, table_schema, table_type, table_name `

	} else if d.dbType == "oracle" {

		ls_sql = `	SELECT	X.OWNER DB_NAME,
							'' TABLE_SCHEMA,
							X.TABLE_NAME TABLE_NAME,
							'BASE TABLE' TABLE_TYPE,
							X.TABLESPACE_NAME,
							TO_CHAR(Y.CREATED,'YYYY-MM-DD HH24:MI:SS') CREATE_DATE,
							TO_CHAR(Y.LAST_DDL_TIME,'YYYY-MM-DD HH24:MI:SS') MODIFY_DATE,
							X.NUM_ROWS,
							ROUND((NUM_ROWS*AVG_ROW_LEN)/(1024.000),2) TOTAL_SPACE_KB
					FROM	ALL_TABLES X
							LEFT JOIN DBA_OBJECTS Y ON Y.OBJECT_TYPE = 'TABLE' AND Y.OWNER = X.OWNER AND Y.OBJECT_NAME = X.TABLE_NAME
					WHERE	UPPER(X.OWNER) = '` + strings.ToUpper(db_name) + `'
					ORDER BY DB_NAME, X.TABLE_NAME`

	} else if d.dbType == "sqllite" {

		ls_sql = `	SELECT	tbl_name table_name 
					FROM	sqlite_master 
					WHERE	type='table'
					ORDER BY table_name`

	} else if d.dbType == "odbc" {
		ls_sql = `	`
	}

	if ls_sql != "" {
		res, cols, err := d.FindAllString(ls_sql)
		if err != nil {
			final_error = err
		} else {
			for _, row := range res {
				tableSchema := ""
				tableName := ""
				tableType := ""
				tablespaceName := ""
				createDate := ""
				modifyDate := ""
				var numRows int64 = 0.00
				var totalSpaceKb float64 = 0.00
				for _, col := range cols {
					if strings.ToLower(col.ColumnName) == "table_schema" {
						tableSchema = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "table_name" {
						tableName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "table_type" {
						tableType = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "tablespace_name" {
						tablespaceName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "create_date" {
						createDate = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "modify_date" {
						modifyDate = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "num_rows" {
						numRows = ToInt64(row[col.ColumnName], 0)
					} else if strings.ToLower(col.ColumnName) == "total_space_kb" {
						totalSpaceKb = ToFloat64(row[col.ColumnName], 0)
					}
				}
				final_result = append(final_result, &DbTableInfo{
					TableSchema:    tableSchema,
					TableName:      tableName,
					TableType:      tableType,
					TablespaceName: tablespaceName,
					CreateDate:     createDate,
					ModifyDate:     modifyDate,
					NumRows:        numRows,
					TotalSpaceKb:   totalSpaceKb,
				})
			}
		}
	}

	return final_result, final_error
}

func (d *DbClient) GetColumns(databaseName string, schemaName string, tableName string) ([]*DbColumnInfo, error) {
	var final_result []*DbColumnInfo = []*DbColumnInfo{}
	var final_error error

	db_name := ""
	if strings.TrimSpace(databaseName) != "" {
		db_name = strings.Replace(databaseName, `'`, `''`, -1)
	} else {
		db_name = d.dbName
	}

	schema_name := ""
	if strings.TrimSpace(schemaName) != "" {
		schema_name = strings.Replace(schemaName, `'`, `''`, -1)
	} else {
		schema_name = d.dbDefaultSchema
	}

	if strings.TrimSpace(tableName) == "" {
		return nil, errors.New("table name is blank")
	}

	var ls_sql string = ""
	if d.dbType == "mysql" {

		ls_sql = `	SELECT	table_schema db_name, 
							lower(table_name) table_name,
							column_name field_name,
							(CASE 
								WHEN data_type IN ('date')
									THEN 'date'
								WHEN data_type IN ('datetime','datetime2','timestamp', 'datetimeoffset', 'timestamp')
									THEN 'datetime'
								WHEN DATA_TYPE IN ('int','integer','tinyint','smallint','mediumint','bigint')
									THEN 'int'
								WHEN DATA_TYPE IN ('numeric', 'decimal', 'double', 'double precision', 'float', 'money', 'real')
									THEN 'number'
								ELSE 'string'
							END) type_data,
							(CASE 
								WHEN COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) > 0
									THEN CHARACTER_MAXIMUM_LENGTH
								WHEN COALESCE(NUMERIC_PRECISION, 0) > 0
									THEN NUMERIC_PRECISION
								ELSE 0
							END) character_length,
							COALESCE(NUMERIC_SCALE, 0) 'precision',
							(CASE 
									WHEN LOWER(IS_NULLABLE) = 'yes'
										THEN '0'
									ELSE '1'
							END) st_required,
							column_key,
							ordinal_position order_no
					FROM	information_schema.columns
					WHERE	table_schema = '` + db_name + `'
							AND table_name = '` + tableName + `' 
					ORDER BY db_name, table_name, ordinal_position`

	} else if d.dbType == "mssql" {

		ls_sql = `	SELECT	table_catalog db_name, 
							table_schema,
							lower(table_name) table_name,
							column_name field_name,
							(CASE 
								WHEN data_type IN ('date')
									THEN 'date'
								WHEN data_type IN ('datetime','datetime2','timestamp', 'datetimeoffset', 'timestamp')
									THEN 'datetime'
								WHEN DATA_TYPE IN ('int','integer','tinyint','smallint','mediumint','bigint')
									THEN 'int'
								WHEN DATA_TYPE IN ('numeric', 'decimal', 'double', 'double precision', 'float', 'money', 'real')
									THEN 'number'
								ELSE 'string'
							END) type_data,
							(CASE 
								WHEN COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) > 0
									THEN CHARACTER_MAXIMUM_LENGTH
								WHEN COALESCE(NUMERIC_PRECISION, 0) > 0
									THEN NUMERIC_PRECISION
								ELSE 0
							END) character_length,
							COALESCE(NUMERIC_SCALE, 0) 'precision',
							(CASE 
									WHEN LOWER(IS_NULLABLE) = 'yes'
										THEN '0'
									ELSE '1'
							END) st_required,
							(CASE 
								WHEN ISNULL(y.xcolumn_name, '') != ''
									THEN 'PRI'
								ELSE ''
							END) column_key,
							ordinal_position order_no
					FROM	information_schema.columns x
							LEFT JOIN (
								SELECT	tc.constraint_name xconstraint_name,
										cc.table_name xtable_name,
										cc.column_name xcolumn_name
								FROM	information_schema.table_constraints tc
										INNER JOIN information_schema.constraint_column_usage cc ON TC.Constraint_Name = CC.Constraint_Name
								WHERE	tc.constraint_type IN (
											'PRIMARY KEY','UNIQUE'
										)
										AND cc.table_catalog = '` + db_name + `'
										AND cc.table_name = '` + tableName + `'
							) y ON y.xCOLUMN_NAME = x.COLUMN_NAME
					WHERE	table_schema = '` + schema_name + `'
							AND table_name = '` + tableName + `' 
					ORDER BY db_name, table_schema, table_name, ordinal_position`

	} else if d.dbType == "postgres" {

		ls_sql = `	SELECT	table_schema db_name, 
							lower(table_name) table_name,
							column_name field_name,
							(CASE 
								WHEN data_type IN ('date')
									THEN 'date'
								WHEN data_type IN ('datetime','datetime2','timestamp', 'datetimeoffset', 'timestamp')
									THEN 'datetime'
								WHEN DATA_TYPE IN ('int','integer','tinyint','smallint','mediumint','bigint')
									THEN 'int'
								WHEN DATA_TYPE IN ('numeric', 'decimal', 'double', 'double precision', 'float', 'money', 'real')
									THEN 'number'
								ELSE 'string'
							END) type_data,
							(CASE 
								WHEN COALESCE(CHARACTER_MAXIMUM_LENGTH, 0) > 0
									THEN CHARACTER_MAXIMUM_LENGTH
								WHEN COALESCE(NUMERIC_PRECISION, 0) > 0
									THEN NUMERIC_PRECISION
								ELSE 0
							END) character_length,
							COALESCE(NUMERIC_SCALE, 0) "precision",
							(CASE 
									WHEN LOWER(IS_NULLABLE) = 'yes'
										THEN '0'
									ELSE '1'
							END) st_required,
							(CASE 
								WHEN y.attname = x.column_name THEN 'PRI'
								ELSE ''
							END) column_key,
							ordinal_position order_no
					FROM	"` + db_name + `".information_schema.columns x
							LEFT JOIN (
								SELECT a.attname
								FROM   pg_index i
								JOIN   pg_attribute a ON a.attrelid = i.indrelid
																		AND a.attnum = ANY(i.indkey)
								WHERE  i.indrelid = '` + tableName + `'::regclass
								AND    i.indisprimary
								GROUP BY a.attname
							) y ON y.attname = x.column_name
					WHERE	table_schema = '` + schema_name + `'
							AND table_name = '` + tableName + `' 
					ORDER BY db_name, table_name, ordinal_position `

	} else if d.dbType == "presto" {

		ls_sql = `	`

	} else if d.dbType == "oracle" {

		ls_sql = `	`

	} else if d.dbType == "sqllite" {

		ls_sql = `	`

	} else if d.dbType == "odbc" {
		ls_sql = `	`
	}

	if ls_sql != "" {
		res, cols, err := d.FindAllString(ls_sql)
		if err != nil {
			final_error = err
		} else {
			for _, row := range res {
				columnName := ""
				fieldType := ""
				databaseType := ""
				var columnLength int64 = 0
				var precision int64 = 0
				var isRequired bool = false
				var isPrimaryKey bool = false
				var sequence int64 = 0
				for _, col := range cols {
					if strings.ToLower(col.ColumnName) == "column_name" {
						columnName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "field_type" {
						fieldType = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "database_type" {
						databaseType = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "column_length" {
						columnLength = ToInt64(row[col.ColumnName], 0)
					} else if strings.ToLower(col.ColumnName) == "precision" {
						precision = ToInt64(row[col.ColumnName], 0)
					} else if strings.ToLower(col.ColumnName) == "is_required" && row[col.ColumnName] == "1" {
						isRequired = true
					} else if strings.ToLower(col.ColumnName) == "is_primary_key" && row[col.ColumnName] == "1" {
						isPrimaryKey = true
					} else if strings.ToLower(col.ColumnName) == "sequence" {
						sequence = ToInt64(row[col.ColumnName], 0)
					}
				}
				final_result = append(final_result, &DbColumnInfo{
					ColumnName:   columnName,
					FieldType:    fieldType,
					DatabaseType: databaseType,
					ColumnLength: columnLength,
					Precision:    precision,
					IsRequired:   isRequired,
					IsPrimaryKey: isPrimaryKey,
					Sequence:     sequence,
				})
			}
		}
	}

	return final_result, final_error
}

func (d *DbClient) GetViews(databaseName ...string) ([]*DbViewInfo, error) {
	var final_result []*DbViewInfo = []*DbViewInfo{}
	var final_error error

	db_name := ""
	if len(databaseName) == 0 || strings.TrimSpace(databaseName[0]) == "" {
		db_name = d.dbName
	}

	schema_name := ""
	if len(databaseName) < 1 || strings.TrimSpace(databaseName[1]) == "" {
		schema_name = d.dbDefaultSchema
	}

	var ls_sql string = ""
	if d.dbType == "mysql" {

		ls_sql = `	SELECT	table_schema db_name, 
							'' table_schema,
							table_name, 
							view_definition,
							definer
					FROM	information_schema.views
					WHERE	table_schema = '` + db_name + `' 
					ORDER BY table_schema, table_name`

	} else if d.dbType == "mssql" {

		ls_sql = `	SELECT	table_catalog db_name, 
							table_schema,
							table_name, 
							view_definition,
							null definer
					FROM	information_schema.views
					WHERE	table_catalog = '` + db_name + `' `
		if strings.TrimSpace(schema_name) != "" {
			ls_sql += `	WHERE table_schema = '` + schema_name + `' `
		}
		ls_sql += `	ORDER BY table_catalog, (CASE WHEN table_schema='dbo' THEN 1 ELSE 2 END), table_name `

	} else if d.dbType == "postgres" {

		ls_sql = `	SELECT	table_catalog db_name, 
							table_schema,
							table_name, 
							view_definition,
							null definer
					FROM	information_schema.views
					WHERE	table_catalog = '` + db_name + `' `
		if strings.TrimSpace(schema_name) != "" {
			ls_sql += `		AND table_schema = '` + schema_name + `' `
		}
		ls_sql += `	ORDER BY table_catalog, (CASE WHEN table_schema='public' THEN 1 ELSE 2 END), table_name `

	} else if d.dbType == "presto" {

		ls_sql = `	SELECT	"` + db_name + `" db_name, table_catalog db_name, table_schema, table_name, table_type
					FROM	"` + db_name + `".information_schema.tables
					WHERE	table_schema  = 'dwd'
					ORDER BY db_name, table_schema, table_type, table_name `

	} else if d.dbType == "oracle" {

		ls_sql = `	SELECT	OWNER DB_NAME, 
							'' TABLE_SCHEMA,
							VIEW_NAME TABLE_NAME, 
							TEXT VIEW_DEFINITION,
							OWNER DEFINER
					FROM	ALL_VIEWS
					WHERE	UPPER(OWNER) = '` + strings.ToUpper(db_name) + `'
					ORDER BY OWNER, TABLE_NAME `

	} else if d.dbType == "sqllite" {

		ls_sql = `	SELECT	'' db_name, 
							'' table_schema,
							tbl_name table_name, 
							sql view_definition,
							null definer
					FROM	sqlite_master 
					WHERE	type = 'view'
					ORDER BY table_name
`

	} else if d.dbType == "odbc" {
		ls_sql = `	`
	}

	if ls_sql != "" {
		res, cols, err := d.FindAllString(ls_sql)
		if err != nil {
			final_error = err
		} else {
			for _, row := range res {
				tableSchema := ""
				tableName := ""
				viewDefinition := ""
				definer := ""
				for _, col := range cols {
					if strings.ToLower(col.ColumnName) == "table_schema" {
						tableSchema = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "table_name" {
						tableName = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "view_definition" {
						viewDefinition = row[col.ColumnName]
					} else if strings.ToLower(col.ColumnName) == "definer" {
						definer = row[col.ColumnName]
					}
				}
				final_result = append(final_result, &DbViewInfo{
					TableSchema:    tableSchema,
					TableName:      tableName,
					ViewDefinition: viewDefinition,
					Definer:        definer,
				})
			}
		}
	}

	return final_result, final_error
}
