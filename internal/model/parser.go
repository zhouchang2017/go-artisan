package model

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tal-tech/go-zero/core/collection"
	"github.com/tal-tech/go-zero/tools/goctl/util/console"
	"github.com/tal-tech/go-zero/tools/goctl/util/stringx"
	"github.com/xwb1989/sqlparser"
)

var (
	errUnsupportDDL      = errors.New("unexpected type")
	errTableBodyNotFound = errors.New("create table spec not found")
	errPrimaryKey        = errors.New("unexpected join primary key")
)

var commonMysqlDataTypeMap = map[string]string{
	// For consistency, all integer types are converted to int64
	// number
	"bool":      "int64",
	"boolean":   "int64",
	"tinyint":   "int8",
	"smallint":  "int16",
	"mediumint": "int64",
	"int":       "int64",
	"integer":   "int64",
	"bigint":    "int64",
	"float":     "float64",
	"double":    "float64",
	"decimal":   "float64",
	// date&time
	"date":      "time.Time",
	"datetime":  "time.Time",
	"timestamp": "time.Time",
	"time":      "string",
	"year":      "int64",
	// string
	"char":       "string",
	"varchar":    "string",
	"binary":     "string",
	"varbinary":  "string",
	"tinytext":   "string",
	"text":       "string",
	"mediumtext": "string",
	"longtext":   "string",
	"enum":       "string",
	"set":        "string",
	"json":       "string",
}

const timeImport = "time.Time"

type (
	Table struct {
		Name          stringx.String
		structName    string
		structArgName string
		Fields        []*Field

		PrimaryKey  *Primary
		UniqueIndex map[string][]*Field
		NormalIndex map[string][]*Field
	}

	// Primary describes a primary key
	Primary struct {
		Field
		AutoIncrement bool
	}

	Field struct {
		Name            stringx.String
		argName         string
		DataBaseType    string
		DataType        string
		Comment         string
		SeqInIndex      int
		OrdinalPosition int
		isPrimaryKey    bool
	}
)

// ContainsTime returns true if contains golang type time.Time
func (t *Table) ContainsTime() bool {
	for _, item := range t.Fields {
		if strings.Contains(item.DataType, timeImport) {
			return true
		}
	}
	return false
}

// StructName returns gen struct name
func (t *Table) StructName() string {
	if t.structName == "" {
		t.structName = t.Name.ToCamel()
	}
	return t.structName
}

// StructArgName returns gen struct arg name
func (t *Table) StructArgName() string {
	if t.structArgName == "" {
		t.structArgName = stringx.From(t.StructName()).Untitle()
	}
	return t.structArgName
}

// ArgName returns field arg name
func (f *Field) ArgName() string {
	if f.argName == "" {
		f.argName = stringx.From(f.Name.ToCamel()).Untitle()
	}
	return f.argName
}

func (f *Field) IsPrimaryKey() bool {
	return f.isPrimaryKey
}

// ConvertDataType converts mysql column type into golang type
func ConvertDataType(dataBaseType string, isDefaultNull bool) (string, error) {
	tp, ok := commonMysqlDataTypeMap[strings.ToLower(dataBaseType)]
	if !ok {
		return "", fmt.Errorf("unexpected database type: %s", dataBaseType)
	}

	return mayConvertNullType(tp, isDefaultNull), nil
}

func mayConvertNullType(goDataType string, isDefaultNull bool) string {
	if !isDefaultNull {
		return goDataType
	}
	return fmt.Sprintf("*%s", goDataType)
}

func Parse(ddl string) (*Table, error) {
	stmt, err := sqlparser.ParseStrictDDL(ddl)
	if err != nil {
		return nil, err
	}

	ddlStmt, ok := stmt.(*sqlparser.DDL)
	if !ok {
		return nil, errUnsupportDDL
	}

	action := ddlStmt.Action
	if action != sqlparser.CreateStr {
		return nil, fmt.Errorf("expected [CREATE] action,but found: %s", action)
	}

	tableName := ddlStmt.NewName.Name.String()
	tableSpec := ddlStmt.TableSpec
	if tableSpec == nil {
		return nil, errTableBodyNotFound
	}

	columns := tableSpec.Columns
	indexes := tableSpec.Indexes
	primaryColumn, uniqueKeyMap, normalKeyMap, err := convertIndexes(indexes)
	if err != nil {
		return nil, err
	}
	primaryKey, fields, fieldM, err := convertColumns(columns, primaryColumn)
	if err != nil {
		return nil, err
	}

	var (
		uniqueIndex = make(map[string][]*Field)
		normalIndex = make(map[string][]*Field)
	)

	for indexName, each := range uniqueKeyMap {
		for _, columnName := range each {
			uniqueIndex[indexName] = append(uniqueIndex[indexName], fieldM[columnName])
		}
	}

	for indexName, each := range normalKeyMap {
		for _, columnName := range each {
			normalIndex[indexName] = append(normalIndex[indexName], fieldM[columnName])
		}
	}

	log := console.NewColorConsole()
	uniqueSet := collection.NewSet()
	for k, i := range uniqueIndex {
		var list []string
		for _, e := range i {
			list = append(list, e.Name.Source())
		}

		joinRet := strings.Join(list, ",")
		if uniqueSet.Contains(joinRet) {
			log.Warning("table %s: duplicate unique index %s", tableName, joinRet)
			delete(uniqueIndex, k)
			continue
		}

		uniqueSet.AddStr(joinRet)
	}

	normalIndexSet := collection.NewSet()
	for k, i := range normalIndex {
		var list []string
		for _, e := range i {
			list = append(list, e.Name.Source())
		}

		joinRet := strings.Join(list, ",")
		if normalIndexSet.Contains(joinRet) {
			log.Warning("table %s: duplicate index %s", tableName, joinRet)
			delete(normalIndex, k)
			continue
		}

		normalIndexSet.Add(joinRet)
	}

	return &Table{
		Name:        stringx.From(tableName),
		PrimaryKey:  &primaryKey,
		UniqueIndex: uniqueIndex,
		NormalIndex: normalIndex,
		Fields:      fields,
	}, nil

}

func convertColumns(columns []*sqlparser.ColumnDefinition, primaryColumn string) (Primary, []*Field, map[string]*Field, error) {
	var (
		primaryKey Primary
		fieldM     = make(map[string]*Field)
		res        = make([]*Field, 0, len(columns))
	)

	for _, column := range columns {
		if column == nil {
			continue
		}

		var comment string
		if column.Type.Comment != nil {
			comment = string(column.Type.Comment.Val)
		}

		var isDefaultNull = true
		if column.Type.NotNull {
			isDefaultNull = false
		} else {
			if column.Type.Default == nil {
				isDefaultNull = false
			} else if string(column.Type.Default.Val) != "null" {
				isDefaultNull = false
			}
		}

		dataType, err := ConvertDataType(column.Type.Type, isDefaultNull)
		if err != nil {
			return Primary{}, nil, nil, err
		}

		var field Field
		field.Name = stringx.From(column.Name.String())
		field.DataBaseType = column.Type.Type
		field.DataType = dataType
		field.Comment = comment

		if field.Name.Source() == primaryColumn {
			field.isPrimaryKey = true
			primaryKey = Primary{
				Field:         field,
				AutoIncrement: bool(column.Type.Autoincrement),
			}
		}

		if _, ok := fieldM[field.Name.Source()]; !ok {
			fieldM[field.Name.Source()] = &field
			res = append(res, &field)
		}

	}
	return primaryKey, res, fieldM, nil
}

func convertIndexes(indexes []*sqlparser.IndexDefinition) (string, map[string][]string, map[string][]string, error) {
	var primaryColumn string
	uniqueKeyMap := make(map[string][]string)
	normalKeyMap := make(map[string][]string)

	isCreateTimeOrUpdateTime := func(name string) bool {
		camelColumnName := stringx.From(name).ToCamel()
		// by default, createTime|updateTime findOne is not used.
		return camelColumnName == "CreateTime" || camelColumnName == "UpdateTime"
	}

	for _, index := range indexes {
		info := index.Info
		if info == nil {
			continue
		}

		indexName := index.Info.Name.String()
		if info.Primary {
			if len(index.Columns) > 1 {
				return "", nil, nil, errPrimaryKey
			}
			columnName := index.Columns[0].Column.String()
			if isCreateTimeOrUpdateTime(columnName) {
				continue
			}

			primaryColumn = columnName
			continue
		} else if info.Unique {
			for _, each := range index.Columns {
				columnName := each.Column.String()
				if isCreateTimeOrUpdateTime(columnName) {
					break
				}

				uniqueKeyMap[indexName] = append(uniqueKeyMap[indexName], columnName)
			}
		} else if info.Spatial {
			// do nothing
		} else {
			for _, each := range index.Columns {
				columnName := each.Column.String()
				if isCreateTimeOrUpdateTime(columnName) {
					break
				}

				normalKeyMap[indexName] = append(normalKeyMap[indexName], each.Column.String())
			}
		}
	}
	return primaryColumn, uniqueKeyMap, normalKeyMap, nil
}
