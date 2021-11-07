package context

import (
	"github.com/actiontech/sqle/sqle/driver/mysql/executor"
	"github.com/actiontech/sqle/sqle/driver/mysql/util"
	"github.com/actiontech/sqle/sqle/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

type ColumnInfo struct {
	// selectivity euqals to the cardinality of column row set / table row set.
	selectivity *float64
}

type TableInfo struct {
	Size     float64
	SizeLoad bool

	// IsLoad indicate whether TableInfo load from database or not.
	IsLoad bool

	// OriginalTable save parser object from db by query "show create table ...";
	// using in inspect and generate rollback sql
	OriginalTable *ast.CreateTableStmt

	//
	MergedTable *ast.CreateTableStmt

	// save alter table parse object from input sql;
	AlterTables []*ast.AlterTableStmt

	columnInfos map[string]*ColumnInfo
}

type SchemaInfo struct {
	DefaultEngine    string
	EngineLoad       bool
	DefaultCharacter string
	CharacterLoad    bool
	DefaultCollation string
	CollationLoad    bool
	Tables           map[string]*TableInfo
}

type Context struct {
	// CurrentSchema will change after sql "use database"
	CurrentSchema string

	Schemas map[string]*SchemaInfo
	// if schemas info has collected, set true
	schemaHasLoad bool

	// executionPlan store batch SQLs' execution plan during one inspect context.
	executionPlan map[string][]*executor.ExplainRecord

	// sysVars keep some MySQL global system variables during one inspect context.
	sysVars map[string]string
}

func NewContext(parent *Context) *Context {
	ctx := &Context{
		Schemas:       map[string]*SchemaInfo{},
		executionPlan: map[string][]*executor.ExplainRecord{},
		sysVars:       map[string]string{},
	}
	if parent == nil {
		return ctx
	}
	ctx.schemaHasLoad = parent.schemaHasLoad
	ctx.CurrentSchema = parent.CurrentSchema
	for schemaName, schema := range parent.Schemas {
		newSchema := &SchemaInfo{
			Tables: map[string]*TableInfo{},
		}
		if schema == nil || schema.Tables == nil {
			continue
		}
		for tableName, table := range schema.Tables {
			newSchema.Tables[tableName] = &TableInfo{
				Size:          table.Size,
				SizeLoad:      table.SizeLoad,
				IsLoad:        table.IsLoad,
				OriginalTable: table.OriginalTable,
				MergedTable:   table.MergedTable,
				AlterTables:   table.AlterTables,
			}
		}
		ctx.Schemas[schemaName] = newSchema
	}

	for k, v := range parent.sysVars {
		ctx.sysVars[k] = v
	}
	return ctx
}

func (c *Context) GetSelectivity(tableName, columnName string) (float64, bool) {
	if schema, ok := c.GetSchema(c.CurrentSchema); ok {
		if table, ok := schema.Tables[tableName]; ok {
			if column, ok := table.columnInfos[columnName]; ok {
				if column.selectivity != nil {
					return *column.selectivity, true
				}
			}
		}
	}
	return 0, false
}

func (c *Context) SetSelectivity(tableName, columnName string, selectivity float64) {
	if schema, ok := c.GetSchema(c.CurrentSchema); ok {
		if table, ok := schema.Tables[tableName]; ok {
			if column, ok := table.columnInfos[columnName]; ok {
				column.selectivity = &selectivity
			}
		}
	}
}

func (c *Context) GetSysVar(name string) (string, bool) {
	v, exist := c.sysVars[name]
	return v, exist
}

func (c *Context) AddSysVar(name, value string) {
	c.sysVars[name] = value
	return
}

func (c *Context) HasLoadSchemas() bool {
	return c.schemaHasLoad
}

func (c *Context) SetSchemasLoad() {
	c.schemaHasLoad = true
}

func (c *Context) LoadSchemas(schemas []string) {
	if c.HasLoadSchemas() {
		return
	}
	for _, schema := range schemas {
		c.Schemas[schema] = &SchemaInfo{}
	}
	c.SetSchemasLoad()
}

func (c *Context) GetSchema(schemaName string) (*SchemaInfo, bool) {
	schema, has := c.Schemas[schemaName]
	return schema, has
}

func (c *Context) HasSchema(schemaName string) (has bool) {
	_, has = c.GetSchema(schemaName)
	return
}

func (c *Context) AddSchema(name string) {
	if c.HasSchema(name) {
		return
	}
	c.Schemas[name] = &SchemaInfo{
		Tables: map[string]*TableInfo{},
	}
}

func (c *Context) DelSchema(name string) {
	delete(c.Schemas, name)
}

func (c *Context) HasLoadTables(schemaName string) (hasLoad bool) {
	if schema, ok := c.GetSchema(schemaName); ok {
		if schema.Tables == nil {
			hasLoad = false
		} else {
			hasLoad = true
		}
	}
	return
}

func (c *Context) LoadTables(schemaName string, tablesName []string) {
	schema, ok := c.GetSchema(schemaName)
	if !ok {
		return
	}
	if c.HasLoadTables(schemaName) {
		return
	}
	schema.Tables = map[string]*TableInfo{}
	for _, name := range tablesName {
		schema.Tables[name] = &TableInfo{
			IsLoad:      true,
			AlterTables: []*ast.AlterTableStmt{},
		}
	}
}

func (c *Context) GetTable(schemaName, tableName string) (*TableInfo, bool) {
	schema, SchemaExist := c.GetSchema(schemaName)
	if !SchemaExist {
		return nil, false
	}
	if !c.HasLoadTables(schemaName) {
		return nil, false
	}
	table, tableExist := schema.Tables[tableName]
	return table, tableExist
}

func (c *Context) HasTable(schemaName, tableName string) (has bool) {
	_, has = c.GetTable(schemaName, tableName)
	return
}

func (c *Context) AddTable(schemaName, tableName string, table *TableInfo) {
	schema, exist := c.GetSchema(schemaName)
	if !exist {
		return
	}
	if !c.HasLoadTables(schemaName) {
		return
	}
	schema.Tables[tableName] = table
}

func (c *Context) DelTable(schemaName, tableName string) {
	schema, exist := c.GetSchema(schemaName)
	if !exist {
		return
	}
	delete(schema.Tables, tableName)
}

func (c *Context) UseSchema(schema string) {
	c.CurrentSchema = schema
}

func (c *Context) AddExecutionPlan(sql string, records []*executor.ExplainRecord) {
	c.executionPlan[sql] = records
}

func (c *Context) GetExecutionPlan(sql string) ([]*executor.ExplainRecord, bool) {
	records, ok := c.executionPlan[sql]
	return records, ok
}

// Update update the context with given ast node.
func (c *Context) Update(node ast.Node) {
	switch s := node.(type) {
	case *ast.UseStmt:
		// change current schema
		if c.HasSchema(s.DBName) {
			c.UseSchema(s.DBName)
		}
	case *ast.CreateDatabaseStmt:
		if c.HasLoadSchemas() {
			c.AddSchema(s.Name)
		}
	case *ast.CreateTableStmt:
		schemaName := c.GetSchemaName(s.Table)
		tableName := s.Table.Name.L
		if c.HasTable(schemaName, tableName) {
			return
		}
		c.AddTable(schemaName, tableName,
			&TableInfo{
				Size:          0, // table is empty after create
				SizeLoad:      true,
				IsLoad:        false,
				OriginalTable: s,
				AlterTables:   []*ast.AlterTableStmt{},
			})
	case *ast.DropDatabaseStmt:
		if c.HasLoadSchemas() {
			c.DelSchema(s.Name)
		}
	case *ast.DropTableStmt:
		if c.HasLoadSchemas() {
			for _, table := range s.Tables {
				schemaName := c.GetSchemaName(table)
				tableName := table.Name.L
				if c.HasTable(schemaName, tableName) {
					c.DelTable(schemaName, tableName)
				}
			}
		}

	case *ast.AlterTableStmt:
		info, exist := c.GetTableInfo(s.Table)
		if exist {
			var oldTable *ast.CreateTableStmt
			if info.MergedTable != nil {
				oldTable = info.MergedTable
			} else if info.OriginalTable != nil {
				n, err := parser.New().ParseOneStmt(info.OriginalTable.Text(), "", "")
				if err != nil {
					log.NewEntry().Errorf("update context, parse alter table %s error: %v", info.OriginalTable.Text(), err)
				}
				var ok bool
				oldTable, ok = n.(*ast.CreateTableStmt)
				if !ok {
					log.NewEntry().Errorf("update context, original table %s is not ast.CreateTableStmt", info.OriginalTable.Text())
				}
			}
			info.MergedTable, _ = util.MergeAlterToTable(oldTable, s)
			info.AlterTables = append(info.AlterTables, s)
			// rename table
			if s.Table.Name.L != info.MergedTable.Table.Name.L {
				schemaName := c.GetSchemaName(s.Table)
				c.DelTable(schemaName, s.Table.Name.L)
				c.AddTable(schemaName, info.MergedTable.Table.Name.L, info)
			}
		}
	default:
	}
}

func (c *Context) GetSchemaName(node *ast.TableName) string {
	schemaName := node.Schema.String()
	if schemaName == "" {
		schemaName = c.CurrentSchema
	}
	return schemaName
}

func (c *Context) GetTableInfo(node *ast.TableName) (*TableInfo, bool) {
	schemaName := c.GetSchemaName(node)
	table := node.Name.String()
	return c.GetTable(schemaName, table)
}
