package schema

import (
	"github.com/sgoby/myhub/config"
	"fmt"
	"strings"
)

type Schema struct {
	databasesMap map[string]*Database
}
type Database struct {
	config   config.Database
	tableMap map[string]*Table
}

//
func NewSchema(cnf config.Schema) (*Schema, error) {
	schema := &Schema{
		databasesMap: make(map[string]*Database),
	}
	//
	for _, dcnf := range cnf.Databases {
		db, err := newDataBase(dcnf)
		if err != nil {
			return nil, err
		}
		key := strings.TrimSpace(dcnf.Name)
		schema.databasesMap[key] = db
	}
	return schema, nil
}

//===============================================================
func (this *Schema) GetDataBase(name string) (*Database, error) {
	key := strings.TrimSpace(name)
	if db, ok := this.databasesMap[key]; ok {
		return db, nil
	}
	return nil, fmt.Errorf("Unknown database '%s'", key)
}
func (this *Schema) Foreach(f func(string, *Database) error, errBreak bool) (err error) {
	for dbName, db := range this.databasesMap {
		err = f(dbName, db)
		if err != nil && errBreak {
			break
		}
	}
	return err
}

//
func newDataBase(cnf config.Database) (*Database, error) {
	db := &Database{
		config:   cnf,
		tableMap: make(map[string]*Table),
	}
	for _, tbCnf := range cnf.Tables {
		tb, err := newTable(tbCnf)
		if err != nil {
			return nil, fmt.Errorf("schema init table error: ", err)
		}
		db.tableMap[tbCnf.Name] = tb
	}
	return db, nil
}

//
func (this *Database) Foreach(f func(string, *Table) error, errBreak bool) (err error) {
	for tbName, tb := range this.tableMap {
		err = f(tbName, tb)
		if err != nil && errBreak {
			break
		}
	}
	return
}

//
func (this *Database) GetTable(tbName string) *Table {
	if tb, ok := this.tableMap[tbName]; ok {
		return tb
	}
	return nil
}

//
func (this *Database) GetProxyDbName() string {
	return this.config.ProxyDataBase
}

//
func (this *Database) GetName() string {
	return this.config.Name
}