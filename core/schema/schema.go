/*
Copyright 2018 Sgoby.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"github.com/sgoby/myhub/config"
	"fmt"
	"strings"
	"github.com/sgoby/myhub/mysql"
)

type Schema struct {
	databasesMap map[string]*Database
}
type Database struct {
	config       config.Database
	tableMap     map[string]*Table
	blacklistSQL []string //SQL黑名单
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
func (this *Schema) GetDataBaseNames() ([]string) {
	var names []string
	for key,_ := range this.databasesMap{
		names = append(names,key)
	}
	return names
}
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
	if len(cnf.BlacklistSQL) > 0{
		blSql := strings.Replace(cnf.BlacklistSQL,"\n","",-1)
		blSql = strings.Replace(cnf.BlacklistSQL,"\r","",-1)
		db.blacklistSQL =strings.Split(blSql,";")
	}
	//
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
func (this *Database) InBlacklistSql(query string) bool {
	if len(this.blacklistSQL) < 1{
		return false
	}
	query = strings.Replace(query,"`","",-1)
	query = strings.Replace(query, "\n", "", -1)
	query = strings.Replace(query, "\r", "", -1)
	fp := mysql.GetFingerprint(query)
	for _,bfp := range this.blacklistSQL{
		if len(bfp) > 0 && fp == strings.ToLower(bfp){
			return true
		}
	}
	return false
}
//
func (this *Database) GetTableNames() (tbNames []string) {
	for tbName, _ := range this.tableMap {
		tbNames = append(tbNames, tbName)
	}
	return tbNames
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
