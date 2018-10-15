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

import(
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/config"
	"fmt"
	"strings"
)
//
type Table struct {
	config config.Table
	createDll *sqlparser.DDL
}
func newTable(cnf config.Table) (*Table,error){
	tb := &Table{
		config:cnf,
	}
	if len(cnf.CreateSql) > 0{
		dll,err := sqlparser.Parse(cnf.CreateSql)
		if err != nil{
			return nil,err
		}
		var ok bool
		if tb.createDll,ok = dll.( *sqlparser.DDL);!ok{
			return nil,fmt.Errorf("not create sql: %s",cnf.CreateSql)
		}
		//
		if len(tb.config.RuleKeyValueType) < 1{
			column := tb.GetColumn(tb.config.RuleKey)
			if column != nil && strings.Contains(strings.ToLower(column.Type.Type),"char"){
				tb.config.RuleKeyValueType = "varchar"
			}
		}
	}
	return tb,nil
}
//获取自增长键名
func (this *Table) GetAutoIncrementKey() *sqlparser.ColumnDefinition{
	if this.createDll == nil{
		return nil
	}
	stmt := this.createDll
	for _,column := range stmt.TableSpec.Columns{
		if column.Type.Autoincrement{
			return column
		}
	}
	return nil
}
//
func (this *Table) Name() string{
	return this.config.Name
}
//
func (this *Table) GetRuleKey() string{
	return this.config.RuleKey
}
//
func (this *Table) GetRuleKeyValueType() string{
	return this.config.RuleKeyValueType
}
//
func (this *Table) GetRuleName() string{
	return this.config.Rule
}
//
func (this *Table) GetColumn(name string) *sqlparser.ColumnDefinition{
	if  this.createDll == nil || this.createDll.TableSpec == nil || this.createDll.TableSpec.Columns == nil{
		return nil
	}
	if len(name) < 1{
		return nil
	}
	for _, column := range this.createDll.TableSpec.Columns {
		if column.Name.String() == name{
			return column
		}
	}
	return nil
}
//
func (this *Table) GetCreateStmt() *sqlparser.DDL{
	return this.createDll
}
//
func (this *Table) GetCreateSql() string{
	if this.createDll == nil{
		return ""
	}
	return sqlparser.String(this.createDll)
}
//
func (this *Table) getRuleExprFromeWhere(expr sqlparser.Where){
}