package schema

import(
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/config"
	"fmt"
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
	}
	return tb,nil
}
//获取自增长键名
func (this *Table) GetAutoIncrementKey()string{
	if this.createDll == nil{
		return ""
	}
	stmt := this.createDll
	for _,column := range stmt.TableSpec.Columns{
		if column.Type.Autoincrement{
			return column.Name.String()
		}
	}
	return ""
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
func (this *Table) GetRuleName() string{
	return this.config.Rule
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