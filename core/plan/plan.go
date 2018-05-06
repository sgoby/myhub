package plan

import (
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
)

type Iplan interface {
	Execute() (sqltypes.Result, error)
}

type planQuery struct {
	queryStmt sqlparser.Statement
	querySql  string
}
type Plan struct {
	NodeDBName   string
	QueryContent []*planQuery //sql 语句
}
//
func NewPlan(db string) Plan {
	return Plan{
		NodeDBName: db,
	}
}
//
func (this *planQuery) GetQueryStmt() sqlparser.Statement{
	return this.queryStmt
}
//
func (this *planQuery) GetQuerySql() string{
	return this.querySql
}
//
func (this *Plan) AddPlanQuery(stmt sqlparser.Statement, sql string) (err error){
	if len(sql) < 1 && stmt == nil{
		return
	}
	if len(sql) < 1 && stmt != nil{
		sql = sqlparser.String(stmt)
	}
	if len(sql) > 0 && stmt == nil{
		stmt,err = sqlparser.Parse(sql)
		if err != nil{
			return err
		}
	}
	pq := &planQuery{
		queryStmt: stmt,
		querySql:  sql,
	}
	this.QueryContent = append(this.QueryContent,pq)
	return nil
}
