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

package plan

import (
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
	"github.com/golang/glog"
	"strings"
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
	QueryContent []*planQuery //sql string
}
type PlanBuilder struct {}
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
//
func (this *PlanBuilder) GetExprByKey(pExpr sqlparser.Expr, key string) (rExpr sqlparser.Expr, isFound bool) {
	switch expr := pExpr.(type) {
	case *sqlparser.AndExpr:
		glog.Info(expr, "AndExpr")
		rExpr, isFound = this.GetExprByKey(expr.Right, key)
		if isFound {
			return rExpr, isFound
		}
		//
		rExpr, isFound = this.GetExprByKey(expr.Left, key)
		if isFound {
			return rExpr, isFound
		}
	case *sqlparser.OrExpr:
		glog.Info(expr, "OrExpr")
		rExpr, isFound = this.GetExprByKey(expr.Right, key)
		if isFound {
			return rExpr, isFound
		}
		rExpr, isFound = this.GetExprByKey(expr.Left, key)
		if isFound {
			return rExpr, isFound
		}
	case *sqlparser.NotExpr:
		glog.Info(expr, "NotExpr")
		rExpr, isFound = this.GetExprByKey(expr.Expr, key)
		if isFound {
			return rExpr, isFound
		}
	case *sqlparser.ParenExpr:
		glog.Info(expr, "ParenExpr")
	case *sqlparser.ComparisonExpr:
		glog.Info(expr, "ComparisonExpr")
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Left.Format(buf)
		fieldName := buf.String()
		//glog.Info(fieldName)
		if key == strings.TrimSpace(fieldName) {
			return expr, true
		}
	case *sqlparser.RangeCond:
		glog.Info(expr.Left, "RangeCond")
		rExpr, isFound = this.GetExprByKey(expr.Left, key)
		if isFound {
			//glog.Info(expr.From,expr.Operator,expr.To,"RangeCond")
			return expr, true
		}
	case *sqlparser.IsExpr:
		glog.Info(expr, "IsExpr")
	case *sqlparser.ExistsExpr:
		glog.Info(expr, "ExistsExpr")
	case *sqlparser.SQLVal:
		glog.Info(expr, "SQLVal")
	case *sqlparser.NullVal:
		glog.Info(expr, "NullVal")
	case sqlparser.BoolVal:
		glog.Info(expr, "BoolVal")
	case *sqlparser.ColName:
		glog.Info(expr, "ColName")
		if expr.Name.String() == key {
			return expr, true
		}
	case sqlparser.ValTuple:
		glog.Info(expr, "ValTuple")
	case *sqlparser.Subquery:
		glog.Info(expr, "Subquery")
	case sqlparser.ListArg:
		glog.Info(expr, "ListArg")
	case *sqlparser.BinaryExpr:
		glog.Info(expr, "BinaryExpr")
	case *sqlparser.UnaryExpr:
		glog.Info(expr, "UnaryExpr")
	case *sqlparser.IntervalExpr:
		glog.Info(expr, "IntervalExpr")
	case *sqlparser.CollateExpr:
		glog.Info(expr, "CollateExpr")
	case *sqlparser.FuncExpr:
		glog.Info(expr, "FuncExpr")
	case *sqlparser.CaseExpr:
		glog.Info(expr, "CaseExpr")
	case *sqlparser.ValuesFuncExpr:
		glog.Info(expr, "ValuesFuncExpr")
	case *sqlparser.ConvertExpr:
		glog.Info(expr, "ConvertExpr")
	case *sqlparser.ConvertUsingExpr:
		glog.Info(expr, "ConvertUsingExpr")
	case *sqlparser.MatchExpr:
		glog.Info(expr, "MatchExpr")
	case *sqlparser.GroupConcatExpr:
		glog.Info(expr, "GroupConcatExpr")
	case *sqlparser.Default:
	default:

	}
	return
}