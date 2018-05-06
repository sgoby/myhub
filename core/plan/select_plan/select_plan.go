package select_plan

import (
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/schema"
	"fmt"
	"strings"
	"github.com/sgoby/myhub/core/rule"
	"github.com/sgoby/myhub/core/plan"
	"github.com/sgoby/myhub/core/rule/result"
	"github.com/golang/glog"
	"strconv"
)

type selectPlanBuilder struct {
	stmt          *sqlparser.Select
	limitOffset   int64
	limitRowcount int64
}

//
func NewselectPlanBuilder(stmt sqlparser.Select) (*selectPlanBuilder,error) {
	var offset, rowcount int64
	var err error
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			tbufOffset := sqlparser.NewTrackedBuffer(nil)
			stmt.Limit.Offset.Format(tbufOffset)
			offset, err = strconv.ParseInt(tbufOffset.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if stmt.Limit.Rowcount != nil {
			tbufRowcount := sqlparser.NewTrackedBuffer(nil)
			stmt.Limit.Rowcount.Format(tbufRowcount)
			rowcount, err = strconv.ParseInt(tbufRowcount.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	//
	builder := &selectPlanBuilder{
		stmt: &stmt,
		limitOffset:offset,
		limitRowcount:rowcount,
	}
	return builder,nil
}

//
func BuildSelectPlan(tb *schema.Table, stmt *sqlparser.Select, manager *rule.RuleManager) ([]plan.Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("stmt is nil")
	}
	var offset, rowcount int64
	var err error
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			tbufOffset := sqlparser.NewTrackedBuffer(nil)
			stmt.Limit.Offset.Format(tbufOffset)
			offset, err = strconv.ParseInt(tbufOffset.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if stmt.Limit.Rowcount != nil {
			tbufRowcount := sqlparser.NewTrackedBuffer(nil)
			stmt.Limit.Rowcount.Format(tbufRowcount)
			rowcount, err = strconv.ParseInt(tbufRowcount.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	//
	builder := &selectPlanBuilder{
		stmt: stmt,
		limitOffset:offset,
		limitRowcount:rowcount,
	}
	//
	expr, isFound := builder.getWhereExprByKey(tb.GetRuleKey())
	if !isFound { //
		expr = nil //get all
	}
	//
	rResults, err := manager.GetShardRule(tb.GetRuleName(), expr)
	if err != nil {
		return nil, err
	}
	return builder.createSelectStmt(rResults, stmt)
}

//
func (this *selectPlanBuilder) createSelectStmt(rResults []result.RuleResult, stmt *sqlparser.Select) ([]plan.Plan, error) {
	var plans []plan.Plan
	for _, rule := range rResults {
		mplan := plan.Plan{
			NodeDBName: rule.NodeDB,
		}
		for _, tbSuffix := range rule.TbSuffixs {
			nStmt := this.tableNameAddSuffix(*stmt, tbSuffix)
			if this.limitRowcount > 0{
				//change limit rowcount
				nStmt.Limit = new(sqlparser.Limit)
				nStmt.Limit.Offset = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d",0)))
				nStmt.Limit.Rowcount = sqlparser.NewIntVal([]byte(fmt.Sprintf("%d",this.limitOffset + this.limitRowcount)))
			}
			mplan.AddPlanQuery(&nStmt, "")
		}
		//
		plans = append(plans, mplan)
	}
	return plans, nil
}

//
func (this *selectPlanBuilder) tableNameAddSuffix(stmt sqlparser.Select, tbSuffix string) sqlparser.Select {
	nStmt := sqlparser.Select{}
	nStmt = stmt
	switch expr := nStmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		nAli := sqlparser.AliasedTableExpr{
			Partitions: expr.Partitions,
			As:         expr.As,
			Hints:      expr.Hints,
		}
		if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
			oldName := tbn.Name.String()
			newTb := tbn.ToViewName()
			newTb.Name = sqlparser.NewTableIdent(oldName + "_" + tbSuffix)
			nAli.Expr = newTb
			//glog.Info(nStmt.From[0],tbSuffix)
		}
		nStmt.From = make(sqlparser.TableExprs, 1)
		nStmt.From[0] = &nAli
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	}
	return nStmt
}

//
func (this *selectPlanBuilder) getWhereExprByKey(key string) (rExpr sqlparser.Expr, isFound bool) {
	if this.stmt.Where == nil {
		return nil, false
	}
	pExpr := this.stmt.Where.Expr
	return this.getExprByKey(pExpr, key)
}
func (this *selectPlanBuilder) getExprByKey(pExpr sqlparser.Expr, key string) (rExpr sqlparser.Expr, isFound bool) {
	switch expr := pExpr.(type) {
	case *sqlparser.AndExpr:
		glog.Info(expr, "AndExpr")
		rExpr, isFound = this.getExprByKey(expr.Right, key)
		if isFound {
			return rExpr, isFound
		}
		//
		rExpr, isFound = this.getExprByKey(expr.Left, key)
		if isFound {
			return rExpr, isFound
		}
	case *sqlparser.OrExpr:
		glog.Info(expr, "OrExpr")
		rExpr, isFound = this.getExprByKey(expr.Right, key)
		if isFound {
			return rExpr, isFound
		}
		rExpr, isFound = this.getExprByKey(expr.Left, key)
		if isFound {
			return rExpr, isFound
		}
	case *sqlparser.NotExpr:
		glog.Info(expr, "NotExpr")
		rExpr, isFound = this.getExprByKey(expr.Expr, key)
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
		rExpr, isFound = this.getExprByKey(expr.Left, key)
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
