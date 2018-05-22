package update_plan

import (
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/plan"
	"github.com/sgoby/myhub/core/rule"
	"fmt"
	"github.com/sgoby/myhub/core/rule/result"
)

type updatePlanBuilder struct {
	plan.PlanBuilder
	stmt *sqlparser.Update
}
func NewselectPlanBuilder(stmt sqlparser.Update)*updatePlanBuilder{
	builder := &updatePlanBuilder{
		stmt:&stmt,
	}
	return builder
}
func BuildUpdatePlan(tb *schema.Table,stmt *sqlparser.Update,manager *rule.RuleManager) ([]plan.Plan,error){
	if stmt == nil{
		return nil,fmt.Errorf("stmt is nil")
	}
	builder := &updatePlanBuilder{
		stmt: stmt,
	}
	//
	if builder.updateHasRuleKey(tb.GetRuleKey()){
		return nil,fmt.Errorf("can't update ruleKey: %s",tb.GetRuleKey())
	}
	//
	expr,isFound := builder.getWhereExprByKey(tb.GetRuleKey())
	if !isFound{ //
		expr = nil //get all
	}
	//
	rResults,err := manager.GetShardRule(tb.GetRuleName(),expr)
	if err != nil{
		return nil,err
	}
	return builder.createSelectStmt(rResults,stmt)
}
//
func (this *updatePlanBuilder) updateHasRuleKey(ruleKey string) bool {
	for _,expr := range this.stmt.Exprs{
		if expr.Name.Name.String() == ruleKey{
			return true
		}
	}
	return false
}
//
func (this *updatePlanBuilder) createSelectStmt(rResults []result.RuleResult,stmt *sqlparser.Update) ([]plan.Plan,error){
	var plans []plan.Plan
	for _,rule := range rResults{
		mplan := plan.Plan{
			NodeDBName:rule.NodeDB,
		}
		for _,tbSuffix := range rule.TbSuffixs{
			nStmt := this.tableNameAddSuffix(*stmt,rule.NodeDB,tbSuffix)
			mplan.AddPlanQuery(&nStmt,"")
		}
		//
		plans = append(plans,mplan)
	}
	return plans,nil
}
//
func  (this *updatePlanBuilder) tableNameAddSuffix(stmt sqlparser.Update,dbName,tbSuffix string) sqlparser.Update{
	nStmt := sqlparser.Update{}
	nStmt = stmt
	switch expr := nStmt.TableExprs[0].(type) {
	case *sqlparser.AliasedTableExpr:
		nAli := sqlparser.AliasedTableExpr{
			Partitions:expr.Partitions,
			As:expr.As,
			Hints:expr.Hints,
		}
		if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
			oldName := tbn.Name.String()
			newTb := tbn.ToViewName()
			if !tbn.Qualifier.IsEmpty(){
				newTb.Qualifier = sqlparser.NewTableIdent(dbName)
			}
			newTb.Name = sqlparser.NewTableIdent(oldName + "_" + tbSuffix)
			nAli.Expr = newTb
			//glog.Info(nStmt.From[0],tbSuffix)
		}
		nStmt.TableExprs = make(sqlparser.TableExprs,1)
		nStmt.TableExprs[0] = &nAli
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	}
	return nStmt
}
//
func (this *updatePlanBuilder)getWhereExprByKey(key string) (rExpr sqlparser.Expr,isFound bool){
	if this.stmt.Where == nil{
		return nil,false
	}
	pExpr := this.stmt.Where.Expr
	return this.GetExprByKey(pExpr,key)
}