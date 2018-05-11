package insert_plan

import (
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/plan"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/rule"
	"fmt"
	"github.com/sgoby/myhub/core/rule/result"
	"github.com/sgoby/myhub/utils/autoinc"
	"github.com/golang/glog"
)
//
type insertPlanBuilder struct{
	stmt *sqlparser.Insert
}
//
func BuildInsertPlan(tb *schema.Table,stmt *sqlparser.Insert,manager *rule.RuleManager,dbName string) ([]plan.Plan,error){
	if stmt == nil{
		return nil,fmt.Errorf("stmt is nil")
	}
	builder := &insertPlanBuilder{
		stmt: stmt,
	}
	valExpr,inColumns := builder.getRuleKeyValue(tb.GetRuleKey())
	if valExpr == nil{
		//is auto increment
		autoKey := tb.GetAutoIncrementKey()
		glog.Info(autoKey)
		if autoKey == tb.GetRuleKey(){
			if inColumns == false{
				mColIdent := sqlparser.NewColIdent(tb.GetRuleKey());
				stmt.Columns = append(stmt.Columns,mColIdent)
			}
			//
			valExpr = builder.getAutoIncrementBykey(dbName,tb.Name(),tb.GetRuleKey())
			values,ok := stmt.Rows.(sqlparser.Values)
			if !ok{
				return nil, fmt.Errorf("create auto value failed")
			}
			//目前只支持单行插入
			values[0]  = append(values[0],valExpr)
		}else {
			return nil, fmt.Errorf("no ruleKey value")
		}
	}
	rResults,err := manager.GetShardRule(tb.GetRuleName(),valExpr)
	if err != nil{
		return nil,err
	}
	return  builder.createInsertStmt(rResults,stmt)
}
//
func (this *insertPlanBuilder) getAutoIncrementBykey(dbName,tbName,autokey string)(expr sqlparser.Expr){
	key := fmt.Sprintf("%s.%s.%s",dbName,tbName,autokey)
	id := autoinc.GetAutoIncrement(key).GetNext()
	//
	return sqlparser.NewIntVal([]byte(fmt.Sprintf("%d",id)))
}
//
func (this *insertPlanBuilder)createInsertStmt(rResults []result.RuleResult,stmt *sqlparser.Insert) ([]plan.Plan,error){
	var plans []plan.Plan
	for _,rule := range rResults{
		mplan := plan.Plan{
			NodeDBName:rule.NodeDB,
		}
		for _,tbSuffix := range rule.TbSuffixs{
			nStmt := this.tableNameAddSuffix(*stmt,tbSuffix)
			mplan.AddPlanQuery(&nStmt,"")
		}
		//
		plans = append(plans,mplan)
	}
	return plans,nil
}
//
func (this *insertPlanBuilder) getRuleKeyValue(ruleKey string) (expr sqlparser.Expr,inColumns bool){
	keyIndex := -1
	for index,colemn := range this.stmt.Columns{
		if colemn.String() == ruleKey{
			keyIndex = index
			break;
		}
	}
	//
	if keyIndex < 0 {
		return
	}
	//
	values,ok := this.stmt.Rows.(sqlparser.Values)
	if !ok{
		return nil,true
	}
	if len(values) < 1{
		return nil,true
	}
	//
	if len(values[0]) < keyIndex{
		return nil,true
	}
	valExpr := values[0][keyIndex]
	return valExpr,true
}
//
func  (this *insertPlanBuilder) tableNameAddSuffix(stmt sqlparser.Insert,tbSuffix string) sqlparser.Insert{
	nStmt := sqlparser.Insert{}
	nStmt = stmt
	newTb := nStmt.Table.ToViewName()
	newTb.Name = sqlparser.NewTableIdent(nStmt.Table.Name.String() + "_" + tbSuffix)
	nStmt.Table = newTb
	return nStmt
}