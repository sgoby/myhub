package alter_plan

import (
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/plan"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/rule"
	"github.com/sgoby/myhub/core/rule/result"
	"fmt"
	"github.com/golang/glog"
)

//
type alterPlanBuilder struct{
	stmt *sqlparser.Alter
}

func BuildAlterPlan(tb *schema.Table,stmt *sqlparser.Alter,manager *rule.RuleManager) ([]plan.Plan,error){
	if stmt == nil{
		return nil,fmt.Errorf("stmt is nil")
	}
	builder := &alterPlanBuilder{
		stmt: stmt,
	}
	rResults,err := manager.GetShardRule(tb.GetRuleName(),nil,tb.GetRuleKeyValueType())
	if err != nil{
		return nil,err
	}
	return builder.createPlans(rResults,stmt)
}
//
func (this *alterPlanBuilder)createPlans(rResults []result.RuleResult,stmt *sqlparser.Alter) ([]plan.Plan,error){
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
func  (this *alterPlanBuilder) tableNameAddSuffix(stmt sqlparser.Alter,tbSuffix string) sqlparser.Alter{
	nStmt := sqlparser.Alter{}
	nStmt = stmt
	nStmt.TableName = stmt.TableName + "_" + tbSuffix
	//newTb := nStmt.NewName.ToViewName()
	//newTb.Name = sqlparser.NewTableIdent(nStmt.NewName.Name.String() + "_" + tbSuffix)
	//nStmt.NewName = newTb
	//nStmt.Table = newTb
	glog.Info(nStmt)
	return nStmt
}