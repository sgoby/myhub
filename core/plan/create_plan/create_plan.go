package create_plan

import (
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/plan"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/rule"
	"fmt"
	"github.com/sgoby/myhub/core/rule/result"
)


//
type createPlanBuilder struct{
	stmt *sqlparser.DDL
}
//
func BuildCreatePlan(tb *schema.Table,stmt *sqlparser.DDL,manager *rule.RuleManager) ([]plan.Plan,error){
	if stmt == nil{
		return nil,fmt.Errorf("stmt is nil")
	}
	if stmt.Action != sqlparser.CreateStr{
		return nil,fmt.Errorf("is not create stmt")
	}
	builder := &createPlanBuilder{
		stmt: stmt,
	}
	rResults,err := manager.GetShardRule(tb.GetRuleName(),nil)
	if err != nil{
		return nil,err
	}
	return  builder.createInsertStmt(rResults,stmt)
}
//
func (this *createPlanBuilder)createInsertStmt(rResults []result.RuleResult,stmt *sqlparser.DDL) ([]plan.Plan,error){
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
func  (this *createPlanBuilder) tableNameAddSuffix(stmt sqlparser.DDL,tbSuffix string) sqlparser.DDL{
	nStmt := sqlparser.DDL{}
	nStmt = stmt
	newTb := nStmt.NewName.ToViewName()
	newTb.Name = sqlparser.NewTableIdent(nStmt.NewName.Name.String() + "_" + tbSuffix)
	nStmt.NewName = newTb
	//glog.Info(nStmt)
	return nStmt
}