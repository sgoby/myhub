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