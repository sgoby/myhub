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

package rule

import (
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/core/rule/stmt"
	"github.com/sgoby/myhub/core/rule/result"
	"errors"
	"fmt"
)

type RuleManager struct {
	RuleMap map[string]IRule
}


const (
	RULE_HASH  = "hash"
	RULE_DATE  = "date"
	RULE_RANGE = "range"
)

//
type IRule interface {
	//获取分表后缀
	GetShardRule(expr sqlparser.Expr) (rResults []result.RuleResult, err error)
}
//
func NewRuleManager(cnfs []config.Rule) (*RuleManager, error) {
	rManger := new(RuleManager)
	rManger.RuleMap = make(map[string]IRule)
	var rule IRule
	var err error
	for _, cnf := range cnfs {
		if cnf.RuleType == RULE_RANGE {
			rule, err = stmt.NewRuleRange(cnf)
			if err != nil {
				return nil, err
			}
		} else if cnf.RuleType == RULE_DATE {
			rule, err = stmt.NewRuleDateRange(cnf)
			if err != nil {
				return nil, err
			}
		} else if cnf.RuleType == RULE_HASH {
			rule, err = stmt.NewRuleHash(cnf)
			if err != nil {
				return nil, err
			}
		}
		rManger.RuleMap[cnf.Name] = rule
	}
	return rManger, nil
}
//if expr is nil, will get all shard table
func (this *RuleManager) GetShardRule(ruleName string,expr sqlparser.Expr) (rResults []result.RuleResult, err error) {
	rule,ok :=  this.RuleMap[ruleName]
	if !ok{
		return rResults,errors.New(fmt.Sprintf("Not found rule: %s",ruleName))
	}
	rResults,err = rule.GetShardRule(expr)
	if err != nil{
		return;
	}
	//rule.GetShardRule(expr)
	return  this.removeRepRuleResult(rResults),err
}
//去掉重复的TbSuffixs
func (this *RuleManager) removeRepRuleResult(rResults []result.RuleResult)([]result.RuleResult){
	tempTbSuffixs := []result.RuleResult{}  // 存放结果
	for _,ts := range rResults{
		isFind := false;
		for  _,tempTs := range tempTbSuffixs{
			if ts.Equal(&tempTs){
				isFind = true;
				break;
			}
		}
		if !isFind{
			tempTbSuffixs = append(tempTbSuffixs,ts)
		}
	}
	return tempTbSuffixs
}