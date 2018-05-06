package rule

import (
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/core/rule/range_rule"
	"github.com/sgoby/myhub/core/rule/hash_rule"
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
			rule, err = range_rule.NewRuleRange(cnf)
			if err != nil {
				return nil, err
			}
		} else if cnf.RuleType == RULE_HASH {
			rule, err = hash_rule.NewRuleHash(cnf)
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
	return rule.GetShardRule(expr)
}
