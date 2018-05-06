package hash_rule

import(
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/core/rule/result"
	"github.com/sgoby/myhub/config"
)

//config ex:<shard nodeDataBase="db1" between="0-10"/>
type RuleHash struct {
	allTableCount int
}


func NewRuleHash(rcnf config.Rule) (*RuleHash,error){
	return nil,nil
}

// if expr is ComparisonExpr, val shuld right expr
func (this *RuleHash)GetShardRule(val sqlparser.Expr) (rResults []result.RuleResult, err error){

	return
}