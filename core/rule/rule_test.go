package rule

import (
	"testing"
	"fmt"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/sqlparser"
)

//
func Test_ruleManager(t *testing.T){
	mConfig,err := config.ParseConfig("D:/workspace/golang/src/github.com/sgoby/myhub/config/conf.xml")
	if err != nil{
		fmt.Println(err)
		return
	}
	fmt.Println(mConfig.Rules)
	//
	rm,err := NewRuleManager(mConfig.Rules)
	if err != nil{
		fmt.Println(err)
		return
	}
	fmt.Println(rm)
	val := "22000"
	expr := sqlparser.NewIntVal([]byte(val))
	rs,err :=rm.GetShardRule("rang_1",expr)
	if err != nil{
		fmt.Println(err)
		return
	}
	fmt.Println(rs)
	for _,r := range rm.RuleMap{
		rs,err = r.GetShardRule(nil)
		if err != nil{
			fmt.Println(err)
			return
		}
		fmt.Println("$$$",rs)
	}
}
