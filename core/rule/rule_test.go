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
