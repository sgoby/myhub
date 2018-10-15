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

package stmt



import (
	"testing"
	"fmt"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/sqlparser"
)

func Test_schema(t *testing.T){
	mConfig,err := config.ParseConfig("D:/workspace/golang/src/github.com/sgoby/myhub/config/conf.xml")
	if err != nil{
		fmt.Println(err)
		return
	}
	rr,err := NewRuleRange(mConfig.Rules[0])
	if err != nil{
		fmt.Println(err)
		return
	}
	//
	sql := "select * from user where id < 0";
	stmt,err := sqlparser.Parse(sql)
	if err != nil{
		fmt.Println("Error:",err)
		return
	}
	if insertStmt,ok := stmt.(*sqlparser.Insert);ok{
		vals := insertStmt.Rows.(sqlparser.Values)
		rRs,err :=  rr.GetShardRule(vals[0][0],"")
		if err != nil{
			fmt.Println("Error:",err)
			return
		}
		fmt.Println("$$$:",rRs)
	}
	if selectStmt,ok := stmt.(*sqlparser.Select);ok{
		rRs,err :=  rr.GetShardRule(selectStmt.Where.Expr,"")
		if err != nil{
			fmt.Println("Error:",err)
			return
		}
		fmt.Println("$$$:",rRs)

	}

}
func Test_dateShard(t *testing.T){
	mShard := new(Shard)
	mShard.config.RangeExpr = "201801-201901"
	err := mShard.parseRangeDate("","YM")
	if err != nil{
		fmt.Println("Error:",err)
		return
	}
	fmt.Println("$$$:",mShard)
}
