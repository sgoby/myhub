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

package mysql

import (
	"testing"
	"fmt"
	hresult "github.com/sgoby/myhub/handle/result"
	"github.com/sgoby/myhub/handle/opt"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/tb"
	"context"
)

func TestConnection(t *testing.T) {

	param := &ConnParams{
		Host:   "127.0.0.1",
		Port:   3306,
		Uname:  "root",
		Pass:   "123456",
		DbName: "test",
	}
	//
	db, err := Open(param, "")
	//db.SetMaxIdleConns(5)
	//
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.Ping()
	if err != nil{
		fmt.Println(err)
		return
	}
	/*
	ctx,err := db.Begin()
	if err != nil{
		fmt.Println(err)
		return
	}
	cSql := "INSERT INTO  dealer_info_0007 (dealer,dealer_name,last_update) VALUES ('sssss','eeee','2018-04-16 15:47:00')"

	rs,err := ctx.Exec(cSql)
	rs,err = ctx.Exec(cSql)
	fmt.Println(rs,err)
	err = ctx.Rollback()
	if err == nil{
		fmt.Println("ctx end")
		return;
	}
	*/
	//db.UseDB("test")
	sql := "SELECT id,dealer FROM dealer_info order by id asc"
	stmt,err := sqlparser.Parse(sql)
	if err != nil{
		fmt.Println(err)
		return
	}
	selectRs := hresult.NewSelectResult(stmt.(*sqlparser.Select))
	//
	sql = "SELECT id,dealer FROM dealer_info_201609"
	nSql,err := opt.OptimizeSelectSql(sql)
	if err != nil{
		fmt.Println("Error:",err)
		return
	}
	re, err := db.Exec(nSql, nil)
	if err != nil {
		fmt.Println("Result error:", err)
		return
	}

	fmt.Println(re)
	selectRs.AddResult(re)
	//
	//<- time.After(time.Second * 10)
	err = db.Ping()
	if err != nil{
		fmt.Println(err)
		return
	}
	//
	sql = "SELECT id,dealer  FROM dealer_info_201610"
	nSql,err = opt.OptimizeSelectSql(sql)
	if err != nil{
		fmt.Println("Error:",err,tb.Stack(10))
		return
	}
	ctx := context.Background()
	re, err = db.ExecContext(ctx,nSql, )
	if err != nil {
		fmt.Println("Result error:", err)
		return
	}
	fmt.Println(re)
	selectRs.AddResult(re)
	reN,errN := selectRs.BuildNewResult()
	if errN != nil {
		fmt.Println("Result error:", errN)
		return
	}
	fmt.Println(*reN)
	/*
	re, err = db.Exec("INSERT INTO dealer_info_201609(dealer,dealer_name,last_update) VALUES ('sanngj','test','2018-04-12 14:00:00')", nil)
	if err != nil {
		fmt.Println("Result error:", err)
		return
	}
	fmt.Println(re)
	*/
	//ch := make(chan int,1)
	//<- ch
}
