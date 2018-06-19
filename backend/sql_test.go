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

package backend

import (
	"testing"
	"fmt"
	"context"
	"github.com/sgoby/myhub/backend/driver"
	"github.com/sgoby/myhub/mysql"
)

func getConnect()(*Client, error) {
	param := &driver.ConnParams{
		Host:   "127.0.0.1",
		Port:   3306,
		Uname:  "root",
		Pass:   "123456",
		DbName: "test",
	}
	ctx := context.Background()
	d := mysql.NewMysqlDriver(ctx)
	return  NewSQL(param, "",d)
}

func Test_Select(t *testing.T) {
	db,err  := getConnect()
	if err != nil {
		fmt.Println(err)
		return
	}
	sql := "SELECT id,dealer FROM dealer_info order by id asc"
	re, err := db.Exec(sql, nil)
	if err != nil {
		fmt.Println("Result error:", err)
		return
	}
	fmt.Println(re)
}
func Test_Tx(t *testing.T) {
	db,err  := getConnect()
	if err != nil {
		fmt.Println(err)
		return
	}
	tx,err := db.Begin()
	if err != nil{
		fmt.Println(err)
		return
	}
	cSql := "INSERT INTO  dealer_info (dealer,dealer_name,last_update) VALUES ('sssss','eeee01','2018-04-16 15:47:00')"
	rs,err := tx.Exec(cSql)
	rs,err = tx.Exec(cSql)
	fmt.Println(rs,err)
	//err = tx.Rollback()
	err = tx.Commit()
	fmt.Println("ctx end")
}
