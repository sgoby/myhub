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

package client

import (
	"fmt"
	"strings"
	"github.com/golang/glog"
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/myhub/core/node"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
)
//
func (this *Connector) describe(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.OtherRead);!ok{
		return rs,err,false;
	}
	query = strings.Replace(query,"`","",-1)
	query = strings.ToLower(query)
	tokens := strings.Split(query," ")
	if tokens[0] != "describe"{
		return;
	}
	if len(tokens) < 2{
		return rs,fmt.Errorf("Error describe"),true
	}
	//
	arr := strings.Split(tokens[1],".");
	dbName := this.GetDB()
	sTbName := arr[0]
	if len(arr) > 1{
		sTbName = arr[1]
	}
	//
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"),true;
	}
	tb := db.GetTable(sTbName)
	if tb == nil{
		if len(db.GetProxyDbName()) > 0 {
			if len(arr) > 1 {
				arr[0] = db.GetProxyDbName();
			}
			tokens[1] = strings.Join(arr,".")
			query = strings.Join(tokens," ")
			proxyRs,err := this.execProxyPlan(db, nil,query, node.HOST_WRITE)
			return proxyRs,err,true;
		}
		return sqltypes.Result{}, fmt.Errorf("Table '%s' doesn't exist",sTbName),true;
	}
	createStmt := tb.GetCreateStmt();
	if createStmt == nil{
		return sqltypes.Result{}, fmt.Errorf("No create sql on config :'%s'",sTbName),true;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Field",querypb.Type_VARCHAR)
	resultRows.AddField("Type",querypb.Type_VARCHAR)
	resultRows.AddField("Null",querypb.Type_VARCHAR)
	resultRows.AddField("Key",querypb.Type_VARCHAR)
	resultRows.AddField("Default",querypb.Type_VARCHAR)
	resultRows.AddField("Extra",querypb.Type_VARCHAR)
	//
	for _,column := range createStmt.TableSpec.Columns{

		Null := "YES"
		if column.Type.NotNull{
			Null = "NO"
		}
		valDefault := ""
		if column.Type.Default != nil {
			bufDefault := sqlparser.NewTrackedBuffer(nil)
			column.Type.Default.Format(bufDefault)
			valDefault = bufDefault.String()
		}
		Extra := ""
		if column.Type.Autoincrement{
			Extra = "auto_increment"
		}
		Key := fmt.Sprintf("%d",column.Type.KeyOpt)
		if column.Type.KeyOpt == 1 {
			Key = "PRI"
		}
		//
		mType := column.Type.Type
		if column.Type.Length != nil{
			lenBuf := sqlparser.NewTrackedBuffer(nil)
			column.Type.Length.Format(lenBuf)
			mType += fmt.Sprintf("(%s)",lenBuf.String())
			glog.Info(mType)
		}
		resultRows.AddRow(column.Name.String(),mType,Null,
			Key,valDefault,Extra)
	}
	//
	rs = *resultRows.ToResult()
	return rs,nil,true;
}
//
func (this *Connector) explain(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.OtherRead);!ok{
		return rs,err,false;
	}
	query = strings.Replace(query,"`","",-1)
	query = strings.ToLower(query)
	tokens := strings.Split(query," ")
	if tokens[0] != "explain"{
		return;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("id",querypb.Type_INT64)
	resultRows.AddField("select_type",querypb.Type_VARCHAR)
	resultRows.AddField("table",querypb.Type_VARCHAR)
	resultRows.AddField("partitions",querypb.Type_VARCHAR)
	resultRows.AddField("type",querypb.Type_VARCHAR)
	resultRows.AddField("possible_keys",querypb.Type_VARCHAR)
	resultRows.AddField("key",querypb.Type_VARCHAR)
	resultRows.AddField("key_len",querypb.Type_VARCHAR)
	resultRows.AddField("ref",querypb.Type_VARCHAR)
	resultRows.AddField("rows",querypb.Type_INT64)
	resultRows.AddField("filtered",querypb.Type_FLOAT32)
	resultRows.AddField("Extra",querypb.Type_VARCHAR)
	rs = *resultRows.ToResult()
	return rs,nil,true;
}

