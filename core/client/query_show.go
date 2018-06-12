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
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/golang/glog"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/tb"
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/myhub/core/node"
	"time"
)

//
func (this *Connector) showCreate(pStmt *sqlparser.Show, query string, mShow *tb.Show) (rs sqltypes.Result, err error, ok bool) {
	if len(mShow.ExprStr) < 2 {
		return rs, nil, true;
	}
	createType := mShow.ExprStr[1]
	if createType == tb.TOKEN_TABLE {
		//
		lastToken := mShow.GetLastToken()
		tbName := lastToken
		dbName := this.GetDB()
		arr := strings.Split(lastToken, ".")
		if len(arr) > 1 {
			tbName = arr[1];
			dbName = arr[0];
		}
		if len(dbName) < 1 {
			dbName = this.GetDB()
		}
		if len(tbName) < 1 {
			glog.Info(mShow.From)
			return rs, fmt.Errorf("You have an error in your SQL syntax;"), true;
		}
		//
		resultRows := mysql.NewRows()
		resultRows.AddField("Table", querypb.Type_VARCHAR)
		resultRows.AddField("Create Table", querypb.Type_VARCHAR)
		db, err := core.App().GetSchema().GetDataBase(dbName)
		if err != nil {
			return rs, err, true;
		}
		tb := db.GetTable(tbName)
		if tb == nil{
			proxyDbName := db.GetProxyDbName()
			if len(proxyDbName) > 0 {
				query := fmt.Sprintf("show create table `%s`.`%s`",proxyDbName,tbName)
				proxyRs, err := this.execProxyPlan(db, nil, query, node.HOST_WRITE)
				return proxyRs, err,true
			}
		}
		resultRows.AddRow(tbName, tb.GetCreateSql())
		return *(resultRows.ToResult()), err, true;
	}
	//
	if createType == tb.TOKEN_DATABASE {
		dbName := mShow.GetLastToken()
		if len(dbName) < 1 {
			return rs, fmt.Errorf("You have an error in your SQL syntax;"), true;
		}
		resultRows := mysql.NewRows()
		resultRows.AddField("Database", querypb.Type_VARCHAR)
		resultRows.AddField("Create Database", querypb.Type_VARCHAR)
		resultRows.AddRow(dbName, fmt.Sprintf("create database `%s`", dbName))
		return *(resultRows.ToResult()), err, true;
	}
	return rs, nil, true;
}

//show a list of all client connector when execute sql: 'show processlist'
func (this *Connector) showProcesslist(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	resultRows := mysql.NewRows()
	resultRows.AddField("Id", querypb.Type_INT64)
	resultRows.AddField("User", querypb.Type_VARCHAR)
	resultRows.AddField("Host", querypb.Type_VARCHAR)
	resultRows.AddField("db", querypb.Type_VARCHAR)
	resultRows.AddField("Command", querypb.Type_VARCHAR)
	resultRows.AddField("Time", querypb.Type_INT64)
	resultRows.AddField("State", querypb.Type_VARCHAR)
	resultRows.AddField("Info", querypb.Type_VARCHAR)
	//
	if this.serverHandler == nil {
		return rs, nil, true;
	}
	for _, c := range this.serverHandler.GetConnectorMap() {
		idleTime := time.Now().Unix() - c.GetLastActiveTime().Unix()
		resultRows.AddRow(c.GetConnectionID(), c.GetUser(), c.GetRemoteAddr().String(), c.GetDB(), "Sleep", idleTime, "", "")
	}
	//
	rs = *(resultRows.ToResult())
	return rs, nil, true;
}

//
func (this *Connector) showProfiles(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	rows := mysql.NewRows()
	rows.AddField("Query_ID", querypb.Type_INT64)
	rows.AddField("Duration", querypb.Type_FLOAT64)
	rows.AddField("Query", querypb.Type_VARCHAR)
	return *(rows.ToResult()), nil, true
}

//
func (this *Connector) showStatus(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	rows := mysql.NewRows()
	rows.AddField("Variables_name", querypb.Type_VARCHAR)
	rows.AddField("Value", querypb.Type_INT64)
	return *(rows.ToResult()), nil, true
}

//
func (this *Connector) showVariables(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	rows := mysql.NewRows()
	rows.AddField("Variables_name", querypb.Type_VARCHAR)
	rows.AddField("Value", querypb.Type_INT64)
	rows.AddRow("lower_case_table_names", 1)
	return *(rows.ToResult()), nil, true
}

//
func (this *Connector) showKeys(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	mShow := tb.ParseShowStmt(query)
	//
	dbName := this.GetDB()
	sDbName := mShow.GetFromDataBase()
	sTbName := mShow.GetFromTable()
	if sDbName == sTbName {
		sDbName = dbName
	}
	//
	if sDbName != dbName { //Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"), true;
	}
	//
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"), true;
	}
	tb := db.GetTable(sTbName)
	if tb == nil {
		if len(db.GetProxyDbName()) > 0 {
			showFrom := mShow.From
			if len(mShow.From) > 0 {
				fromArr := strings.Split(showFrom, ".")
				fromArr[0] = db.GetProxyDbName()
				mShow.From = strings.Join(fromArr, ".")
				query = mShow.String()
			}
			proxyRs, err := this.execProxyPlan(db, nil, query, node.HOST_WRITE)
			return proxyRs, err, true;
		}
		return sqltypes.Result{}, fmt.Errorf("Table '%s' doesn't exist", sTbName), true;
	}
	createStmt := tb.GetCreateStmt();
	if createStmt == nil {
		return sqltypes.Result{}, fmt.Errorf("No create sql on config :'%s'", sTbName), true;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Table", querypb.Type_VARCHAR)
	resultRows.AddField("Non_unique", querypb.Type_VARCHAR)
	resultRows.AddField("Key_name", querypb.Type_VARCHAR)
	resultRows.AddField("Seq_in_index", querypb.Type_VARCHAR)
	resultRows.AddField("Column_name", querypb.Type_VARCHAR)
	resultRows.AddField("Collation", querypb.Type_VARCHAR)
	resultRows.AddField("Cardinality", querypb.Type_VARCHAR)
	resultRows.AddField("Sub_part", querypb.Type_VARCHAR)
	resultRows.AddField("Packed", querypb.Type_VARCHAR)
	resultRows.AddField("Null", querypb.Type_VARCHAR)
	resultRows.AddField("Index_type", querypb.Type_VARCHAR)
	resultRows.AddField("Comment", querypb.Type_VARCHAR)
	resultRows.AddField("Index_comment", querypb.Type_VARCHAR)
	//
	for _, index := range createStmt.TableSpec.Indexes {
		glog.Infof("%v",index.Columns)
	}
	rs = *resultRows.ToResult()
	return rs, nil, true;
}

//
func (this *Connector) showFields(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	mShow := tb.ParseShowStmt(query)
	//
	dbName := this.GetDB()
	sDbName := mShow.GetFromDataBase()
	sTbName := mShow.GetFromTable()
	if sDbName == sTbName {
		sDbName = dbName
	}
	//
	if sDbName != dbName { //Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"), true;
	}
	//
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"), true;
	}
	tb := db.GetTable(sTbName)
	if tb == nil {
		if len(db.GetProxyDbName()) > 0 {
			showFrom := mShow.From
			if len(mShow.From) > 0 {
				fromArr := strings.Split(showFrom, ".")
				fromArr[0] = db.GetProxyDbName()
				mShow.From = strings.Join(fromArr, ".")
				query = mShow.String()
			}
			// = proxyDbName
			proxyRs, err := this.execProxyPlan(db, nil, query, node.HOST_WRITE)
			return proxyRs, err, true;
		}
		return sqltypes.Result{}, fmt.Errorf("Table '%s' doesn't exist", sTbName), true;
	}
	createStmt := tb.GetCreateStmt();
	if createStmt == nil {
		return sqltypes.Result{}, fmt.Errorf("No create sql on config :'%s'", sTbName), true;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Field", querypb.Type_VARCHAR)
	resultRows.AddField("Type", querypb.Type_VARCHAR)
	resultRows.AddField("Collation", querypb.Type_VARCHAR)
	resultRows.AddField("Null", querypb.Type_VARCHAR)
	resultRows.AddField("Key", querypb.Type_VARCHAR)
	resultRows.AddField("Default", querypb.Type_VARCHAR)
	resultRows.AddField("Extra", querypb.Type_VARCHAR)
	resultRows.AddField("Privileges", querypb.Type_VARCHAR)
	resultRows.AddField("Comment", querypb.Type_VARCHAR)
	//

	for _, column := range createStmt.TableSpec.Columns {
		Null := "YES"
		if column.Type.NotNull {
			Null = "NO"
		}
		valDefault := ""
		if column.Type.Default != nil {
			bufDefault := sqlparser.NewTrackedBuffer(nil)
			column.Type.Default.Format(bufDefault)
			valDefault = bufDefault.String()
		}
		Extra := ""
		if column.Type.Autoincrement {
			Extra = "auto_increment"
		}
		valComment := ""
		if column.Type.Comment != nil {
			bufComment := sqlparser.NewTrackedBuffer(nil)
			column.Type.Comment.Format(bufComment)
			valComment = bufComment.String()
		}
		Key := fmt.Sprintf("%d", column.Type.KeyOpt)
		if column.Type.KeyOpt == 1 {
			Key = "PRI"
		}
		//
		resultRows.AddRow(column.Name.String(), column.Type.Type, column.Type.Collate, Null,
			Key, valDefault, Extra, "select,insert,update,references", valComment)
	}
	//
	rs = *resultRows.ToResult()
	return rs, nil, true;
}

//show databases
func (this *Connector) showDatebases(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	resultRows := mysql.NewRows()
	resultRows.AddField("Database", querypb.Type_VARCHAR)
	//
	dbs := this.MyConn.GetDatabases()
	for _, dbName := range dbs {
		if dbName == "*" && core.App().GetSchema() != nil{
			nemes := core.App().GetSchema().GetDataBaseNames();
			for _,n := range nemes{
				resultRows.AddRow(n)
			}
			break
		}
		resultRows.AddRow(dbName)
	}
	//
	rs = *resultRows.ToResult()
	return rs, nil, true;
}

//show tables
func (this *Connector) showTables(pStmt *sqlparser.Show, query string) (rs sqltypes.Result, err error, ok bool) {
	mShow := tb.ParseShowStmt(query)
	dbName := this.GetDB()
	if len(dbName) <= 0 {
		return sqltypes.Result{}, fmt.Errorf("No database selected"), false;
	}
	if len(mShow.From) < 1 {
		mShow.From = dbName;
	}
	//
	resultRows := mysql.NewRows()
	resultRows.AddField("Tables_in_"+mShow.From, querypb.Type_VARCHAR)
	if mShow.Full {
		resultRows.AddField("Tables_type", querypb.Type_VARCHAR)
	}
	//
	if mShow.From != dbName { //Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"), true;
	}
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"), true;
	}
	//if has proxy node, show tables from proxy first.
	var proxyRs sqltypes.Result
	proxyDbName := db.GetProxyDbName()
	if len(proxyDbName) > 0 {
		mShow.From = proxyDbName
		proxyRs, err = this.execProxyPlan(db, nil, mShow.String(), node.HOST_WRITE)
	}
	//Tables_in_dbName,Tables_type
	tbNames := db.GetTableNames()
	//rows := mysql.NewRows()
	for _, name := range tbNames {
		if mShow.Full {
			resultRows.AddRow(name, "BASE TABLE")
		} else {
			resultRows.AddRow(name)
		}
	}
	pRs := resultRows.ToResult()
	//
	if len(proxyRs.Rows) > 0 {
		proxyRs.Rows = append(proxyRs.Rows, pRs.Rows...)
		return proxyRs, nil, true;
	}
	return *pRs, nil, true;
}
