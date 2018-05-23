package client

import (
	"fmt"
	"strings"
	"github.com/golang/glog"
	"github.com/sgoby/myhub/tb"
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/myhub/core/node"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
)

//
func (this *Connector) selectDababase(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	stmt,ok := pStmt.(*sqlparser.Select)
	if !ok{
		return rs,err,false;
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	stmt.SelectExprs.Format(buf)
	if buf.String() != "database()"{
		return
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("database()",querypb.Type_VARCHAR)
	//
	dbs := this.MyConn.GetDatabases()
	for _,dbName := range dbs{
		resultRows.AddRow(dbName)
	}
	//
	rs = *resultRows.ToResult()
	return rs,nil,true;
}

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
//
func (this *Connector) showKeys(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.Show);!ok{
		return rs,err,false;
	}
	mShow := tb.ParseShowStmt(query)
	if !mShow.IsShowKeys(){
		return rs,err,false;
	}
	//
	dbName := this.GetDB()
	sDbName := mShow.GetFromDataBase()
	sTbName := mShow.GetFromTable()
	if sDbName == sTbName{
		sDbName = dbName
	}
	//
	if sDbName != dbName{//Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"),true;
	}
	//
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"),true;
	}
	tb := db.GetTable(sTbName)
	if tb == nil{
		if len(db.GetProxyDbName()) > 0 {
			showFrom := mShow.From
			if len(mShow.From) > 0 {
				fromArr := strings.Split(showFrom,".")
				fromArr[0] = db.GetProxyDbName()
				mShow.From = strings.Join(fromArr,".")
				query = mShow.String()
			}
			proxyRs,err := this.execProxyPlan(db,nil, query, node.HOST_WRITE)
			return proxyRs,err,true;
		}
		return sqltypes.Result{}, fmt.Errorf("Table '%s' doesn't exist",sTbName),true;
	}
	createStmt := tb.GetCreateStmt();
	if createStmt == nil{
		return sqltypes.Result{}, fmt.Errorf("No create sql on config :'%s'",sTbName),true;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Table",querypb.Type_VARCHAR)
	resultRows.AddField("Non_unique",querypb.Type_VARCHAR)
	resultRows.AddField("Key_name",querypb.Type_VARCHAR)
	resultRows.AddField("Seq_in_index",querypb.Type_VARCHAR)
	resultRows.AddField("Column_name",querypb.Type_VARCHAR)
	resultRows.AddField("Collation",querypb.Type_VARCHAR)
	resultRows.AddField("Cardinality",querypb.Type_VARCHAR)
	resultRows.AddField("Sub_part",querypb.Type_VARCHAR)
	resultRows.AddField("Packed",querypb.Type_VARCHAR)
	resultRows.AddField("Null",querypb.Type_VARCHAR)
	resultRows.AddField("Index_type",querypb.Type_VARCHAR)
	resultRows.AddField("Comment",querypb.Type_VARCHAR)
	resultRows.AddField("Index_comment",querypb.Type_VARCHAR)
	for _,index := range createStmt.TableSpec.Indexes{
		glog.Info(index)
	}
	rs = *resultRows.ToResult()
	return rs,nil,true;
}
//
func (this *Connector) showFields(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.Show);!ok{
		return rs,err,false;
	}
	mShow := tb.ParseShowStmt(query)
	if !mShow.IsShowFields(){
		return rs,err,false;
	}
	//
	dbName := this.GetDB()
	sDbName := mShow.GetFromDataBase()
	sTbName := mShow.GetFromTable()
	if sDbName == sTbName{
		sDbName = dbName
	}
	//
	if sDbName != dbName{//Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"),true;
	}
	//
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"),true;
	}
	tb := db.GetTable(sTbName)
	if tb == nil{
		if len(db.GetProxyDbName()) > 0 {
			showFrom := mShow.From
			if len(mShow.From) > 0 {
				fromArr := strings.Split(showFrom,".")
				fromArr[0] = db.GetProxyDbName()
				mShow.From = strings.Join(fromArr,".")
				query = mShow.String()
			}
			// = proxyDbName
			proxyRs,err := this.execProxyPlan(db,nil, query, node.HOST_WRITE)
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
	resultRows.AddField("Collation",querypb.Type_VARCHAR)
	resultRows.AddField("Null",querypb.Type_VARCHAR)
	resultRows.AddField("Key",querypb.Type_VARCHAR)
	resultRows.AddField("Default",querypb.Type_VARCHAR)
	resultRows.AddField("Extra",querypb.Type_VARCHAR)
	resultRows.AddField("Privileges",querypb.Type_VARCHAR)
	resultRows.AddField("Comment",querypb.Type_VARCHAR)
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
		valComment := ""
		if column.Type.Comment != nil {
			bufComment := sqlparser.NewTrackedBuffer(nil)
			column.Type.Comment.Format(bufComment)
			valComment = bufComment.String()
		}
		Key := fmt.Sprintf("%d",column.Type.KeyOpt)
		if column.Type.KeyOpt == 1 {
			Key = "PRI"
		}
		//
		resultRows.AddRow(column.Name.String(),column.Type.Type,column.Type.Collate,Null,
			Key,valDefault,Extra,"select,insert,update,references",valComment)
	}
	//
	rs = *resultRows.ToResult()
	return rs,nil,true;
}

//show databases
func (this *Connector) showDatebases(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.Show);!ok{
		return rs,err,false;
	}
	mShow := tb.ParseShowStmt(query)
	if !mShow.IsShowDatabases(){
		return rs,err,false;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Database",querypb.Type_VARCHAR)
	//
	dbs := this.MyConn.GetDatabases()
	for _,dbName := range dbs{
		resultRows.AddRow(dbName)
	}
	//
	rs = *resultRows.ToResult()
	return rs,nil,true;
}
//show tables
func (this *Connector) showTables(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool){
	if _,ok := pStmt.(*sqlparser.Show);!ok{
		return rs,err,false;
	}
	mShow := tb.ParseShowStmt(query)
	if !mShow.IsShowTables(){
		return rs,err,false;
	}
	dbName := this.GetDB()
	if len(dbName) <= 0{
		return sqltypes.Result{}, fmt.Errorf("No database selected"),false;
	}
	if len(mShow.From) < 1{
		mShow.From = dbName;
	}
	//
	resultRows := mysql.NewRows()
	resultRows.AddField("Tables_in_"+mShow.From, querypb.Type_VARCHAR)
	if mShow.Full{
		resultRows.AddField("Tables_type", querypb.Type_VARCHAR)
	}
	//
	if mShow.From != dbName{//Denies Authority
		return sqltypes.Result{}, fmt.Errorf("Denies Authority"),true;
	}
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, fmt.Errorf("No database use"),true;
	}
	//if has proxy node, show tables from proxy first.
	var proxyRs sqltypes.Result
	proxyDbName := db.GetProxyDbName()
	if len(proxyDbName) > 0 {
		mShow.From = proxyDbName
		proxyRs,err = this.execProxyPlan(db, nil,mShow.String(), node.HOST_WRITE)
	}
	//Tables_in_dbName,Tables_type
	tbNames := db.GetTableNames()
	//rows := mysql.NewRows()
	for _,name := range tbNames{
		if mShow.Full {
			resultRows.AddRow(name, "BASE TABLE")
		}else{
			resultRows.AddRow(name)
		}
	}
	pRs := resultRows.ToResult()
	//
	if len(proxyRs.Rows) > 0{
		proxyRs.Rows = append(proxyRs.Rows,pRs.Rows...)
		return proxyRs,nil,true;
	}
	return *pRs,nil,true;
}
