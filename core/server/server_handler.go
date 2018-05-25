package server

import (
	"errors"
	"regexp"
	"strings"
	"sync"

	"github.com/golang/glog"
	hubclient "github.com/sgoby/myhub/core/client"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/sqlparser/sqltypes"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"github.com/sgoby/myhub/tb"
	"time"
	"fmt"
	"strconv"
	"github.com/sgoby/myhub/core"
)

type ServerHandler struct {
	connectorMap map[uint32]*hubclient.Connector
	mu           *sync.Mutex
}

//
func NewServerHandler() *ServerHandler {
	mServerHandler := new(ServerHandler)
	mServerHandler.connectorMap = make(map[uint32]*hubclient.Connector)
	mServerHandler.mu = new(sync.Mutex)
	return mServerHandler
}

/*
NewConnection(c *Conn)

// ConnectionClosed is called when a connection is closed.
ConnectionClosed(c *Conn)

// ComQuery is called when a connection receives a query.
// Note the contents of the query slice may change after
// the first call to callback. So the Handler should not
// hang on to the byte slice.
ComQuery(conn interface{}, query string, callback func(*sqltypes.Result) error) error
*/

//NewConnection is implement of Handler interface on server.go
func (this *ServerHandler) NewConnection(c *mysql.Conn) interface{} {
	return this.addConnector(c)
}

//ConnectionClosed is implement of Handler interface on server.go
func (this *ServerHandler) ConnectionClosed(c *mysql.Conn) {
	this.delConnector(c)
}

//QueryTimeRecord is implement of Handler interface on server.go
func (this *ServerHandler) QueryTimeRecord(query string, startTime time.Time){
	slowTime := core.App().GetSlowLogTime()
	if slowTime <= 0{
		return
	}
	millisecond := float64(time.Now().Sub(startTime).Nanoseconds()) / float64(1000000)
	if millisecond < float64(slowTime){
		return
	}
	glog.Slow(fmt.Sprintf("%s [use: %.2f]",query,millisecond))
}

//ComQuery is implement of Handler interface on server.go
func (this *ServerHandler) ComQuery(conn interface{}, query string, callback func(*sqltypes.Result) error) error {
	glog.Query("Query: ", query)
	reg, err := regexp.Compile("^\\/\\*.+?\\*\\/$")
	if reg.MatchString(query) {
		callback(&sqltypes.Result{})
		return nil
	}

	//set names 'utf8' collate 'utf8_unicode_ci'
	reg, err = regexp.Compile("^set.*collate")
	if reg.MatchString(query) {
		callback(&sqltypes.Result{})
		return nil
	}
	//
	mConnector,ok := conn.(*hubclient.Connector)
	if !ok{
		return errors.New("not connect 22!")
	}
	mConnector.UpActiveTime()
	//
	if rs,err, isVersion := this.comKill(query); isVersion || err != nil{
		if err != nil{
			return err
		}
		callback(rs)
		return nil
	}
	//
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return err
	}
	//
	if rs, isVersion := this.selectVersion(stmt); isVersion {
		callback(rs)
		return nil
	}
	//
	if rs,err, isVersion := this.comShow(stmt,query); isVersion || err != nil{
		if err != nil{
			return err
		}
		callback(rs)
		return nil
	}
	//
	rs, err := mConnector.ComQuery(stmt, query)
	//日志记录到文件
	defer glog.Flush()
	//
	if err != nil {
		return err
	}
	err = callback(&rs)
	if err != nil {
		return err
	}
	return nil
}
//
func (this *ServerHandler) comKill(query string) (rs *sqltypes.Result,err error,ok bool) {
	query = strings.Replace(query,"`","",-1)
	query = strings.Replace(query,"\n","",-1)
	query = strings.ToLower(query)
	tokens := strings.Split(query," ")
	cmdKill := ""
	cmdKillIdStr := ""
	for _,token := range tokens{
		if len(cmdKill) <= 0 && token == "kill"{
			cmdKill = token
			continue
		}
		if len(cmdKill) > 0 && len(cmdKillIdStr) <= 0{
			cmdKillIdStr = token
			break;
		}
	}
	if len(cmdKill) <= 0{
		return nil,nil,false
	}
	//
	id,err := strconv.ParseInt(cmdKillIdStr,10,64)
	if err != nil{
		return nil,err,true;
	}
	//
	c := this.getConnectorById(id);
	if c == nil{
		return nil,fmt.Errorf("no connection of :%d",id),true;
	}
	err = c.Close()
	return &sqltypes.Result{RowsAffected:1},err,true;
}
//
func (this *ServerHandler) comShow(stmt sqlparser.Statement,query string) (rs *sqltypes.Result,err error,ok bool) {
	showStmt, ok := stmt.(*sqlparser.Show)
	if !ok {
		return nil,nil, false
	}
	//fmt.Println(showStmt)
	if showStmt.Type == "variables" {
		rows := mysql.NewRows()
		rows.AddField("Variables_name", querypb.Type_VARCHAR)
		rows.AddField("Value", querypb.Type_INT64)
		rows.AddRow("lower_case_table_names", 1)
		return rows.ToResult(),nil, true
	}
	//STATUS
	if showStmt.Type == "status" {
		rows := mysql.NewRows()
		rows.AddField("Variables_name", querypb.Type_VARCHAR)
		rows.AddField("Value", querypb.Type_INT64)
		return rows.ToResult(),nil, true
	}
	//PROFILES //Query_ID,Duration,Query PROFILES
	if strings.ToUpper(showStmt.Type) == "PROFILES" {
		rows := mysql.NewRows()
		rows.AddField("Query_ID", querypb.Type_INT64)
		rows.AddField("Duration", querypb.Type_FLOAT64)
		rows.AddField("Query", querypb.Type_VARCHAR)
		return rows.ToResult(),nil, true
	}
	//
	rs,err,ok = this.showProcesslist(stmt,query)
	if err != nil || ok{
		return;
	}
	//
	return nil,nil, false
}
//
func (this *ServerHandler) showProcesslist(pStmt sqlparser.Statement,query string)(rs *sqltypes.Result,err error,ok bool){
	_, ok = pStmt.(*sqlparser.Show)
	if !ok {
		return nil,nil, false
	}
	//
	mShow := tb.ParseShowStmt(query)
	if !mShow.IsShowProcesslist(){
		return rs,err,false;
	}
	resultRows := mysql.NewRows()
	resultRows.AddField("Id",querypb.Type_INT64)
	resultRows.AddField("User",querypb.Type_VARCHAR)
	resultRows.AddField("Host",querypb.Type_VARCHAR)
	resultRows.AddField("db",querypb.Type_VARCHAR)
	resultRows.AddField("Command",querypb.Type_VARCHAR)
	resultRows.AddField("Time",querypb.Type_INT64)
	resultRows.AddField("State",querypb.Type_VARCHAR)
	resultRows.AddField("Info",querypb.Type_VARCHAR)
	//
	for id,c := range this.connectorMap{
		idleTime := time.Now().Unix() - c.GetLastActiveTime().Unix()
		resultRows.AddRow(id,c.GetUser(),c.GetRemoteAddr().String(),c.GetDB(),"Sleep",idleTime,"","")
	}
	//
	rs = resultRows.ToResult()
	return rs,nil,true;
}
//
func (this *ServerHandler) selectVersion(stmt sqlparser.Statement) (*sqltypes.Result, bool) {
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, false
	}
	if len(selectStmt.SelectExprs) > 0 {
		if aliaExpr, ok := selectStmt.SelectExprs[0].(*sqlparser.AliasedExpr); ok {
			buf := sqlparser.NewTrackedBuffer(nil)
			aliaExpr.Expr.Format(buf)
			if buf.String() == "version()" {
				rows := mysql.NewRows()
				rows.AddField("version()", querypb.Type_VARCHAR)
				rows.AddRow("1.0.0 - MyHub")
				return rows.ToResult(), true
			}
		}
	}
	//
	return nil, false
}

//获取当前总连接数
func (this *ServerHandler) getConnectorCount() int {
	return len(this.connectorMap)
}

//
func (this *ServerHandler) getConnector(c *mysql.Conn) *hubclient.Connector {
	this.mu.Lock()
	conn, ok := this.connectorMap[c.ConnectionID]
	this.mu.Unlock()
	if ok {
		return conn
	}
	return nil
}
//
func (this *ServerHandler) getConnectorById(id int64) *hubclient.Connector {
	this.mu.Lock()
	conn, ok := this.connectorMap[uint32(id)]
	this.mu.Unlock()
	if ok {
		return conn
	}
	return nil
}
//
func (this *ServerHandler) addConnector(c *mysql.Conn) *hubclient.Connector {
	mConnector := hubclient.NewConnector(c)
	//
	this.mu.Lock()
	conn, ok := this.connectorMap[c.ConnectionID]
	this.connectorMap[c.ConnectionID] = mConnector
	this.mu.Unlock()
	if ok && conn != nil {
		conn.Close()
	}
	//
	return mConnector
}

//
func (this *ServerHandler) delConnector(c *mysql.Conn) {
	//
	this.mu.Lock()
	conn, ok := this.connectorMap[c.ConnectionID]
	delete(this.connectorMap, c.ConnectionID)
	this.mu.Unlock()
	if ok && conn != nil {
		conn.Close()
	}
}
