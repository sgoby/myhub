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
ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error
*/
func (this *ServerHandler) NewConnection(c *mysql.Conn) {
	this.addConnector(c)
}
func (this *ServerHandler) ConnectionClosed(c *mysql.Conn) {
	this.delConnector(c)
}
func (this *ServerHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	glog.Info(query)
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
	mConnector := this.getConnector(c)
	if mConnector == nil {
		return errors.New("not connect!")
	}
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
	if rs, isVersion := this.comShow(stmt); isVersion {
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
func (this *ServerHandler) comShow(stmt sqlparser.Statement) (*sqltypes.Result, bool) {
	showStmt, ok := stmt.(*sqlparser.Show)
	if !ok {
		return nil, false
	}
	//fmt.Println(showStmt)
	if showStmt.Type == "variables" {
		rows := mysql.NewRows()
		rows.AddField("Variables_name", querypb.Type_VARCHAR)
		rows.AddField("Value", querypb.Type_INT64)
		rows.AddRow("lower_case_table_names", 1)
		return rows.ToResult(), true
	}
	//STATUS
	if showStmt.Type == "status" {
		rows := mysql.NewRows()
		rows.AddField("Variables_name", querypb.Type_VARCHAR)
		rows.AddField("Value", querypb.Type_INT64)
		return rows.ToResult(), true
	}
	//PROFILES //Query_ID,Duration,Query PROFILES
	if strings.ToUpper(showStmt.Type) == "PROFILES" {
		rows := mysql.NewRows()
		rows.AddField("Query_ID", querypb.Type_INT64)
		rows.AddField("Duration", querypb.Type_FLOAT64)
		rows.AddField("Query", querypb.Type_VARCHAR)
		return rows.ToResult(), true
	}
	//
	return nil, false
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
