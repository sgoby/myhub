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

package server

import (
	"errors"
	"regexp"
	"strings"
	//"sync"

	"github.com/golang/glog"
	hubclient "github.com/sgoby/myhub/core/client"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/sqlparser/sqltypes"
	"time"
	"fmt"
	"strconv"
	"github.com/sgoby/myhub/core"
)

type ServerHandler struct {
	connectorMap *ConnectorMap//map[uint32]*hubclient.Connector
	//mu           *sync.Mutex
}

//
func NewServerHandler() *ServerHandler {
	mServerHandler := new(ServerHandler)
	mServerHandler.connectorMap = NewConnectorMap()//make(map[uint32]*hubclient.Connector)
	//mServerHandler.mu = new(sync.Mutex)
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
	mConnector,ok := conn.(*hubclient.Connector)
	if !ok{
		return errors.New("not connect!")
	}
	mConnector.UpActiveTime()
	//
	glog.Query("Query: ", query)
	if mConnector.IsBlacklistQuery(query){
		return fmt.Errorf("Myhub refused execute: %s",query)
	}
	//
	stmt, err := sqlparser.Parse(query)
	if err != nil {
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
		//kill
		if rs,err, isVersion := this.comKill(query); isVersion || err != nil{
			if err != nil{
				return err
			}
			callback(rs)
			return nil
		}
		return err
	}
	//
	rs, err := mConnector.ComQuery(stmt, query)
	//
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



//NewConnection is implement of IServerHandler interface on conn.go
func (this *ServerHandler) GetConnectorMap() []*hubclient.Connector{
	return this.connectorMap.GetSlice()
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

//get total number of all connector
func (this *ServerHandler) getConnectorCount() int {
	return this.connectorMap.Len()
}

//
func (this *ServerHandler) getConnector(c *mysql.Conn) *hubclient.Connector {
	conn, ok := this.connectorMap.Get(int64(c.ConnectionID))
	if ok {
		return conn
	}
	return nil
}
//
func (this *ServerHandler) getConnectorById(id int64) *hubclient.Connector {
	conn, ok := this.connectorMap.Get(id)
	if ok {
		return conn
	}
	return nil
}

//add a client connector when a new client connected
func (this *ServerHandler) addConnector(c *mysql.Conn) *hubclient.Connector {
	mConnector := hubclient.NewConnector(c)
	mConnector.SetServerHandler(this)
	//
	this.connectorMap.Put(mConnector)
	return mConnector
}

//delete a client connector when client closed.
func (this *ServerHandler) delConnector(c *mysql.Conn) {
	conn := this.connectorMap.Del(int64(c.ConnectionID))
	if conn != nil {
		conn.Close()
	}
}
