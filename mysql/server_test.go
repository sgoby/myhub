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
	"io/ioutil"
	"net"
	"os"
	"testing"
	"context"
	"github.com/sgoby/sqlparser/sqltypes"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"fmt"
	"github.com/sgoby/sqlparser"
	"time"
	"github.com/sgoby/myhub/backend/driver"
)

var selectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "id",
			Type: querypb.Type_INT32,
		},
		{
			Name: "name",
			Type: querypb.Type_VARCHAR,
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
	RowsAffected: 2,
}

type testHandler struct {
	lastConn *Conn
	err      error
	client  *Client
}
func (th *testHandler) RegisterClient(pClient *Client){
	th.client  = pClient
}
func (th *testHandler) NewConnection(c *Conn) (interface{}){
	th.lastConn = c
	return nil
}

func (th *testHandler) ConnectionClosed(c *Conn) {
}
// QueryTimeRecord is called after ComQuery, the function
// is recorded the time of ComQuery used, wirte slow log
// if it more than the slow log config's time
func (th *testHandler) QueryTimeRecord(query string, startTime time.Time){

}
func (th *testHandler) ComQuery(conn interface{}, query string, callback func(*sqltypes.Result) error) error{

//func (th *testHandler) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	fmt.Println(query)
	switch query {
	case "select version()":
		re := &sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "version()",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1.0.0 - MyHub")),
				},
			},
			RowsAffected: 1,
		}
		callback(re)
	default:
		_,err := sqlparser.Parse(query)
		if err != nil{
			callback(&sqltypes.Result{})
			fmt.Println(err)
			return nil
		}
		if th.client != nil{
			rs,err := th.client.Exec(query,nil)
			if err != nil{
				return err
			}
			for _,field := range rs.Fields{
				field.Table = ""
				field.OrgTable = ""
				field.Database = ""
			}
			callback(&rs)
		}else{
			callback(&sqltypes.Result{})
		}
	}
	return nil
}

func getHostPort(t *testing.T, a net.Addr) (string, int) {
	// For the host name, we resolve 'localhost' into an address.
	// This works around a few travis issues where IPv6 is not 100% enabled.
	hosts, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("LookupHost(localhost) failed: %v", err)
	}
	host := hosts[0]
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}

func TestConnectionFromListener(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	// Make sure we can create our own net.Listener for use with the mysql
	// listener
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("net.Listener failed: %v", err)
	}

	l, err := NewFromListener(listener, authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &driver.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionWithoutSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &driver.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestConnectionWithSourceHost(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()

	authServer.Entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &driver.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	_, err = Connect(context.Background(), params)
	// target is localhost, should not work from tcp connection
	if err == nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
}

func TestConnectionUnixSocket(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()

	authServer.Entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	unixSocket, err := ioutil.TempFile("", "mysql_vitess_test.sock")
	if err != nil {
		t.Fatalf("Failed to create temp file")
	}
	os.Remove(unixSocket.Name())

	l, err := NewListener("unix", unixSocket.Name(), authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	// Setup the right parameters.
	params := &driver.ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}

	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}
	c.Close()
}

func TestClientFoundRows(t *testing.T) {
	th := &testHandler{}

	authServer := NewAuthServerStatic()
	authServer.Entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	l, err := NewListener("tcp", ":0", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	go l.Accept()

	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &driver.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	// Test without flag.
	c, err := Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	foundRows := th.lastConn.Capabilities & CapabilityClientFoundRows
	if foundRows != 0 {
		t.Errorf("FoundRows flag: %x, second bit must be 0", th.lastConn.Capabilities)
	}
	c.Close()
	if !c.IsClosed() {
		t.Errorf("IsClosed returned true on Close-d connection.")
	}

	// Test with flag.
	params.Flags |= CapabilityClientFoundRows
	c, err = Connect(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	foundRows = th.lastConn.Capabilities & CapabilityClientFoundRows
	if foundRows == 0 {
		t.Errorf("FoundRows flag: %x, second bit must be set", th.lastConn.Capabilities)
	}
	c.Close()
}