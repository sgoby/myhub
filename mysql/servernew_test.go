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
)


func TestConnectionWithSourceHost_Test(t *testing.T) {
	th := &testHandler{}
	//
	param := &ConnParams{
		Host:   "127.0.0.1",
		Port:   3306,
		Uname:  "root",
		Pass:   "123456",
	}
	//
	db, err := Open(param, "")
	if err != nil {
		fmt.Println(err)
		return
	}
	th.RegisterClient(db)
	//
	fmt.Println("======= start ========")
	authServer := NewAuthServerStatic()
	authServer.Entries["root"] = []*AuthServerStaticEntry{
		{
			Password:   "123456",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}

	//authServer := new(AuthServerNone)
	l, err := NewListener("tcp", ":8520", authServer, th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	l.Accept()
/*
	host, port := getHostPort(t, l.Addr())

	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	_, err = Connect(context.Background(), params)
	// target is localhost, should not work from tcp connection
	if err == nil {
		t.Errorf("Should be able to connect to server but found error: %v", err)
	}*/
}
