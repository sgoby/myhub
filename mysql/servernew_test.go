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
