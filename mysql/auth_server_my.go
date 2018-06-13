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
	"net"
	"bytes"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
)

type AuthServerMy struct{
	// Method can be set to:
	// - MysqlNativePassword
	// - MysqlClearPassword
	// - MysqlDialog
	// It defaults to MysqlNativePassword.
	Method string
	// Entries contains the users, passwords and user data.
	Entries map[string][]*AuthServerMyEntry
}
//
type AuthServerMyEntry struct {
	Password    string
	UserData    string
	SourceHosts []string
	Databases   []string
}
//
func NewAuthServerMy() *AuthServerMy{
	return &AuthServerMy{
		Method:  MysqlNativePassword,
		Entries: make(map[string][]*AuthServerMyEntry),
	}
}

// AuthMethod is part of the AuthServer interface.
func (this *AuthServerMy)AuthMethod(user string) (string, error){
	return this.Method, nil
}

// Salt is part of the AuthServer interface.
func (this *AuthServerMy)Salt() ([]byte, error){
	return NewSalt()
}

// ValidateHash is part of the AuthServer interface.
func (this *AuthServerMy)ValidateHash(salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, error){
	// Find the entry.
	entries, ok := this.Entries[user]
	if !ok {
		return &MyUserData{userName:user}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		//fmt.Println("ValidateHash",entry,remoteAddr)
		if this.matchSourceHost(remoteAddr, entry.SourceHosts) && len(entry.Password) < 1{
			return &MyUserData{userName:user,databases:entry.Databases}, nil
		}
		computedAuthResponse := scramblePassword(salt, []byte(entry.Password))
		// Validate the password.
		if this.matchSourceHost(remoteAddr, entry.SourceHosts) && bytes.Compare(authResponse, computedAuthResponse) == 0 {
			return &MyUserData{userName:user,databases:entry.Databases}, nil
		}
	}
	return &MyUserData{userName:user}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v' %v", user,remoteAddr)
}

// Negotiate is part of the AuthServer interface.
// It will be called if Method is anything else than MysqlNativePassword.
// We only recognize MysqlClearPassword and MysqlDialog here.
func (this *AuthServerMy)Negotiate(c *Conn, user string, remoteAddr net.Addr) (Getter, error){

	// Finish the negotiation.
	password, err := AuthServerNegotiateClearOrDialog(c, this.Method)
	if err != nil {
		return nil, err
	}

	// Find the entry.
	entries, ok := this.Entries[user]
	if !ok {
		return &MyUserData{userName:user}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		//fmt.Println("Negotiate",entry)
		// Validate the password.
		if this.matchSourceHost(remoteAddr, entry.SourceHosts) && entry.Password == password {
			return &MyUserData{userName:user,databases:entry.Databases}, nil
		}
	}
	return &MyUserData{userName:user}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
}

func (this *AuthServerMy)matchSourceHost(remoteAddr net.Addr, targetSourceHost []string) bool {
	if targetSourceHost == nil{
		return true
	}
	switch rAddr := remoteAddr.(type) {
	case *net.UnixAddr:
		return true
	case *net.TCPAddr:
		currentIp := rAddr.IP.String()
		if currentIp == "::1"{
			currentIp = "localhost"
		}
		for _,sourceHost := range targetSourceHost{
			if sourceHost == "*" || sourceHost == "%"{
				return true
			}else if sourceHost == currentIp{
				return true
			}else if "127.0.0.1" == currentIp && sourceHost == "localhost"{
				return true
			}
		}
	}
	return false
}
//
// MyUserData holds the username
type MyUserData struct {
	userName string
	databases []string
}
func NewMyUserData(value string) *MyUserData{
	return &MyUserData{
		userName:value,
	}
}
// Get returns the wrapped username
func (sud *MyUserData) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: sud.userName,Groups:sud.databases}
}