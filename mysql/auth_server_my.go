package mysql

import (
	"net"
	"bytes"
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
		return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		computedAuthResponse := scramblePassword(salt, []byte(entry.Password))
		// Validate the password.
		//fmt.Println(remoteAddr,entry.SourceHost)
		if this.matchSourceHost(remoteAddr, entry.SourceHosts) && bytes.Compare(authResponse, computedAuthResponse) == 0 {
			return &StaticUserData{entry.UserData}, nil
		}
	}
	return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)

	return nil,nil
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
		return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
	}
	for _, entry := range entries {
		// Validate the password.
		if this.matchSourceHost(remoteAddr, entry.SourceHosts) && entry.Password == password {
			return &StaticUserData{entry.UserData}, nil
		}
	}
	return &StaticUserData{""}, NewSQLError(ERAccessDeniedError, SSAccessDeniedError, "Access denied for user '%v'", user)
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
		for _,sourceHost := range targetSourceHost{
			if sourceHost == "*" || sourceHost == "%"{
				return true
			}else if sourceHost == currentIp{
				return true
			}
		}
	}
	return false
}