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

package tb

import (
	"strings"
	"fmt"
	"github.com/golang/glog"
)

const (
	SHOW_TABLES      = "tables"
	SHOW_FIELDS      = "fields"
	SHOW_KEYS        = "keys"
	SHOW_CREATE      = "create"
	SHOW_PROCESSLIST = "processlist"
	SHOW_DATABASES   = "databases"
	SHOW_VARIABLES   = "variables"
	SHOW_STATUS      = "status"
	SHOW_PROFILES    = "profiles"
	TOKEN_TABLE      = "table"
	TOKEN_DATABASE   = "database"
)

type Show struct {
	Full    bool
	ExprStr []string
	From    string
	Tokens  []string
}

func ParseShowStmt(query string) *Show {
	pShow := &Show{}
	query = strings.Replace(query, "`", "", -1)
	query = strings.Replace(query, "\n", "", -1)
	query = strings.ToLower(query)
	pShow.Tokens = strings.Split(query, " ")
	//
	var findFrom bool;
	for _, token := range pShow.Tokens {
		switch token {
		case "full":
			pShow.Full = true
		case SHOW_TABLES, SHOW_FIELDS, SHOW_KEYS, SHOW_CREATE, SHOW_PROCESSLIST, SHOW_DATABASES,
			SHOW_VARIABLES, SHOW_STATUS, SHOW_PROFILES, TOKEN_TABLE, TOKEN_DATABASE:
			pShow.ExprStr = append(pShow.ExprStr, token)
		case "from":
			findFrom = true;
		default:
			if findFrom && len(pShow.From) < 1 {
				pShow.From = token
			}
		}
	}
	return pShow;
}
func (this *Show) ExprIsEmpty() bool {
	if len(this.ExprStr) < 1 {
		return true
	}
	return false
}
func (this *Show) String() string {
	fullStr := ""
	if this.Full {
		fullStr = "full"
	}
	query := fmt.Sprintf("show %s %s", fullStr, strings.Join(this.ExprStr," "))
	if len(this.From) > 0 {
		query += fmt.Sprintf(" from %s", this.From)
	}
	return query
}

//
func (this *Show) GetLastToken() string {
	if len(this.Tokens) < 1{
		return ""
	}
	return this.Tokens[len(this.Tokens) - 1]
}
//
func (this *Show) GetFromDataBase() string {
	if len(this.ExprStr) < 1 || len(this.From) < 1 {
		return ""
	}
	arr := strings.Split(this.From, ".")
	return arr[0];
}

//
func (this *Show) GetFromTable() string {
	if len(this.ExprStr) < 1 || len(this.From) < 1 {
		return ""
	}
	arr := strings.Split(this.From, ".")
	if len(arr) > 1 {
		return arr[1]
	}
	return arr[0];
}
