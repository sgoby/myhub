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
)


const(
	SHOW_TABLES = "tables"
	SHOW_FIELDS = "fields"
	SHOW_KEYS = "keys"
	SHOW_CREATE = "create"
	SHOW_PROCESSLIST = "processlist"
	SHOW_DATABASES = "databases"
)

type Show struct {
	Full bool
	ExprStr string
	From string
}

func ParseShowStmt(query string) *Show{
	pShow := &Show{}
	query = strings.Replace(query,"`","",-1)
	query = strings.Replace(query,"\n","",-1)
	query = strings.ToLower(query)
	tokens := strings.Split(query," ")
	//
	var findFrom bool;
	for _, token := range tokens{
		switch token {
		case "full":
			pShow.Full = true
		case SHOW_TABLES,SHOW_FIELDS,SHOW_KEYS,SHOW_CREATE,SHOW_PROCESSLIST,SHOW_DATABASES:
			pShow.ExprStr = token
		case "from":
			findFrom = true;
		default:
			if findFrom && len(pShow.From) < 1{
				pShow.From = token
			}
		}
	}
	return pShow;
}
func (this *Show) String() string {
	fullStr := ""
	if this.Full{
		fullStr = "full"
	}
	query := fmt.Sprintf("show %s %s",fullStr,this.ExprStr)
	if len(this.From) > 0{
		query += fmt.Sprintf(" from %s",this.From)
	}
	return query
}


//
func (this *Show) IsShowDatabases() bool {
	if this.ExprStr == SHOW_DATABASES{
		return true;
	}
	return false;
}

//
func (this *Show) IsShowProcesslist() bool {
	if this.ExprStr == SHOW_PROCESSLIST{
		return true;
	}
	return false;
}
//
func (this *Show) IsShowTables() bool {
	if this.ExprStr == SHOW_TABLES{
		return true;
	}
	return false;
}
func (this *Show) IsShowFields() bool {
	if this.ExprStr == SHOW_FIELDS{
		return true;
	}
	return false;
}
func (this *Show) IsShowKeys() bool {
	if this.ExprStr == SHOW_KEYS{
		return true;
	}
	return false;
}
//
func (this *Show) GetFromDataBase() string {
	if this.IsShowTables(){
		return this.From;
	}
	arr := strings.Split(this.From,".")
	return arr[0];
}
//
func (this *Show) GetFromTable() string {
	if this.IsShowTables(){
		return "";
	}
	arr := strings.Split(this.From,".")
	if len(arr) > 1{
		return arr[1]
	}
	return arr[0];
}