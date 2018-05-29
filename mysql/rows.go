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
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"github.com/sgoby/sqlparser/sqltypes"
	"fmt"
)

type Rows struct {
	fields []*querypb.Field
	rows   [][]sqltypes.Value
}

//
func NewRows() *Rows {
	return &Rows{
	}
}
//
func (this *Rows) ToResult() *sqltypes.Result{
	return &sqltypes.Result{
		Fields:this.fields,
		Rows:this.rows,
		RowsAffected:uint64(len(this.rows)),
	}
}
//
func (this *Rows) AddField(name string, fType querypb.Type) {
	this.fields = append(this.fields, &querypb.Field{
		Name: name,
		Type: fType})
}

//
func (this *Rows) AddRow(vals ...interface{}) error {
	var row []sqltypes.Value
	for i, val := range vals {
		if i >= len(this.fields) {
			continue
		}
		t := this.fields[i].Type
		v:= sqltypes.MakeTrusted(t, []byte(fmt.Sprint(val)))
		row = append(row, v)
	}
	this.rows = append(this.rows, row)
	return nil
}
