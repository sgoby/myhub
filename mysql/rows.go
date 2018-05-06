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
