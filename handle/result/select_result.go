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

package result

import (
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
	"sort"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"strconv"
	"github.com/golang/glog"
)

//
type SelectResult struct {
	stmt        *sqlparser.Select
	resultSlice []sqltypes.Result
	tempRows    [][]sqltypes.Value
	tempFields  []*querypb.Field
}

//
func NewSelectResult(stmt *sqlparser.Select) *SelectResult {
	mSelectResult := &SelectResult{
		stmt:        stmt,
		resultSlice: []sqltypes.Result{},
		tempRows:    [][]sqltypes.Value{},
		tempFields:  []*querypb.Field{},
	}
	//
	return mSelectResult;
}

//添加结果集
func (this *SelectResult) AddResult(rsArr ... sqltypes.Result) {
	for _, rs := range rsArr {
		this.resultSlice = append(this.resultSlice, rs)
		this.tempRows = append(this.tempRows, rs.Rows[0:]...)
		//
		if len(this.tempFields) < 1 {
			this.tempFields = rs.Fields
		} else if len(rs.Fields) > len(this.tempFields) {
			this.tempFields = rs.Fields
		}
	}
}

//获取最终结查
func (this *SelectResult) BuildNewResult() (*sqltypes.Result, error) {
	//fmt.Println(this.tempRows)
	err := this.handleRowsGroupBy()
	if err != nil {
		return nil, err
	}
	//排序
	this.sort()
	//
	this.optTempFieldsRows()
	//
	this.optTempFields()
	//
	var rows [][]sqltypes.Value
	var offset, rowcount int64
	if this.stmt.Limit != nil && len(this.tempRows) > 0{
		if this.stmt.Limit.Offset != nil {
			tbufOffset := sqlparser.NewTrackedBuffer(nil)
			this.stmt.Limit.Offset.Format(tbufOffset)
			offset, err = strconv.ParseInt(tbufOffset.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if this.stmt.Limit.Rowcount != nil {
			tbufRowcount := sqlparser.NewTrackedBuffer(nil)
			this.stmt.Limit.Rowcount.Format(tbufRowcount)
			rowcount, err = strconv.ParseInt(tbufRowcount.String(), 10, 64)
			if err != nil {
				return nil, err
			}
		}
		//
		rows = make([][]sqltypes.Value,rowcount)
		glog.Infof("### limit %d,%d", offset, rowcount)
		if offset < int64(len(this.tempRows)) && rowcount > 0 {
			if offset+rowcount < int64(len(this.tempRows)) {
				copy(rows, this.tempRows[offset:offset+rowcount])
			} else {
				rows = make([][]sqltypes.Value,len(this.tempRows) - int(offset))
				copy(rows, this.tempRows[offset:])
			}
		}
	} else {
		rows = this.tempRows
	}
	//
	newResult := &sqltypes.Result{
		Fields:       this.tempFields,
		Rows:         rows,
		RowsAffected: uint64(len(this.tempRows)),
	}
	return newResult, nil
}
//
func (this *SelectResult) optTempFields() {
	for _, mField := range this.tempFields {
		mField.OrgTable = ""
		mField.Database = ""
		mField.Table = ""
	}
}

//对优化批序的字段进行还原
func (this *SelectResult) optTempFieldsRows() {
	if this.hasStarExpr() {
		return
	}
	//
	exprLen := len(this.stmt.SelectExprs)
	if len(this.tempFields) > exprLen {
		this.tempFields = this.tempFields[0:exprLen]
	}
	for i, row := range this.tempRows {
		if len(row) > exprLen {
			row = row[0:exprLen]
			this.tempRows[i] = row
		}
	}
	//
	for _, field := range this.tempFields {
		field.Table = ""
		field.OrgTable = ""
		field.Database = ""
	}
}

//
func (this *SelectResult) hasStarExpr() bool {
	for _, sExpr := range this.stmt.SelectExprs {
		_, startOk := sExpr.(*sqlparser.StarExpr)
		if startOk {
			return true
		}
	}
	return false;
}

//====================================================
//排序
func (this *SelectResult) sort() {
	if this.stmt.OrderBy == nil {
		return
	}
	//fmt.Println("#### OrderBy")
	sort.Sort(this)
}

//
func (this *SelectResult) getFieldIndex(name string) int {
	if this.tempFields == nil || len(this.tempFields) < 1 {
		return -1
	}
	//======
	for index, field := range this.tempFields {
		if field.Name == name {
			return index
		}
	}
	return -1
}
