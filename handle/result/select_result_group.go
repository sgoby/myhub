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
	"github.com/golang/glog"
	"fmt"
)

//
type tempStruct struct {
	slice [][]sqltypes.Value
}

type funcExprObj struct {
	expr sqlparser.Expr
	funcation  execFunc
}

func (this *tempStruct) add(row []sqltypes.Value) {
	this.slice = append(this.slice, row)
}

//group sort
func sortGroupRows(rows [][]sqltypes.Value, fieldIndexs []int) (newRows [][]sqltypes.Value) {
	if len(fieldIndexs) < 1 || len(rows) < 1 {
		return rows
	}
	//
	type valuesRows [][]sqltypes.Value
	rowsMap := make(map[interface{}]valuesRows)
	fieldIndex := fieldIndexs[0]
	//
	for _, row := range rows {
		index := string(row[fieldIndex].Raw())
		mRows, ok := rowsMap[index]
		if ok {
			mRows = append(mRows, row)
		} else {
			mRows = valuesRows{row}
		}
		rowsMap[index] = mRows
	}
	//
	oldLen := len(fieldIndexs)
	if oldLen > 1 { //if has more group
		copy(fieldIndexs, fieldIndexs[1:])
		fieldIndexs = fieldIndexs[:oldLen-1]
		//
		for _, rows := range rowsMap {
			pRows := sortGroupRows(rows, fieldIndexs)
			if len(pRows) > 0 {
				newRows = append(newRows, pRows...)
			}
		}
	} else { //is not has more group
		for _, rows := range rowsMap {
			//fmt.Println("###",rows)
			newRows = append(newRows, rows...)
		}
	}
	return
}

//处理group
func (this *SelectResult) handleRowsGroupBy() (err error) {
	fieldIndexs := []int{}
	exprs := this.stmt.GroupBy //sqlparser.SelectExprs
	//
	var valStr string
	for _, expr := range exprs {
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Format(buf)
		valStr = buf.String()
		index := this.getFieldIndex(valStr);
		if index >= 0 {
			fieldIndexs = append(fieldIndexs, index)
		}
	}
	if len(fieldIndexs) > 0 {
		//
		this.tempRows = sortGroupRows(this.tempRows, fieldIndexs)
	}
	//处理函数
	this.tempRows, err = this.handleRowsFuncExpr(fieldIndexs)
	//普通合并
	this.tempRows, err = this.mergeGroupResults(fieldIndexs)
	//
	return
}

//SelectExpr
func (this *SelectResult) handleRowsFuncExpr(groupFieldIndexs []int) (newRows [][]sqltypes.Value, err error) {
	rows := this.tempRows
	newRows = this.tempRows
	if len(rows) < 1 {
		return this.tempRows, nil
	}
	//
	exprs := this.stmt.SelectExprs //sqlparser.SelectExprs
	if len(exprs) < 1 {
		return
	}
	//
	exprMap := make(map[int]funcExprObj)
	for _, expr := range exprs {
		//
		pExpr, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			continue
		}
		//此处，应该通过字段名去取位置序号,因类有*号的可能存在。
		fieldName := ""
		if !pExpr.As.IsEmpty() {
			fieldName = pExpr.As.String()
		} else {
			buf := sqlparser.NewTrackedBuffer(nil)
			pExpr.Format(buf)
			fieldName = buf.String()
		}
		index := this.getFieldIndex(fieldName);
		//pExpr.Expr
		funcExpr, funcOk := pExpr.Expr.(*sqlparser.FuncExpr)
		if funcOk {
			//exprMap[i] = funcExpr
			if f, ok := Aggregates[funcExpr.Name.Lowered()]; ok {
				//加入要执行的slice
				exprMap[index] = funcExprObj{
					expr:funcExpr,
					funcation:f,
				}
			}
		}else{
			if _, funcOk := pExpr.Expr.(*sqlparser.GroupConcatExpr);funcOk{
				if f, ok := Aggregates[FUNC_GROUP_CONCAT]; ok {
					exprMap[index] = funcExprObj{
						expr:funcExpr,
						funcation:f,
					}
				}
			}
		}
	}
	if len(exprMap) < 1{
		return
	}
	var valueRows [][]sqltypes.Value
	for index, mFunExpr := range exprMap {
		valueRowsTemp,err := mFunExpr.funcation(mFunExpr.expr,rows, groupFieldIndexs, index)
		if err != nil {
			return newRows,err
		}
		glog.Info(valueRowsTemp)
		if len(valueRows) < 1{
			valueRows = valueRowsTemp
			continue
		}
		for i:=0;i< len(valueRows);i++{
			if i >= len(valueRowsTemp){
				return newRows,fmt.Errorf("error len rows %d to %d",i,len(valueRowsTemp))
			}
			if index >= len(valueRows[i]) || index >= len(valueRowsTemp[i]){
				return newRows,fmt.Errorf("error len row %d",index)
			}
			valueRows[i][index] = valueRowsTemp[i][index]
		}
	}
	return valueRows,nil
}
//合并
func (this *SelectResult) mergeGroupResults(groupFieldIndexs []int)(newRows [][]sqltypes.Value, err error){
	rows := this.tempRows
	var lastUniqueKey []sqltypes.Value
	var tempRow []sqltypes.Value
	//
	if len(groupFieldIndexs) <= 0 {
		return rows,nil
	}
	//
	for _,row := range rows{
		uniqueKey := getRowUniqueSlice(row, groupFieldIndexs)
		if tempRow == nil {
			tempRow = row
			lastUniqueKey = uniqueKey
			continue
		}else{
			if equalUniqueSlice(uniqueKey, lastUniqueKey) {
				continue
			}else{
				if len(tempRow) > 0 {
					newRows = append(newRows, tempRow)
				}
				//=====================
				lastUniqueKey = uniqueKey
				tempRow = row
			}
		}
	}
	//
	if len(tempRow) > 0  {
		newRows = append(newRows, tempRow)
	}
	return newRows,nil
}