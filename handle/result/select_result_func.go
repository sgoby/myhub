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
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"github.com/sgoby/sqlparser/sqltypes"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"regexp"
	"github.com/sgoby/sqlparser"
)

//
//执行函数接口
type execFunc func(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error)

//
var Aggregates = map[string]execFunc{
	FUNC_AVG: execFuncAvg,
	//FUNC_BIT_AND:      ,
	//FUNC_BIT_OR:       ,
	//FUNC_BIT_XOR:      ,
	FUNC_COUNT: execFuncCount,
	FUNC_GROUP_CONCAT: execFuncGroupConcat,
	FUNC_MAX: execFuncMax,
	FUNC_MIN: execFuncMin,
	//FUNC_STD:          ,
	//FUNC_STDDEV_POP:   ,
	//FUNC_STDDEV_SAMP:  ,
	//FUNC_STDDEV:       ,
	FUNC_SUM: execFuncSum,
	//FUNC_VAR_POP:      ,
	//FUNC_VAR_SAMP:     ,
	//FUNC_VARIANCE:     ,
}

const (
	FUNC_AVG          = "avg"
	FUNC_BIT_AND      = "bit_and"
	FUNC_BIT_OR       = "bit_or"
	FUNC_BIT_XOR      = "bit_xor"
	FUNC_COUNT        = "count"
	FUNC_GROUP_CONCAT = "group_concat"
	FUNC_MAX          = "max"
	FUNC_MIN          = "min"
	FUNC_STD          = "std"
	FUNC_STDDEV_POP   = "stddev_pop"
	FUNC_STDDEV_SAMP  = "stddev_samp"
	FUNC_STDDEV       = "stddev"
	FUNC_SUM          = "sum"
	FUNC_VAR_POP      = "var_pop"
	FUNC_VAR_SAMP     = "var_samp"
	FUNC_VARIANCE     = "variance"
)

//
func execFuncGroupConcat(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	groupConcat, funcOk := expr.(*sqlparser.GroupConcatExpr)
	if !funcOk{
		return rows,nil
	}
	separator := ","
	if len(groupConcat.Separator) > 0{
		separator = groupConcat.Separator
	}
	//
	var lastUniqueKey []sqltypes.Value
	var tempRow []sqltypes.Value
	var newRows [][]sqltypes.Value
	var tempStrVal string
	//
	for _, row := range rows {
		tempVal := row[funcIndex]
		val := string(tempVal.Raw())
		if len(val) <= 0{
			continue
		}
		if len(groupFieldIndexs) > 0 {
			uniqueKey := getRowUniqueSlice(row, groupFieldIndexs)
			if equalUniqueSlice(uniqueKey, lastUniqueKey) {
				if len(tempStrVal) > 0 {
					tempStrVal += separator + val
				} else {
					tempStrVal = val
				}
				tempRow[funcIndex] = sqltypes.NewVarChar(tempStrVal)
			} else {
				if len(tempRow) > 0 {
					newRows = append(newRows, tempRow)
				}
				tempStrVal = val
				lastUniqueKey = uniqueKey
				tempRow = row
			}
		}else{
			if tempRow == nil {
				tempRow = row
			}
			if len(tempStrVal) > 0 {
				tempStrVal += separator + val
			} else {
				tempStrVal = val
			}
			tempRow[funcIndex] = sqltypes.NewVarChar(tempStrVal)
		}
	}
	//
	if len(tempRow) > 0 && len(tempRow) > funcIndex {
		newRows = append(newRows, tempRow)
	}
	//
	return newRows, nil
}

//
func execFuncCount(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncComm(rows, groupFieldIndexs, funcIndex, FUNC_COUNT)
}

//
func execFuncSum(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncComm(rows, groupFieldIndexs, funcIndex, FUNC_SUM)
}

//
func execFuncAvg(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncComm(rows, groupFieldIndexs, funcIndex, FUNC_AVG)
}

//
func execFuncComm(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int, funcType string) ([][]sqltypes.Value, error) {
	var lastUniqueKey []sqltypes.Value
	var tempRow []sqltypes.Value
	var newRows [][]sqltypes.Value
	var tempCount float64
	var stepCount int64
	//
	for _, row := range rows {
		//
		tempVal := row[funcIndex]
		float64Val, err := strconv.ParseFloat(string(tempVal.Raw()), 64)
		if err != nil {
			return nil, err
		}
		//
		if len(groupFieldIndexs) > 0 {
			uniqueKey := getRowUniqueSlice(row, groupFieldIndexs)
			if equalUniqueSlice(uniqueKey, lastUniqueKey) {
				if funcType == FUNC_COUNT || funcType == FUNC_SUM || funcType == FUNC_AVG {
					tempCount += float64Val
					stepCount += 1
				}
			} else {
				if len(tempRow) > 0 && len(tempRow) > funcIndex {
					if funcType == FUNC_AVG {
						valFloat64 := float64(tempCount) / float64(stepCount)
						tempRow[funcIndex] = sqltypes.NewFloat64(valFloat64)
					} else {
						if tempRow[funcIndex].IsIntegral() {
							tempRow[funcIndex] = sqltypes.NewInt64(int64(tempCount))
						} else if tempRow[funcIndex].IsFloat() {
							tempRow[funcIndex] = sqltypes.NewFloat64(tempCount)
						} else if tempRow[funcIndex].IsUnsigned() {
							tempRow[funcIndex] = sqltypes.NewUint64(uint64(tempCount))
						} else if tempRow[funcIndex].Type() == querypb.Type_DECIMAL {
							decStr := fmt.Sprintf("%f", tempCount)
							decStr = optNumStr(decStr)
							newV, _ := sqltypes.NewValue(querypb.Type_DECIMAL, []byte(decStr))
							tempRow[funcIndex] = newV
						} else {
							tempRow[funcIndex], _ = sqltypes.InterfaceToValue(tempCount)
						}
					}
					newRows = append(newRows, tempRow)
				}
				//=====================
				tempCount = float64Val
				lastUniqueKey = uniqueKey
				tempRow = row
				stepCount = 1
			}
		} else {
			if tempRow == nil {
				tempRow = row
			}
			if funcType == FUNC_COUNT || funcType == FUNC_SUM || funcType == FUNC_AVG {
				tempCount += float64Val
				stepCount += 1
			}
		}
	}
	//record last row 记录最后一条
	if len(tempRow) > 0 && len(tempRow) > funcIndex {
		if funcType == FUNC_AVG {
			valFloat64 := float64(tempCount) / float64(stepCount)
			tempRow[funcIndex] = sqltypes.NewFloat64(valFloat64)
		} else {
			if tempRow[funcIndex].IsIntegral() {
				tempRow[funcIndex] = sqltypes.NewInt64(int64(tempCount))
			} else if tempRow[funcIndex].IsFloat() {
				tempRow[funcIndex] = sqltypes.NewFloat64(tempCount)
			} else if tempRow[funcIndex].IsUnsigned() {
				tempRow[funcIndex] = sqltypes.NewUint64(uint64(tempCount))
			} else if tempRow[funcIndex].Type() == querypb.Type_DECIMAL {
				decStr := fmt.Sprintf("%f", tempCount)
				decStr = optNumStr(decStr)
				newV, _ := sqltypes.NewValue(querypb.Type_DECIMAL, []byte(decStr))
				tempRow[funcIndex] = newV
			} else {
				tempRow[funcIndex], _ = sqltypes.InterfaceToValue(tempCount)
			}
		}
		newRows = append(newRows, tempRow)
	}
	//
	return newRows, nil
}
//
func optNumStr(val string) string{
	numStrs := strings.Split(val,".")
	if len(numStrs) < 2{
		return val
	}
	reg,err := regexp.Compile("[0]+$")
	if err != nil{
		return val
	}
	numStrs[1] = reg.ReplaceAllString(numStrs[1],"")
	if len(numStrs[1]) < 1{
		return numStrs[0]
	}
	return strings.Join(numStrs,".")
}
//
func execFuncMax(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncMinMaxComm(rows, groupFieldIndexs, funcIndex, FUNC_MAX)
}
func execFuncMin(expr sqlparser.Expr,rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncMinMaxComm(rows, groupFieldIndexs, funcIndex, FUNC_MIN)
}
func execFuncMinMaxComm(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int, funcType string) ([][]sqltypes.Value, error) {
	var lastUniqueKey []sqltypes.Value
	var tempRow []sqltypes.Value
	var newRows [][]sqltypes.Value
	//
	for _, row := range rows {
		if funcIndex >= len(row) {
			return nil, errors.New("execFuncMax: funcIndex is out of row len")
		}
		if isEmptyRowValue(row){
			continue
		}
		//
		if len(groupFieldIndexs) > 0 {
			uniqueKey := getRowUniqueSlice(row, groupFieldIndexs)
			if equalUniqueSlice(uniqueKey, lastUniqueKey) {
				currentVal := row[funcIndex]
				tempVal := tempRow[funcIndex]
				if funcType == FUNC_MAX { //more than 大于
					if CompareValue(currentVal, tempVal) > 0 {
						tempRow = row
					}
				} else if funcType == FUNC_MIN { //less than 小于
					if CompareValue(currentVal, tempVal) < 0 {
						tempRow = row
					}
				}
			} else {
				if len(tempRow) > 0 && len(tempRow) > funcIndex {
					newRows = append(newRows, tempRow)
				}
				//=====================
				lastUniqueKey = uniqueKey
				tempRow = row
			}
		} else {
			//first row 第一行
			if tempRow == nil {
				tempRow = row
				continue
			}
			//
			currentVal := row[funcIndex]
			tempVal := tempRow[funcIndex]
			if funcType == FUNC_MAX { //大于
				if CompareValue(currentVal, tempVal) > 0 {
					tempRow = row
				}
			} else if funcType == FUNC_MIN { //小于
				if CompareValue(currentVal, tempVal) < 0 {
					tempRow = row
				}
			}
		}
	}
	//
	if len(tempRow) > 0 && len(tempRow) > funcIndex {
		newRows = append(newRows, tempRow)
	}
	//
	return newRows, nil
}

//
func isEmptyRowValue(row []sqltypes.Value) bool{
	for _,val := range row{
		if !val.IsNull(){
			return false
		}
	}
	return true
}

//
func getRowUniqueSlice(row []sqltypes.Value, groupFieldIndexs []int) []sqltypes.Value {
	var uniqueKey []sqltypes.Value
	for _, index := range groupFieldIndexs {
		if len(row) > index {
			uniqueKey = append(uniqueKey, row[index])
		}
	}
	return uniqueKey
}
func equalUniqueSlice(row1, row2 []sqltypes.Value) bool {
	if row2 == nil || len(row2) < 1 || len(row1) != len(row2) {
		return false
	}
	//
	for i, val := range row1 {
		if bytes.Compare(val.Raw(), row2[i].Raw()) != 0 {
			return false
		}
	}
	return true
}
