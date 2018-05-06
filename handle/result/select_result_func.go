package result

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sgoby/sqlparser/sqltypes"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
)

//
//执行函数接口
type execFunc func(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error)

//
var Aggregates = map[string]execFunc{
	FUNC_AVG: execFuncAvg,
	//FUNC_BIT_AND:      ,
	//FUNC_BIT_OR:       ,
	//FUNC_BIT_XOR:      ,
	FUNC_COUNT: execFuncCount,
	//FUNC_GROUP_CONCAT: ,
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
func execFuncCount(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncComm(rows, groupFieldIndexs, funcIndex, FUNC_COUNT)
}

//
func execFuncSum(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncComm(rows, groupFieldIndexs, funcIndex, FUNC_SUM)
}

//
func execFuncAvg(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
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
							decStr := fmt.Sprintf("%.2f", tempCount)
							decStr = strings.Replace(decStr, ".00", "", -1)
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
	//记录最后一条
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
				decStr := fmt.Sprintf("%.2f", tempCount)
				decStr = strings.Replace(decStr, ".00", "", -1)
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
func execFuncMax(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
	return execFuncMinMaxComm(rows, groupFieldIndexs, funcIndex, FUNC_MAX)
}
func execFuncMin(rows [][]sqltypes.Value, groupFieldIndexs []int, funcIndex int) ([][]sqltypes.Value, error) {
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
		//
		if len(groupFieldIndexs) > 0 {
			uniqueKey := getRowUniqueSlice(row, groupFieldIndexs)
			if equalUniqueSlice(uniqueKey, lastUniqueKey) {
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
			} else {
				if len(tempRow) > 0 && len(tempRow) > funcIndex {
					newRows = append(newRows, tempRow)
				}
				//=====================
				lastUniqueKey = uniqueKey
				tempRow = row
			}
		} else {
			//第一行
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
