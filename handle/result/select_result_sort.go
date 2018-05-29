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
	"bytes"
	"github.com/sgoby/sqlparser"
	"strconv"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"regexp"
)

func (this *SelectResult) RowsNum() int {
	return len(this.tempRows)
}

//Len方法返回集合中的元素个数
func (this *SelectResult) Len() int {
	return this.RowsNum()
}

// Less方法报告索引i的元素是否比索引j的元素小
func (this *SelectResult) Less(i, j int) bool {
	rows := this.tempRows
	reg,err := regexp.Compile("^[1-9][0-9]*$")
	if err != nil{
		return false;
	}
	var valStr string
	var index int;
	for _, order := range this.stmt.OrderBy {
		buf := sqlparser.NewTrackedBuffer(nil)
		order.Expr.Format(buf)
		valStr = buf.String()
		index = this.getFieldIndex(valStr)
		if index < 0 && reg.MatchString(valStr){
			indexN,err := strconv.ParseInt(valStr,10,64)
			if err == nil && int(indexN) <= len(this.tempFields){
				index = int(indexN) - 1
			}
		}
		//
		if index < 0{
			return false;
		}
		//
		s := true
		if CompareValue(rows[i][index], rows[j][index]) > 0 {
			s = false
		}
		if order.Direction == sqlparser.DescScr {
			return !s
		} else {
			return s
		}
	}
	return true;
}

// Swap方法交换索引i和j的两个元素
func (this *SelectResult) Swap(i, j int) {
	tempRow := this.tempRows[i]
	this.tempRows[i] = this.tempRows[j]
	this.tempRows[j] = tempRow
}

//如果v1==v2返回0；如果a<b返回-1；否则返回+1 小于比较
func CompareValue(v1, v2 sqltypes.Value) int {
	if v1.Type() != v2.Type() {
		if v1.IsNull() {
			return -1
		}
		return 1
	}
	if v1.IsIntegral() || v1.IsFloat() || v1.IsSigned() || v1.IsUnsigned() || v1.Type() == querypb.Type_DECIMAL {
		vStr1, err1 := strconv.ParseFloat(string(v1.Raw()), 64)
		if err1 != nil {
			return -1
		}
		vStr2, err2 := strconv.ParseFloat(string(v2.Raw()), 64)
		if err2 != nil {
			return 1
		}
		if vStr1 > vStr2 {
			return 1
		} else if vStr1 < vStr2 {
			return -1
		} else {
			return 0
		}
	} else {
		return bytes.Compare(v1.ToBytes(), v2.ToBytes())
	}
}
