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

package client

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/sqlparser"
	"github.com/sgoby/myhub/mysql"
	querypb "github.com/sgoby/sqlparser/vt/proto/query"
	"errors"
	"strconv"
	"time"
	"strings"
)

//执行函数接口
type execFunc func(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error)

const (
	FUNC_AVG            = "avg"
	FUNC_BIT_AND        = "bit_and"
	FUNC_BIT_OR         = "bit_or"
	FUNC_BIT_XOR        = "bit_xor"
	FUNC_COUNT          = "count"
	FUNC_GROUP_CONCAT   = "group_concat"
	FUNC_MAX            = "max"
	FUNC_MIN            = "min"
	FUNC_STD            = "std"
	FUNC_STDDEV_POP     = "stddev_pop"
	FUNC_STDDEV_SAMP    = "stddev_samp"
	FUNC_STDDEV         = "stddev"
	FUNC_SUM            = "sum"
	FUNC_VAR_POP        = "var_pop"
	FUNC_VAR_SAMP       = "var_samp"
	FUNC_VARIANCE       = "variance"
	FUNC_VERSION        = "version"
	FUNC_LAST_INSERT_ID = "last_insert_id"
	FUNC_DATABASE       = "database"
	FUNC_NOW            = "now"
	FUNC_CURDATE        = "curdate"
	FUNC_CURTIME        = "curtime"
)

func (this *Connector) queryNoFromSelect(pStmt *sqlparser.Select, query string) (rs sqltypes.Result, err error, ok bool) {
	rows := mysql.NewRows()
	var vals []interface{}
	for _, expr := range pStmt.SelectExprs {
		t, val, fieldName, err := this.querySelectExpr(expr)
		if err != nil {
			return sqltypes.Result{}, err, true
		}
		rows.AddField(fieldName, t)
		vals = append(vals, val)
	}
	rows.AddRow(vals...)
	return *(rows.ToResult()), nil, true
}
func (this *Connector) querySelectExpr(expr sqlparser.SelectExpr) (pType querypb.Type, val interface{}, fieldName string, err error) {
	switch nExpr := expr.(type) {
	case *sqlparser.StarExpr:
		return querypb.Type_NULL_TYPE, nil, "", errors.New("No tables used")
	case *sqlparser.AliasedExpr:
		buf := sqlparser.NewTrackedBuffer(nil)
		nExpr.Expr.Format(buf)
		fieldName = buf.String()
		fieldName = strings.Trim(fieldName, "'")
		fieldName = strings.Trim(fieldName, "\"")
		fieldName = strings.Trim(fieldName, "`")
		//
		if !nExpr.As.IsEmpty() {
			fieldName = nExpr.As.String()
		}
		t, val, err := this.queryExpr(nExpr.Expr)
		return t, val, fieldName, err
	case sqlparser.Nextval:
	}
	return querypb.Type_NULL_TYPE, nil, "", errors.New("No tables used")
}
func (this *Connector) queryExpr(pExpr sqlparser.Expr) (pType querypb.Type, val interface{}, err error) {
	switch expr := pExpr.(type) {
	case *sqlparser.AndExpr:
		glog.Info(expr, "AndExpr")
	case *sqlparser.OrExpr:
		glog.Info(expr, "OrExpr")
	case *sqlparser.NotExpr:
		glog.Info(expr, "NotExpr")
	case *sqlparser.ParenExpr:
		glog.Info(expr, "ParenExpr")
	case *sqlparser.ComparisonExpr:
		glog.Info(expr, "ComparisonExpr")
	case *sqlparser.RangeCond:
		glog.Info(expr.Left, "RangeCond")
	case *sqlparser.IsExpr:
		glog.Info(expr, "IsExpr")
	case *sqlparser.ExistsExpr:
		glog.Info(expr, "ExistsExpr")
	case *sqlparser.SQLVal:
		glog.Info(expr, "SQLVal")
		return querypb.Type_VARCHAR, string(expr.Val), nil
	case *sqlparser.NullVal:
		glog.Info(expr, "NullVal")
	case sqlparser.BoolVal:
		glog.Info(expr, "BoolVal")
	case *sqlparser.ColName:
		glog.Info(expr, "ColName")
		colName := expr.Name.String()
		if strings.Index(colName,"max_allowed_packet") >= 0{
			return 	querypb.Type_INT64,4194304,nil
		}
		if strings.Index(colName,"version_comment") >= 0{
			return 	querypb.Type_VARCHAR,"MyHub Server (Apache)",nil
		}
		return querypb.Type_NULL_TYPE, nil, fmt.Errorf("Unknown column '%s' in 'field list'", expr.Name.String())
	case sqlparser.ValTuple:
		glog.Info(expr, "ValTuple")
	case *sqlparser.Subquery:
		glog.Info(expr, "Subquery")
	case sqlparser.ListArg:
		glog.Info(expr, "ListArg")
	case *sqlparser.BinaryExpr:
		glog.Info(expr, "BinaryExpr")
	case *sqlparser.UnaryExpr:
		glog.Info(expr, "UnaryExpr")
	case *sqlparser.IntervalExpr:
		glog.Info(expr, "IntervalExpr")
	case *sqlparser.CollateExpr:
		glog.Info(expr, "CollateExpr")
	case *sqlparser.FuncExpr:
		glog.Info(expr, "FuncExpr")
		if this.execFuncMap == nil{
			this.execFuncMap = map[string]execFunc{
				FUNC_VERSION:        this.funcVersion,
				FUNC_LAST_INSERT_ID: this.funcLastInsertId,
				FUNC_MAX:            this.funcMinMaxAvg,
				FUNC_MIN:            this.funcMinMaxAvg,
				FUNC_AVG:            this.funcMinMaxAvg,
				FUNC_NOW:            this.funcNow,
				FUNC_CURDATE:        this.funcCurDate,
				FUNC_CURTIME:        this.funcCurTime,
				FUNC_DATABASE:       this.funcDatabase,
			}
		}
		if f, ok := this.execFuncMap[expr.Name.Lowered()]; ok {
			return f(expr)
		}
	case *sqlparser.CaseExpr:
		glog.Info(expr, "CaseExpr")
	case *sqlparser.ValuesFuncExpr:
		glog.Info(expr, "ValuesFuncExpr")
	case *sqlparser.ConvertExpr:
		glog.Info(expr, "ConvertExpr")
	case *sqlparser.ConvertUsingExpr:
		glog.Info(expr, "ConvertUsingExpr")
	case *sqlparser.MatchExpr:
		glog.Info(expr, "MatchExpr")
	case *sqlparser.GroupConcatExpr:
		glog.Info(expr, "GroupConcatExpr")
	case *sqlparser.Default:
	default:
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	pExpr.Format(buf)
	return querypb.Type_VARCHAR, "", fmt.Errorf("No support expr: %s", buf.String())
}

//==================
func (this *Connector) funcMinMaxAvg(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	if len(funExpr.Exprs) < 1 {
		return querypb.Type_FLOAT64, nil, fmt.Errorf("You have an error in your SQL syntax")
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	funExpr.Exprs.Format(buf)
	num, err := strconv.ParseFloat(buf.String(), 64)
	if err != nil {
		return querypb.Type_FLOAT64, nil, err
	}
	return querypb.Type_FLOAT64, num, nil
}
func (this *Connector) funcNow(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	return querypb.Type_VARCHAR, time.Now().Format("2006-01-02 15:04:05"), nil
}
func (this *Connector) funcCurDate(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	return querypb.Type_VARCHAR, time.Now().Format("2006-01-02"), nil
}
func (this *Connector) funcCurTime(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	return querypb.Type_VARCHAR, time.Now().Format("15:04:05"), nil
}
func (this *Connector) funcDatabase(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	dbName := this.GetDB()
	if len(dbName) < 1{
		return querypb.Type_NULL_TYPE, nil, nil
	}
	return querypb.Type_VARCHAR, this.GetDB(), nil
}
func (this *Connector) funcVersion(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	return querypb.Type_VARCHAR, mysql.DefaultServerVersion, nil
}
func (this *Connector) funcLastInsertId(funExpr *sqlparser.FuncExpr) (pType querypb.Type, val interface{}, err error) {
	return querypb.Type_INT64, this.lastInsertId, nil
}
