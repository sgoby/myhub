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

package opt

import (
	"github.com/sgoby/sqlparser"
	"fmt"
)


type optSelect struct {
	stmt        *sqlparser.Select
}

//optimize select query sql.
func OptimizeSelectSql(sql string)(nSql string,nErr error){
	stmt,err := sqlparser.Parse(sql)
	if err != nil{
		return sql,err
	}
	selectStmt,ok := stmt.(*sqlparser.Select)
	if !ok{
		return sql,nil
	}
	nStmt := OptimizeSelect(selectStmt)
	return sqlparser.String(nStmt),nil
}

//optimize select statement.
func OptimizeSelectStmtSql(stmt sqlparser.Statement)(nSql string,isSelect bool,nErr error){
	if stmt == nil{
		return "",false,fmt.Errorf("not select statement")
	}
	selectStmt,ok := stmt.(*sqlparser.Select)
	if !ok{
		return sqlparser.String(stmt),false,fmt.Errorf("not select statement")
	}
	nStmt := OptimizeSelect(selectStmt)
	return sqlparser.String(nStmt),true,nil
}

//optimize select statement.
func OptimizeSelect(stmt *sqlparser.Select)(nStmt *sqlparser.Select){
	mOptSelect := optSelect{
		stmt: stmt,
	}
	mOptSelect.optimizeSelectGroup()
	return mOptSelect.stmt
}

//optimize select group.
func (this *optSelect) optimizeSelectGroup(){
	if len(this.stmt.GroupBy) < 1 {
		return
	}
	for _,group := range this.stmt.GroupBy{
		buf := sqlparser.NewTrackedBuffer(nil)
		group.Format(buf)
		//
		if this.getFieldIndex(buf.String()) < 0{
			//add 'group by' expression
			gSelectExpr := &sqlparser.AliasedExpr{
				As:sqlparser.NewColIdent(""),
				Expr:group,
			}
			this.stmt.SelectExprs = append(this.stmt.SelectExprs,gSelectExpr)
		}
	}
}

//find field index by name
func (this *optSelect) getFieldIndex(name string) int {
	mSelectExprs := this.stmt.SelectExprs
	for i,sExpr := range mSelectExprs{
		_,startOk := sExpr.(*sqlparser.StarExpr)
		if startOk && len(mSelectExprs) == 1{
			return 0
		}
		vExpr,ok := sExpr.(*sqlparser.AliasedExpr)
		if !ok{
			continue
		}
		//
		if ! vExpr.As.IsEmpty(){
			if vExpr.As.String() == name{
				return i
			}
		}
		buf := sqlparser.NewTrackedBuffer(nil)
		vExpr.Expr.Format(buf)
		if buf.String() == name{
			return i
		}
	}
	return -1
}