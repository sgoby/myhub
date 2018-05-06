package opt

import (
	"github.com/sgoby/sqlparser"
	"fmt"
)


type optSelect struct {
	stmt        *sqlparser.Select
}
//
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
//
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
//优化select 语句
func OptimizeSelect(stmt *sqlparser.Select)(nStmt *sqlparser.Select){
	mOptSelect := optSelect{
		stmt: stmt,
	}
	mOptSelect.optimizeSelectGroup()
	return mOptSelect.stmt
}
//
func (this *optSelect) optimizeSelectGroup(){
	if len(this.stmt.GroupBy) < 1 {
		return
	}
	//
	for _,group := range this.stmt.GroupBy{
		buf := sqlparser.NewTrackedBuffer(nil)
		group.Format(buf)
		//
		if this.getFieldIndex(buf.String()) < 0{
			//添加group by 字段
			gSelectExpr := &sqlparser.AliasedExpr{
				As:sqlparser.NewColIdent(""),
				Expr:group,
			}
			this.stmt.SelectExprs = append(this.stmt.SelectExprs,gSelectExpr)
		}
	}
}
//
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