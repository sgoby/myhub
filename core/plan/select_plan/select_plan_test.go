package select_plan

import (
	"testing"
	"github.com/sgoby/sqlparser"
	"fmt"
	"github.com/golang/glog"
	"flag"
	"os"
)

func init() {
	//  直接初始化，主要使服务器启动后自己直接加载，并不用命令行执行对应的参数
	//flag.Set("alsologtostderr", "true") // 日志写入文件的同时，输出到stderr
	flag.Set("log_dir", "./logs")        // 日志文件保存目录
	flag.Set("stderrthreshold", "0")                  // 配置V输出的等级。
	flag.Parse()
}
func Test_SelectPlan(t *testing.T) {
	defer glog.Flush()
	fmt.Println(os.Getwd())
	sql := "select * from user b where  id = 52 and   up_time between 52 and  500 " //name = 'dddd' and sex = 'man'
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Println(err)
		return
	}

	if pStmt, ok := stmt.(*sqlparser.Select); ok {
		/*
		builder := NewselectPlanBuilder(*pStmt)
		stmt1 := builder.tableNameAddSuffix(*pStmt, "0001")
		sql := sqlparser.String(&stmt1)
		fmt.Println(sql)
		stmt2 := builder.tableNameAddSuffix(*pStmt, "0002")
		sql2 := sqlparser.String(&stmt2)
		fmt.Println(sql, sql2)
		*/

		builder := NewselectPlanBuilder(*pStmt)
		expr, pok := builder.getWhereExprByKey("up_time")
		if pok {
			fmt.Println(expr)
		} else {
			fmt.Println("Not found", pok)
		}

	}
}

//
func tableNameAddSuffix(stmt sqlparser.Select, tbSuffix string) sqlparser.Select {
	nStmt := sqlparser.Select{}
	nStmt = stmt
	switch expr := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		nAli := sqlparser.AliasedTableExpr{
			Partitions: expr.Partitions,
			As:         expr.As,
			Hints:      expr.Hints,
		}
		if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
			oldName := tbn.Name.String()
			newTb := tbn.ToViewName()
			newTb.Name = sqlparser.NewTableIdent(oldName + "_" + tbSuffix)
			nAli.Expr = newTb
			fmt.Println(nStmt.From[0])
		}
		nStmt.From = make(sqlparser.TableExprs, 1)
		nStmt.From[0] = &nAli
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	}
	return nStmt
}
