package client

import (
	"testing"
	"github.com/sgoby/sqlparser"
	"fmt"
)

func Test_Query(t *testing.T){
	/*
	ALTER TABLE `test`.`dddd` CHANGE `id` `id` INT(11) NOT NULL;
	ALTER TABLE `test`.`dddd` ADD COLUMN `tt` VARCHAR(32) DEFAULT '' NULL AFTER `val`;
	ALTER TABLE `test`.`dddd` DROP COLUMN `tt`;
	*/
	sql := "ALTER TABLE `test`.`dddd` CHANGE `id` `id` INT(11) NOT NULL"
	//sql := "select * from dddd"
	stmt,err := sqlparser.Parse(sql)
	if err != nil{
		fmt.Println(err)
		return
	}
	switch nStmt := stmt.(type) {
	case *sqlparser.Select:
		fmt.Println("Select")
		fmt.Println("Select",sqlparser.String(nStmt))
	case *sqlparser.Begin:
		fmt.Println("Begin")
	case *sqlparser.Rollback:
		fmt.Println("Rollback")
	case *sqlparser.Commit:
		fmt.Println("Commit")
	case *sqlparser.Use:
		fmt.Println("Use")
	case *sqlparser.Show:
		fmt.Println("Show")
	case *sqlparser.Insert:
		fmt.Println("Insert")
	case *sqlparser.Update:
		fmt.Println("Update")
	case *sqlparser.Delete:
		fmt.Println("Delete")
	case *sqlparser.DDL:
		nStmt.PartitionSpec = &sqlparser.PartitionSpec{Action:"reorganize partition",Name:sqlparser.NewColIdent("id")}
		//fmt.Println(nStmt.Action,nStmt.NewName,nStmt.Table.Name)
		fmt.Println("DDL",nStmt)
		fmt.Println(sqlparser.String(nStmt))
	default:
		fmt.Println("unknow",nStmt)
	}
}