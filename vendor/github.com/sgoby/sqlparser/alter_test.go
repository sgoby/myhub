package sqlparser

import (
	"testing"
	"fmt"
)

func Test_Alter(t *testing.T){
	//a := &Alter{}
	a,err := ParseAlterStmt("ALTER TABLE `test`.`dddd` CHANGE `id` `id` INT(11) NOT NULL;")
	if err != nil{
		fmt.Println(err)
	}
	sql := String(a)
	fmt.Println(sql)
}
