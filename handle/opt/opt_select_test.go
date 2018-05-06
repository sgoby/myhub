package opt

import (
	"testing"
	"fmt"
)

func Test_OptSelect(t *testing.T){
	sql := "SELECT name FROM dealer_info_201609  GROUP BY id,ss";
	nSql,err := OptimizeSelectSql(sql)
	if err != nil{
		fmt.Println("Error:",err)
		return
	}
	fmt.Println(sql)
	fmt.Println(nSql)
}
