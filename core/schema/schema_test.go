package schema


import (
	"testing"
	"fmt"
	"github.com/sgoby/myhub/config"
)

func Test_schema(t *testing.T){
	mConfig,err := config.ParseConfig("D:/workspace/golang/src/github.com/sgoby/myhub/config/conf.xml")
	if err != nil{
		fmt.Println(err)
		return
	}
	sc,err := NewSchema(mConfig.Schema)
	if err != nil{
		fmt.Println(err)
	}
	db,err := sc.GetDataBase("test")
	if err != nil{
		fmt.Println(err)
	}
	fmt.Println(db)
}
