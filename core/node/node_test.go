package node


import (
	"fmt"
	"testing"
	"github.com/sgoby/myhub/config"
)
func Test_NodeManager(t *testing.T){
	mConfig,err := config.ParseConfig("D:/workspace/golang/src/github.com/sgoby/myhub/config/conf.xml")
	if err != nil{
		fmt.Println(err)
		return
	}
	fmt.Println(mConfig)
	nManager,err := NewNodeManager(mConfig.Nodes)
	if err != nil{
		fmt.Println(err)
		return
	}
	db, err := nManager.GetMysqlClient("test",HOST_READ)
	sql := "SELECT id,dealer FROM dealer_info_201609"
	re, err := db.Exec(sql, nil)
	if err != nil {
		fmt.Println("Result error:", err)
		return
	}
	fmt.Println(re)
	nManager.Close()
	//
	ch := make(chan int)
	ch <- 1
}