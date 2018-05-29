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