package main

import (
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/core/server"
	"flag"
	"runtime"
	"github.com/sgoby/myhub/core/client"
	"github.com/golang/glog"
	"fmt"
	"strings"
)


var appConf *config.Config

func init() {
	var err error;
	configFilePath := flag.String("cnf", "conf/myhub.xml", "setting config file")
	appConf,err = config.ParseConfig(*configFilePath)
	if err != nil{
		fmt.Println(err)
		return
	}
	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", appConf.LogPath)
	if strings.ToLower(appConf.LogSql) == "on"{
		flag.Set("query", "true")
	}
	if appConf.SlowLogTime > 0{
		flag.Set("slow", "true")
	}
	//
	lv := 5
	switch strings.ToLower(appConf.LogLevel){
	case "debug","info":
		lv = 0
	case "warn":
		lv = 1
	case "error":
		lv = 2
	}
	if lv > 0{
		flag.Set("lv", fmt.Sprintf("%d",lv))
	}
	flag.Parse()
}

func main(){
	runtime.GOMAXPROCS(runtime.NumCPU())
	if err := core.App().LoadConfig(*appConf);err != nil{
		glog.Exit(err)
		return
	}
	//
	c := client.NewDefaultConnector()
	if err := c.AutoCrateTables();err != nil{
		glog.Exit(err)
		return
	}
	//
	serverHandle := server.NewServerHandler()
	core.App().Run(serverHandle)
}