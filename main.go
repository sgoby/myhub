package main

import (
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/core/server"
	"flag"
	"runtime"
	"github.com/sgoby/myhub/core/client"
	"github.com/golang/glog"
)



func init() {
	//flag.Var(&logging.traceLocation, "log_backtrace_at", "when logging hits line file:N, emit a stack trace")
	//glog's setting
	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", "logs")
	flag.Parse()
}

func main(){
	runtime.GOMAXPROCS(runtime.NumCPU())
	//
	configFilePath := flag.String("cnf", "conf/myhub.xml", "setting config file")
	//
	mConfig,err := config.ParseConfig(*configFilePath)
	if err != nil{
		glog.Exit(err)
		return
	}
	//
	if err = core.App().LoadConfig(*mConfig);err != nil{
		glog.Exit(err)
		return
	}
	//
	c := client.NewDefaultConnector()
	if err = c.AutoCrateTables();err != nil{
		glog.Exit(err)
		return
	}
	//
	serverHandle := server.NewServerHandler()
	core.App().Run(serverHandle)
}