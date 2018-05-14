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


var appConf *config.Config

func init() {
	var err error;
	configFilePath := flag.String("cnf", "conf/myhub.xml", "setting config file")
	appConf,err = config.ParseConfig(*configFilePath)
	if err != nil{
		glog.Exit(err)
		return
	}
	//appConf = mConfig;
	//flag.Var(&logging.traceLocation, "log_backtrace_at", "when logging hits line file:N, emit a stack trace")
	//glog's setting
	flag.Set("alsologtostderr", "true")
	flag.Set("log_dir", appConf.LogPath)
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