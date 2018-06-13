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
	"os"
	"os/signal"
	 _"net/http/pprof"
	"runtime/pprof"
	"syscall"
	"net/http"
	"strings"
)

var appConf *config.Config

func init() {
	var err error;
	//conf/myhub.xml
	configFilePath := flag.String("cnf", "conf/myhub.xml", "setting config file")
	flag.Parse()
	fmt.Println(*configFilePath)
	appConf, err = config.ParseConfig(*configFilePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	err = os.MkdirAll(appConf.LogPath, os.ModeDir)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	logCnf := glog.LogConfig{}
	logCnf.LogDir = appConf.LogPath
	if strings.ToLower(appConf.LogSql) == "on" {
		logCnf.Query = true
	}
	if appConf.SlowLogTime > 0 {
		logCnf.Slow = true
	}
	logCnf.DefaultLV = appConf.LogLevel
	glog.InitWithCnf(logCnf)
	//
	//runtime.MemProfileRate = 1
}

func main() {
	if err := core.App().LoadConfig(*appConf); err != nil {
		glog.Exit(err)
		return
	}
	//debug.SetGCPercent(5)
	if appConf.WorkerProcesses > 0 {
		runtime.GOMAXPROCS(appConf.WorkerProcesses)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	//
	c := client.NewDefaultConnector()
	if err := c.AutoCrateTables(); err != nil {
		glog.Exit(err)
		return
	}
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	//
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
	)
	//
	go func() {
		for {
			sig := <-sc
			if sig == syscall.SIGINT || sig == syscall.SIGTERM || sig == syscall.SIGQUIT {
				//saveHeapProfile();
				glog.Flush()
				core.App().Close()
				glog.Exit("MyHub close ...")
			} else if sig == syscall.SIGPIPE {
				glog.Info("Ignore broken pipe signal")
			}
		}
	}()
	//
	serverHandle := server.NewServerHandler()
	core.App().Run(serverHandle)
}

func saveHeapProfile() {
	//runtime.GC()
	f, err := os.OpenFile("myhub.prof", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	pprof.Lookup("heap").WriteTo(f, 1)
}