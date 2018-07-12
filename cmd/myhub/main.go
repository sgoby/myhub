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
	"github.com/sgoby/myhub/core/client"
	"github.com/golang/glog"
	"fmt"
	"os"
	"os/signal"
	_ "net/http/pprof"
	"runtime/pprof"
	"syscall"
	"net/http"
)

const (
	production  = "production"
	development = "development"
)

const (
	SIGUSR1 = syscall.Signal(0xa)
	SIGUSR2 = syscall.Signal(0xc)
)

var env *string
var configFilePath *string

func init() {
	configFilePath = flag.String("cnf", "conf/myhub.xml", "setting config file")
	env = flag.String("env", "development", "program environment")
	flag.Parse()
}

func main() {
	appConf, err := config.ParseConfig(*configFilePath,false)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	if err := core.App().LoadConfig(*appConf); err != nil {
		glog.Exit(err)
		return
	}
	//
	c := client.NewDefaultConnector()
	if err := c.AutoCrateTables(); err != nil {
		glog.Exit(err)
		return
	}
	if *env == development {
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	}
	//
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
		SIGUSR1,
		SIGUSR2,
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
			} else if sig == SIGUSR1 {
				glog.Warning("reload config......")
				reloadConfig()
			}
		}
	}()
	//
	serverHandle := server.NewServerHandler()
	core.App().Run(serverHandle)
}

//
func reloadConfig() {
	newConf, err := config.ParseConfig(*configFilePath,true)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := core.App().TestConfig(*newConf);err != nil{
		fmt.Println(err)
		return
	}
	if err := core.App().LoadConfig(*newConf); err != nil {
		fmt.Println(err)
		return
	}
}

//
func saveHeapProfile() {
	//runtime.GC()
	f, err := os.OpenFile("myhub.prof", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	pprof.Lookup("heap").WriteTo(f, 1)
}
