/*
Copyright 2018 Sgoby.com.

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

package core

import (
	"context"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/myhub/core/node"
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/rule"
	"github.com/golang/glog"
	"strings"
)

var myApp *Application

func init(){
	myApp = new(Application)
	myApp.Context,myApp.cancelFunc = context.WithCancel(context.Background())
}

type Application struct {
	Context      context.Context
	cancelFunc   func()
	config       config.Config
	authServer   mysql.AuthServer
	listener     *mysql.Listener
	//serverHandle *server.ServerHandler
	nodeManager  *node.NodeManager
	schema       *schema.Schema
	ruleManager  *rule.RuleManager
}
//
func App() *Application {
	return myApp
}
func (this *Application) SetAuthServer(au mysql.AuthServer){
	this.authServer = au
}
func (this *Application) GetSchema() *schema.Schema{
	return this.schema
}
func (this *Application) GetRuleManager() *rule.RuleManager{
	return this.ruleManager
}
func (this *Application) GetNodeManager() *node.NodeManager{
	return this.nodeManager
}
func (this *Application) GetSlowLogTime() int{
	return this.config.SlowLogTime
}
//
func (this *Application) GetListener() *mysql.Listener {
	return this.listener
}
func (this *Application) LoadConfig(cnf config.Config) (err error){
	authServerMy := mysql.NewAuthServerMy()
	for _,userCnf := range cnf.Users {
		if len(userCnf.AllowIps) < 1 {
			userCnf.AllowIps = "127.0.0.1"
		}
		//
		if len(userCnf.Databases) < 1 {
			userCnf.Databases = "*"
		}
		//
		mAuthServerMyEntry := &mysql.AuthServerMyEntry{
			Password:userCnf.Password,
			SourceHosts:strings.Split(userCnf.AllowIps,","),
			Databases:strings.Split(userCnf.Databases,","),
		}
		//
		if entry,ok := authServerMy.Entries[userCnf.Name];ok{
			entry = append(entry,mAuthServerMyEntry)
			continue
		}
		authServerMy.Entries[userCnf.Name] = []*mysql.AuthServerMyEntry{mAuthServerMyEntry}
	}
	this.SetAuthServer(authServerMy)
	//
	this.nodeManager,err = node.NewNodeManager(cnf.Nodes)
	if err !=nil{
		return err
	}
	//
	this.schema,err = schema.NewSchema(cnf.Schema)
	if err !=nil{
		return err
	}
	//
	this.ruleManager,err = rule.NewRuleManager(cnf.Rules)
	if err !=nil{
		return err
	}
	//
	this.config = cnf
	return nil
}
//
func (this *Application) Run(sh mysql.Handler) (err error) {
	this.listener, err = mysql.NewListener("tcp", this.config.ServeListen, this.authServer, sh)
	if err != nil {
		return err
	}
	defer this.listener.Close()
	glog.Info("Listener on: ",this.config.ServeListen)
	glog.Flush()
	this.listener.Accept()
	return nil
}
//
func (this *Application) Close(){

}