package core

import (
	"context"
	"github.com/sgoby/myhub/config"
	"github.com/sgoby/myhub/mysql"
	//"github.com/sgoby/myhub/core/server"
	"github.com/sgoby/myhub/core/node"
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/rule"
	"github.com/golang/glog"
)
var myApp *Application

func init(){
	myApp = new(Application)
	myApp.Context,myApp.cancelFunc = context.WithCancel(context.Background())
	//myApp.serverHandle = server.NewServerHandler()
	myApp.authServer = mysql.NewAuthServerStatic()
}

type Application struct {
	Context      context.Context
	cancelFunc   func()
	config       config.Config
	authServer   *mysql.AuthServerStatic
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
func (this *Application) GetSchema() *schema.Schema{
	return this.schema
}
func (this *Application) GetRuleManager() *rule.RuleManager{
	return this.ruleManager
}
func (this *Application) GetNodeManager() *node.NodeManager{
	return this.nodeManager
}
//
func (this *Application) GetListener() *mysql.Listener {
	return this.listener
}
func (this *Application) LoadConfig(cnf config.Config) (err error){
	this.authServer.Entries[cnf.ServeUser] = []*mysql.AuthServerStaticEntry{
		{
			Password:   cnf.ServePassword,
			SourceHost: "",
			//UserData:   "userData1",
		},
	}
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
	this.listener.Accept()
	return nil
}
//
func (this *Application) Close(){

}