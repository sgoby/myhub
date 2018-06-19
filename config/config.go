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

package config

import (
	"io/ioutil"
	"encoding/xml"
	"regexp"
	"os"
	"strings"
	"fmt"
	"path/filepath"
	"github.com/mcuadros/go-defaults"
)

type Config struct {
	ServeListen     string `xml:"serveListen" default:"0.0.0.0:8520"`
	ServeUser       string `xml:"serveUser"`
	ServePassword   string `xml:"servePassword"`
	ServeCharset    string `xml:"serveCharset"`
	WorkerProcesses int    `xml:"workerProcesses"`
	MaxConnections  int    `xml:"maxConnections" default:"2048"`
	//
	WebListen   string `xml:"webListen"`
	WebUser     string `xml:"webUser"`
	WebPassword string `xml:"webPassword"`
	//
	LogPath     string `xml:"logPath" default:"logs"`
	LogLevel    string `xml:"logLevel" default:"error"`
	LogSql      string `xml:"logSql" default:"off"`
	SlowLogTime int    `xml:"slowLogTime"`
	//
	BlacklistSql string `xml:"blacklistSql"`
	//
	Nodes  Node   `xml:"node"`
	Schema Schema `xml:"schema"`
	Rules  []Rule `xml:"rules>rule"`
	Users  []User `xml:"users>user"`
}

//
type User struct {
	Name      string `xml:"name,attr"`
	Password  string `xml:"passwrod,attr"`
	Charset   string `xml:"charset,attr"`
	Databases string `xml:"db,attr"`        //Multiple configurations join with ","
	AllowIps  string `xml:"ip,attr"`        //Multiple configurations join with ","
	Privilege string `xml:"privilege,attr"` //ex: select,update,delete. Multiple configurations join with ","
	Tables    string `xml:"table,attr"`     //Multiple configurations join with ","
}

//
type Node struct {
	Databases []OrgDatabase `xml:"dataBases>dataBase"`
	Hosts     []Host        `xml:"hosts>host"`
}

//
type OrgDatabase struct {
	Name         string `xml:"name,attr"`
	Host         string `xml:"host,attr"`
	MaxOpenConns int    `xml:"maxOpenConns,attr" default:"16"`
	MaxIdleConns int    `xml:"maxIdleConns,attr" default:"4"`
	MaxIdleTime  int    `xml:"maxIdleTime,attr" default:"60"`
}
type Host struct {
	RwType   string `xml:"type,attr"`
	Name     string `xml:"name,attr"`
	Address  string `xml:"address,attr"`
	User     string `xml:"user,attr"`
	Password string `xml:"password,attr"`
	Weight   int    `xml:"weight,attr"` //权重
	ReadHost []Host `xml:"host"`
	Driver   string `xml:"driver"`
}

//============================================================================
//Schema
type Schema struct {
	Databases []Database `xml:"dataBase"`
}
type Database struct {
	Tables        []Table `xml:"table"`
	Name          string  `xml:"name,attr"`
	ProxyDataBase string  `xml:"proxyDataBase,attr"` //可直接代理某一个数据库
	BlacklistSQL  string  `xml:"blacklistSql,attr"`  //SQL黑名单
}

//
type Table struct {
	Name      string `xml:"name,attr"`
	CreateSql string `xml:"createSql,attr"`
	Rule      string `xml:"rule,attr"`
	RuleKey   string `xml:"ruleKey,attr"`
	//Shards    []Shard `xml:"shard"`
}
type Shard struct {
	Node      string `xml:"nodeDataBase,attr"`
	RowLimit  string `xml:"rowLimit,attr"`
	RangeExpr string `xml:"between,attr"`
}

//
type Rule struct {
	Name     string  `xml:"name,attr"`
	RuleType string  `xml:"ruleType,attr"`
	Shards   []Shard `xml:"shard"`
	Format   string  `xml:"format,attr"`
	MaxLen   int     `xml:"maxLen,attr"`
}

// record current config file dir
var confDir string
//
func ParseConfig(cnfPath string) (conf *Config, err error) {
	if len(cnfPath) < 1 {
		conf = creatDefaultConfig()
		return
	}
	cnfPath, _, err = optFilePath(cnfPath)
	if err != nil {
		return nil, err
	}
	confDir = filepath.Dir(cnfPath)
	//
	data, err := ioutil.ReadFile(cnfPath)
	if err != nil {
		return nil, err
	}
	mConfig := new(Config)
	err = xml.Unmarshal([]byte(data), &mConfig)
	if err != nil {
		return nil, err
	}
	err = mConfig.optSchema()
	if err != nil {
		return nil, err
	}
	err = mConfig.optUser()
	if err != nil {
		return nil, err
	}
	//
	defaults.SetDefaults(mConfig)
	for i, db := range mConfig.Nodes.Databases {
		defaults.SetDefaults(&db)
		mConfig.Nodes.Databases[i] = db
	}
	return mConfig, nil
}

//
func creatDefaultConfig() *Config {
	cnf := new(Config)
	cnf.AddUser(User{
		Name:      "root",
		Password:  "",
		Databases: "*",
		AllowIps:  "localhost",
		Charset:   "utf-8",
	})
	defaults.SetDefaults(cnf)
	return cnf
}

//
func (this *Config) AddUser(u User) {
	this.Users = append(this.Users, u)
}

//optimization user's database
func (this *Config) optUser() error {
	for i, user := range this.Users {
		if user.Databases == "*" {
			var dbSlice []string
			for _, db := range this.Schema.Databases {
				dbSlice = append(dbSlice, db.Name)
			}
			user.Databases = strings.Join(dbSlice, ",")
		}
		if len(user.AllowIps) < 1 {
			user.AllowIps = "127.0.0.1"
		}
		if len(user.Charset) < 1 {
			user.Charset = "utf-8"
		}
		this.Users[i] = user
	}
	return nil
}

//optimization schema
func (this *Config) optSchema() error {
	for dbN, db := range this.Schema.Databases {
		for tbN, tb := range db.Tables {
			fPath, ok, err := optFilePath(tb.CreateSql)
			if !ok {
				continue
			}
			if err != nil {
				return err
			}
			tb.CreateSql = fPath
			buf, err := ioutil.ReadFile(tb.CreateSql)
			if err != nil {
				return err
			}
			tb.CreateSql = string(buf)
			//
			db.Tables[tbN] = tb
		}
		if len(db.BlacklistSQL) > 0 {
			fPath, ok, err := optFilePath(db.BlacklistSQL)
			if !ok {
				continue
			}
			if err != nil {
				return err
			}
			db.BlacklistSQL = fPath
			buf, err := ioutil.ReadFile(db.BlacklistSQL)
			if err != nil {
				return err
			}
			db.BlacklistSQL = string(buf)
		}
		//
		this.Schema.Databases[dbN] = db
	}
	return nil
}

//
func optFilePath(filePath string) (newFilePath string, isFilePath bool, err error) {
	if len(filePath) < 1 {
		return filePath, false, err
	}
	reg, err := regexp.Compile("(^[a-zA-Z]\\:\\/|^\\.\\/|^\\/|^[a-zA-Z_])((\\w|\\/|\\.|\\-))*(\\/\\w+|\\.\\w+)$")
	if err != nil {
		return filePath, false, err
	}
	localreg, err := regexp.Compile("(^\\.\\/|^[a-zA-Z_][^\\:\\/])+")
	if err != nil {
		return filePath, false, err
	}
	if !reg.MatchString(filePath) {
		return filePath, false, fmt.Errorf("the string is not file path: %s", filePath)
	}
	//current directory
	if localreg.MatchString(filePath) {
		localDir := confDir
		if (len(localDir) <= 0) {
			localDir, err = os.Getwd()
			if err != nil {
				return filePath, true, err
			}
		}
		strings.Replace(filePath, "./", "", -1)
		filePath = localDir + "/" + filePath;
	}
	return filePath, true, nil
}
