package config

import (
	"io/ioutil"
	"encoding/xml"
	"regexp"
	"os"
	"strings"
	"fmt"
)

type Config struct {
	ServeListen   string `xml:"serveListen"`
	ServeUser     string `xml:"serveUser"`
	ServePassword string `xml:"servePassword"`
	ServeCharset  string `xml:"serveCharset"`
	//
	WebListen   string `xml:"webListen"`
	WebUser     string `xml:"webUser"`
	WebPassword string `xml:"webPassword"`
	//
	LogPath     string `xml:"logPath"`
	LogLevel    string `xml:"logLevel"`
	LogSql      string `xml:"logSql"`
	SlowLogTime int    `xml:"slowLogTime"`
	//
	AllowIPs     string `xml:"allowIPs"`
	BlacklistSql string `xml:"blacklistSql"`
	//
	Nodes  Node   `xml:"node"`
	Schema Schema `xml:"schema"`
	Rules  []Rule `xml:"rules>rule"`
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
	MaxOpenConns int    `xml:"maxOpenConns,attr"`
	MaxIdleConns int    `xml:"maxIdleConns,attr"`
	MaxIdleTime  int    `xml:"maxIdleTime,attr"`
}
type Host struct {
	RwType   string `xml:"type,attr"`
	Name     string `xml:"name,attr"`
	Address  string `xml:"address,attr"`
	User     string `xml:"user,attr"`
	Password string `xml:"password,attr"`
	Weight   int    `xml:"weight,attr"` //权重
	ReadHost []Host `xml:"host"`
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
}

//
func ParseConfig(filePath string) (conf *Config, err error) {
	fPath,_,err := optFilePath(filePath)
	if err != nil{
		return nil,err
	}
	filePath = fPath
	//
	data, err := ioutil.ReadFile(filePath)
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
	//
	return mConfig, nil
}

//
func (this *Config) optSchema() error {
	for dbN, db := range this.Schema.Databases {
		for tbN, tb := range db.Tables {

			fPath,ok,err := optFilePath(tb.CreateSql)
			if !ok{
				continue
			}
			if err != nil{
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
		this.Schema.Databases[dbN] = db
	}
	return nil
}

//
func optFilePath(filePath string) (newFilePath string,isFilePath bool, err error) {
	reg, err := regexp.Compile("(^[a-zA-Z]\\:\\/|^\\.\\/|^\\/|^[a-zA-Z_])((\\w|\\/|\\.|\\-))*(\\/\\w+|\\.\\w+)$")
	if err != nil {
		return filePath,false, err
	}
	localreg, err := regexp.Compile("(^\\.\\/|^[a-zA-Z_][^\\:\\/])+")
	if err != nil {
		return filePath,false, err
	}
	if !reg.MatchString(filePath) {
		return filePath,false, fmt.Errorf("the string is not file path: %s", filePath)
	}
	//current directory
	if localreg.MatchString(filePath) {
		localDir, err := os.Getwd()
		if err != nil {
			return filePath,true, err
		}
		strings.Replace(filePath, "./", "", -1)
		filePath = localDir + "/" + filePath;
	}
	return filePath,true, nil
}