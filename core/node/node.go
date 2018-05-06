package node

import (
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/myhub/config"
	"strings"
	"errors"
	"fmt"
	"strconv"
	"math/rand"
	"time"
)

const (
	HOST_WRITE = "write"
	HOST_READ  = "read"
)
//
type NodeManager struct {
	config   config.Node
	hostsMap map[string]*Host
}
//
func NewNodeManager(conf config.Node) (*NodeManager, error) {
	nm := &NodeManager{
		config:   conf,
		hostsMap: make(map[string]*Host),
	}
	nm.initHostMap()
	err := nm.initDatabaseMap()
	//
	return nm, err
}
//
func (this *NodeManager) Close() error{
	for _, host := range this.hostsMap {
		err := host.close()
		if err != nil{
			return err
		}
	}
	return nil
}
//
func (this *NodeManager) GetMysqlClient(dbName, rwType string) (*mysql.Client, error) {
	//
	for _, host := range this.hostsMap {
		c, err := host.GetMysqlClient(dbName, rwType)
		if err == nil && c != nil {
			return c, err
		}
	}
	//
	return nil, errors.New("Not found database")
}
func (this *NodeManager) initHostMap() {
	for _, conf := range this.config.Hosts {
		this.hostsMap[conf.Name] = this.newHost(conf)
	}
}
func (this *NodeManager) initDatabaseMap() error {
	for _, dbCnf := range this.config.Databases {
		err := this.newDataBase(dbCnf)
		if err != nil {
			return err
		}
	}
	return nil
}
func (this *NodeManager) newHost(conf config.Host) *Host {
	host := &Host{
		config:      conf,
		databaseMap: make(map[string]*Database),
	}
	//
	for _, cf := range conf.ReadHost {
		rHost := this.newHost(cf)
		host.readHosts = append(host.readHosts, rHost)
	}
	return host
}
func (this *NodeManager) newDataBase(conf config.OrgDatabase) (error) {
	db := &Database{
		config: conf,
	}
	host, ok := this.hostsMap[conf.Host]
	if !ok {
		return errors.New(fmt.Sprintf("Can't found host '%s'", conf.Host))
	}
	db.host = host
	db.host.addDataBase(conf.Name, db)
	cW, err := db.openWithHost(host)
	if err != nil {
		return err
	}
	db.myWriteClient = cW
	//
	for _, rhost := range host.readHosts {
		cR, err := db.openWithHost(rhost)
		if err != nil {
			return err
		}
		db.myReadClient = append(db.myReadClient, cR)
	}
	return nil
}

//=========================================================================================
//
type Host struct {
	config      config.Host
	databaseMap map[string]*Database
	readHosts   []*Host //只读库
}

//
type Database struct {
	config        config.OrgDatabase
	host          *Host
	myReadClient  []*mysql.Client //根据权重随机分配 *[]mysql.Client
	myWriteClient *mysql.Client
}
//
func (this *Host) addDataBase(name string, db *Database) {
	this.databaseMap[name] = db
}
//
func (this *Host) close() error{
	for _,db := range this.databaseMap{
		err := db.close()
		if err != nil{
			return err
		}
	}
	return nil
}
//
func (this *Host) GetMysqlClient(dbName, rwType string) (*mysql.Client, error) {
	db, ok := this.databaseMap[dbName]
	if ok {
		return db.getMysqlClient(rwType)
	}
	return nil, errors.New("Not found database")
}
//======================================================================
func (this *Database) close() error{
	if this.myWriteClient != nil{
		err := this.myWriteClient.Close()
		if err != nil{
			return err
		}
	}
	//
	for _,rc := range this.myReadClient{
		if rc != nil{
			err :=  rc.Close()
			if err != nil{
				return err
			}
		}
	}
	return nil
}
//with host type
func (this *Database) getMysqlClient(rwType string) (*mysql.Client, error) {
	if  rwType == HOST_READ && this.myReadClient != nil &&  len(this.host.readHosts) > 0 {
		var wArr []int
		for i, rhost := range this.host.readHosts {
			for j := 0; j < rhost.config.Weight; j++ {
				wArr = append(wArr, i)
			}
		}
		//
		n := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(this.myReadClient))
		index := 0
		if n < len(wArr) {
			index = wArr[n]
		}
		if index < len(this.myReadClient) && this.myReadClient[index] !=nil{
			return this.myReadClient[index], nil
		}
	}
	if this.myWriteClient != nil {
		return this.myWriteClient, nil
	}
	return nil, errors.New("No db connector can use")
}

//
func (this *Database) openWithHost(h *Host) (*mysql.Client, error) {
	params, err := this.getConnParamsWithHost(h)
	if err != nil {
		return nil, err
	}
	myClient, err := mysql.Open(&params, "")
	if err != nil {
		return nil, err
	}
	//
	myClient.SetMaxIdleConns(this.config.MaxIdleConns)
	myClient.SetMaxOpenConns(this.config.MaxOpenConns)
	//this.myClient.SetMaxLifeTime()
	return myClient, nil
}
func (this *Database) getConnParamsWithHost(pHost *Host) (mysql.ConnParams, error) {
	params := mysql.ConnParams{}
	//
	addrs := strings.Split(pHost.config.Address, ":")
	hostName := pHost.config.Name
	if len(addrs) < 2 {
		return params, errors.New(fmt.Sprintf("Invalid address: %s", hostName))
	}
	host := addrs[0]
	post, err := strconv.Atoi(addrs[1])
	if err != nil {
		return params, err
	}
	//
	params.Host = host
	params.Port = post
	params.Uname = pHost.config.User
	params.Pass = pHost.config.Password
	params.DbName = this.config.Name
	return params, nil
}