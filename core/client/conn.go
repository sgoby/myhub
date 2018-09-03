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

package client

import (
	"github.com/sgoby/myhub/mysql"
	"github.com/sgoby/sqlparser/sqltypes"
	"github.com/sgoby/myhub/core"
	"github.com/sgoby/sqlparser"
	"fmt"
	"github.com/sgoby/myhub/core/node"
	"github.com/sgoby/myhub/core/schema"
	"github.com/sgoby/myhub/core/plan"
	"context"
	"sync"
	hresult "github.com/sgoby/myhub/handle/result"
	"github.com/sgoby/myhub/handle/opt"
	"time"
	"github.com/sgoby/myhub/core/plan/select_plan"
	"github.com/sgoby/myhub/core/plan/insert_plan"
	"github.com/sgoby/myhub/core/plan/update_plan"
	"github.com/golang/glog"
	"github.com/sgoby/myhub/core/plan/create_plan"
	"net"
	"github.com/sgoby/myhub/tb"
	"github.com/sgoby/myhub/core/plan/delete_plan"
	"github.com/sgoby/myhub/backend"
)

const (
	EXECUTE_TIMEOUT = 30 // the timeout of total execute
)
//
type IServerHandler interface {
	GetConnectorMap() []*Connector
}

//
type Connector struct {
	MyConn         *mysql.Conn
	DbName         string
	InTransaction  bool                 // turn on the transaction else
	transactionMap map[string]*backend.Tx //[dsn]
	mu             *sync.Mutex
	cancel         func()          // cancel is called after done
	ctx            context.Context // ctx lives for the life of the Connector.
	lastActiveTime time.Time       //  active Time
	extStmtQuerys  []func(pStmt sqlparser.Statement, query string) (rs sqltypes.Result, err error, ok bool)
	lastInsertId   uint64 //LAST_INSERT_ID
	serverHandler  IServerHandler
	execFuncMap    map[string]execFunc
}

//just used for sys auto create table
func NewDefaultConnector() *Connector {
	return NewConnector(&mysql.Conn{})
}

//
func NewConnector(c *mysql.Conn) *Connector {
	conn := &Connector{
		MyConn:         c,
		mu:             new(sync.Mutex),
		transactionMap: make(map[string]*backend.Tx),
		lastActiveTime: time.Now(),
	}
	conn.ctx, conn.cancel = context.WithCancel(core.App().Context)
	//register ext function
	conn.extStmtQuerys = append(conn.extStmtQuerys, conn.explain)
	conn.extStmtQuerys = append(conn.extStmtQuerys, conn.describe)
	//
	return conn;
}

//
func (this *Connector) SetServerHandler(sh IServerHandler) {
	this.serverHandler = sh
}

//Verify database permissions
func (this *Connector) VerifyDatabaseAuth(dbName string) bool {
	dbs := this.MyConn.GetDatabases()
	if dbs == nil {
		return true
	}
	for _, db := range dbs {
		if db == "*" || db == dbName {
			return true
		}
	}
	return false
}

//
func (this *Connector) AutoCrateTables() error {
	mSchema := core.App().GetSchema()
	mRuleManager := core.App().GetRuleManager()
	//
	return mSchema.Foreach(func(name string, db *schema.Database) error {
		return db.Foreach(func(tbName string, tb *schema.Table) error {
			stmt := tb.GetCreateStmt()
			if stmt == nil {
				return nil
			}
			plans, err := create_plan.BuildCreatePlan(tb, stmt, mRuleManager)
			if err != nil {
				return err
			}
			rs, err := this.execAutoCreatePlans(plans)
			if err != nil {
				return err
			}
			glog.Info(rs)
			return nil
		}, true)
	}, true)
}

//rollback transaction
func (this *Connector) TxRollback() error {
	ctx, cancel := context.WithTimeout(this.ctx, time.Second*EXECUTE_TIMEOUT) //default timeout
	defer cancel()
	var execErr error;
	var wg sync.WaitGroup
	for _, tx := range this.transactionMap {
		wg.Add(1)
		go func(pTx *backend.Tx, mctx context.Context) {
			defer wg.Done()
			select {
			case <-mctx.Done():
				execErr = mctx.Err()
				return
			default:
			}
			//glog.Info("Rollback: ")
			err := pTx.Rollback()
			if err != nil {
				cancel()
				glog.Error(err)
				execErr = err
			}
		}(tx, ctx)
	}
	return execErr
}
func (this *Connector) TxCommit() error {
	ctx, cancel := context.WithTimeout(this.ctx, time.Second*EXECUTE_TIMEOUT) //default timeout
	defer cancel()
	var execErr error;
	var wg sync.WaitGroup
	for _, tx := range this.transactionMap {
		wg.Add(1)
		go func(pTx *backend.Tx, mctx context.Context) {
			defer wg.Done()
			select {
			case <-mctx.Done():
				execErr = mctx.Err()
				return
			default:
			}
			//glog.Info("Commit: ")
			err := pTx.Commit()
			if err != nil {
				cancel()
				glog.Error(err)
				execErr = err
			}
		}(tx, ctx)
	}
	return execErr
}

//
func (this *Connector) clearTransactionTx() {
	this.transactionMap = make(map[string]*backend.Tx)
}

//
func (this *Connector) addTransactionTx(dsn string, tx *backend.Tx) {
	this.transactionMap[dsn] = tx
}

//
func (this *Connector) getTransactionTx(dsn string) *backend.Tx {
	if tx, ok := this.transactionMap[dsn]; ok {
		return tx
	}
	return nil
}

//
func (this *Connector) UseDataBase(dbName string) {
	this.MyConn.SchemaName = dbName
}

//
func (this *Connector) UpActiveTime() {
	this.lastActiveTime = time.Now()
}

//
func (this *Connector) GetLastActiveTime() time.Time {
	return this.lastActiveTime;
}

//
func (this *Connector) GetDB() string {
	return this.MyConn.SchemaName
}

//
func (this *Connector) GetUser() string {
	return this.MyConn.User;
}

//
func (this *Connector) GetConnectionID() int64 {
	return this.MyConn.ID();
}

//
func (this *Connector) GetRemoteAddr() net.Addr {
	return this.MyConn.RemoteAddr()
}
//if the query in the blacklist, Myhub will refuse execute
func (this *Connector) IsBlacklistQuery(query string) bool{
	if core.App().GetSchema() == nil{
		return false
	}
	db,err := core.App().GetSchema().GetDataBase(this.GetDB())
	if err != nil{
		return false
	}
	return db.InBlacklistSql(query)
}
//
func (this *Connector) ComQuery(stmt sqlparser.Statement, query string) (sqltypes.Result, error) {
	rwType := node.HOST_WRITE
	switch nStmt := stmt.(type) {
	case *sqlparser.Select:
		rwType = node.HOST_READ
		if len(nStmt.From) < 1 {
			rs, err, ok := this.queryNoFromSelect(nStmt, query)
			if err != nil || ok {
				return rs, err
			}
		} else {
			tbNameExpr, ok := nStmt.From[0].(*sqlparser.AliasedTableExpr)
			if ok {
				glog.Info("unKnow Select, not support:", tbNameExpr)
				if tbn, ok := tbNameExpr.Expr.(sqlparser.TableName); ok {
					//
					// DUAL is purely for the convenience of people who require that all SELECT statements
					// should have FROM and possibly other clauses. MySQL may ignore the clauses. MySQL
					// does not require FROM DUAL if no tables are referenced.
					if tbn.Name.String() == "dual" {
						rs, err, ok := this.queryNoFromSelect(nStmt, query)
						if err != nil || ok {
							return rs, err
						}
					}
				}
			}
		}
	case *sqlparser.Begin: //begin transaction
		this.InTransaction = true;
		return sqltypes.Result{RowsAffected: 1}, nil
	case *sqlparser.Rollback: //
		this.InTransaction = false;
		err := this.TxRollback()
		this.clearTransactionTx()
		return sqltypes.Result{RowsAffected: 1}, err
	case *sqlparser.Commit: //
		this.InTransaction = false;
		err := this.TxCommit()
		this.clearTransactionTx()
		return sqltypes.Result{RowsAffected: 1}, err
	case *sqlparser.Use:
		dbName := nStmt.DBName.String()
		if this.VerifyDatabaseAuth(dbName) {
			this.UseDataBase(nStmt.DBName.String())
			return sqltypes.Result{RowsAffected: 1}, nil
		}
		return sqltypes.Result{}, fmt.Errorf("Access denied for user '%s' to database '%s'", this.MyConn.User, dbName)
	case *sqlparser.Show:
		return this.execShowStatement(nStmt,query)
	case *sqlparser.Update, *sqlparser.Insert:
	case *sqlparser.Delete:
		if nStmt.Where == nil{
			return sqltypes.Result{}, fmt.Errorf("Myhub refused execute: %s",query)
		}
	case *sqlparser.DDL:
		if nStmt.Action == sqlparser.DropStr || nStmt.Action == sqlparser.TruncateStr{
			return sqltypes.Result{}, fmt.Errorf("Myhub refused execute: %s",query)
		}
		glog.Info("unKnow DDL, not support:", nStmt)
	default: //case *sqlparser.OtherRead: //explain
		var rs sqltypes.Result
		var err error
		var ok bool
		for _, f := range this.extStmtQuerys {
			rs, err, ok = f(stmt, query)
			if err != nil || ok {
				return rs, err
			}
		}
		glog.Info("unKnow, not support:", nStmt)
		return sqltypes.Result{}, nil
	}
	//
	dbName := this.GetDB()
	if len(dbName) <= 0 {
		return sqltypes.Result{}, fmt.Errorf("No database selected")
	}
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return sqltypes.Result{}, err
	}
	//================================
	plans, err := this.buildSchemaPlan(db, stmt)
	if len(plans) < 1 && err == nil {
		// if build failed
		return this.execProxyPlan(db, stmt, query, rwType)
	} else if err != nil {
		return sqltypes.Result{}, err
	}
	return this.execSchemaPlans(stmt, plans, rwType)
}

//
func (this *Connector) execShowStatement(pStmt *sqlparser.Show, query string) (rs sqltypes.Result,err error) {
	mShow := tb.ParseShowStmt(query)
	if mShow.ExprIsEmpty(){
		return sqltypes.Result{}, nil
	}
	switch mShow.ExprStr[0] {
	case tb.SHOW_FIELDS:
		rs,err,_ = this.showFields(pStmt,query)
	case tb.SHOW_TABLES:
		rs,err,_ = this.showTables(pStmt,query)
	case tb.SHOW_CREATE:
		rs,err,_ = this.showCreate(pStmt,query,mShow)
	case tb.SHOW_DATABASES:
		rs,err,_ = this.showDatebases(pStmt,query)
	case tb.SHOW_KEYS:
		rs,err,_ = this.showKeys(pStmt,query)
	case tb.SHOW_PROCESSLIST:
		rs,err,_ = this.showProcesslist(pStmt,query)
	case tb.SHOW_PROFILES:
		rs,err,_ = this.showProfiles(pStmt,query)
	case tb.SHOW_STATUS:
		rs,err,_ = this.showStatus(pStmt,query)
	case tb.SHOW_VARIABLES:
		rs,err,_ = this.showVariables(pStmt,query)
	}
	return
}
//
func (this *Connector) Close() (err error) {
	if len(this.transactionMap) > 0 {
		err = this.TxRollback()
		this.clearTransactionTx()
	}
	if !this.MyConn.IsClosed() {
		this.MyConn.Close()
	}
	if this.cancel != nil {
		this.cancel()
	}
	return nil
}

//build shard table execute plan
func (this *Connector) buildSchemaPlan(db *schema.Database, pStmt sqlparser.Statement) (plans []plan.Plan, err error) {
	if pStmt == nil {
		return nil, nil //to poxy database exceute
	}
	switch stmt := pStmt.(type) {
	case *sqlparser.Select:
		// not support multiple table
		if len(stmt.From) > 1 || len(stmt.From) < 1 {
			return nil, nil
		}
		tb, _ := this.getSchemaTable(stmt.From[0])
		if tb == nil {
			return nil, nil
		}
		//
		return select_plan.BuildSelectPlan(tb, stmt, core.App().GetRuleManager())
	case *sqlparser.Update:
		// not support multiple table
		if len(stmt.TableExprs) > 1 || len(stmt.TableExprs) < 1 {
			return nil, nil
		}
		tb, _ := this.getSchemaTable(stmt.TableExprs[0])
		if tb == nil {
			return nil, nil
		}
		//
		return update_plan.BuildUpdatePlan(tb, stmt, core.App().GetRuleManager())
	case *sqlparser.Insert: // replace
		tbName := stmt.Table.Name.String()
		tb, _ := this.getSchemaTableByName(tbName)
		if tb == nil {
			return nil, nil
		}
		//
		return insert_plan.BuildInsertPlan(tb, stmt, core.App().GetRuleManager(), this.GetDB())
	case *sqlparser.Delete:
		// not support multiple table
		if len(stmt.TableExprs) > 1 || len(stmt.TableExprs) < 1 {
			return nil, nil
		}
		tb, _ := this.getSchemaTable(stmt.TableExprs[0])
		if tb == nil {
			return nil, nil
		}
		//
		return delete_plan.BuildDeletePlan(tb, stmt, core.App().GetRuleManager())
	case *sqlparser.DDL:
		glog.Info("DDL", pStmt)
	case *sqlparser.Stream:
		glog.Info("Stream", pStmt)
	default:
		glog.Info("unKnow", pStmt)
	}
	return nil, nil
}

//========================================================================
func (this *Connector) getSchemaTable(pExpr sqlparser.TableExpr) (*schema.Table, error) {
	switch expr := pExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
			dbName := this.GetDB()
			tbName := tbn.Name.String()
			db, err := core.App().GetSchema().GetDataBase(dbName)
			if err != nil {
				return nil, err
			}
			return db.GetTable(tbName), nil
		}
		if tbn, ok := expr.Expr.(*sqlparser.Subquery); ok {
			if stmt,ok := tbn.Select.(*sqlparser.Select);ok{
				return this.getSchemaTable(stmt.From[0])
			}
		}
	case *sqlparser.ParenTableExpr:
	case *sqlparser.JoinTableExpr:
	}
	return nil, nil
}
func (this *Connector) getSchemaTableByName(tbName string) (*schema.Table, error) {
	dbName := this.GetDB()
	db, err := core.App().GetSchema().GetDataBase(dbName)
	if err != nil {
		return nil, err
	}
	return db.GetTable(tbName), nil
}

//========================================================================
//execute shard plan
func (this *Connector) execProxyPlan(db *schema.Database, pStmt sqlparser.Statement, query string, rwType string) (rs sqltypes.Result, err error) {
	proxyDbName := db.GetProxyDbName()
	if len(proxyDbName) < 1 {
		return sqltypes.Result{}, fmt.Errorf("no database")
	}
	myClient, err := core.App().GetNodeManager().GetMysqlClient(proxyDbName, rwType)
	if err != nil {
		return sqltypes.Result{}, err
	}
	if pStmt == nil {
		return myClient.Exec(query)
	}
	switch stmt := pStmt.(type) {
	case *sqlparser.Select:
		if stmt.From != nil {
			for _, from := range stmt.From {
				if expr, ok := from.(*sqlparser.AliasedTableExpr); ok {
					if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
						if !tbn.Qualifier.IsEmpty() && tbn.Qualifier.String() == this.GetDB() {
							newTb := tbn.ToViewName()
							if !tbn.Qualifier.IsEmpty() {
								newTb.Qualifier = sqlparser.NewTableIdent(proxyDbName)
							}
							expr.Expr = newTb
							query = sqlparser.String(stmt)
						}
					}
				}
			}
		}
	case *sqlparser.Insert:
		tbn := stmt.Table
		if !tbn.IsEmpty() && tbn.Qualifier.String() == this.GetDB() {
			if !tbn.Qualifier.IsEmpty() {
				tbn.Qualifier = sqlparser.NewTableIdent(proxyDbName)
			}
		}
		stmt.Table = tbn
	case *sqlparser.Update:
		if stmt.TableExprs != nil {
			for _, tbExpr := range stmt.TableExprs {
				if expr, ok := tbExpr.(*sqlparser.AliasedTableExpr); ok {
					if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
						if !tbn.Qualifier.IsEmpty() && tbn.Qualifier.String() == this.GetDB() {
							newTb := tbn.ToViewName()
							if !tbn.Qualifier.IsEmpty() {
								newTb.Qualifier = sqlparser.NewTableIdent(proxyDbName)
							}
							expr.Expr = newTb
							query = sqlparser.String(stmt)
						}
					}
				}
			}
		}
	default:
	}
	rs,err = myClient.Exec(query)
	if err != nil{
		if this.needReconnect(err){
			myClient.UpStatus(false)
		}
	}
	return
}

//
func (this *Connector) execAutoCreatePlans(plans []plan.Plan) (sqltypes.Result, error) {
	return this.execSchemaPlans(nil, plans, node.HOST_WRITE)
}

//
func (this *Connector) execSchemaPlans(mainStmt sqlparser.Statement, plans []plan.Plan, rwType string) (sqltypes.Result, error) {
	ctx, cancel := context.WithTimeout(this.ctx, time.Second*EXECUTE_TIMEOUT) //default timeout
	defer cancel()
	//
	var rsArr []sqltypes.Result
	var execErr error
	//
	var wg sync.WaitGroup
	for _, plan := range plans {
		nodedb, err := core.App().GetNodeManager().GetMysqlClient(plan.NodeDBName, rwType)
		if err != nil {
			return sqltypes.Result{}, err
		}
		for _, query := range plan.QueryContent {
			querySql := query.GetQuerySql()
			//
			nSql, isSelect, err := opt.OptimizeSelectStmtSql(query.GetQueryStmt()) // optimization execute sql
			if isSelect && err == nil {
				querySql = nSql
			}
			//
			if err != nil && isSelect {
				return sqltypes.Result{}, fmt.Errorf("Statement is nil", err)
			}
			wg.Add(1)
			go func(sql string, this *Connector, mctx context.Context,pMainStmt sqlparser.Statement) {
				defer wg.Done()
				defer func() {
					if x := recover(); x != nil {
						cancel()
						glog.Errorf("execSchemaPlans caught panic:\n%v\n%s", x, tb.Stack(4))
						execErr = fmt.Errorf("execSchema Error:%v",x)
					}
				}()
				//
				select {
				case <-mctx.Done():
					execErr = mctx.Err()
					return
				default:
				}
				if this.InTransaction  {
					//transaction for write only
					if rwType == node.HOST_WRITE{
						rs, err :=this.execTransactionTx(nodedb,sql,mctx)
						if err != nil{
							if this.needReconnect(err){
								nodedb.UpStatus(false)
							}
							cancel()
							execErr = err
						}
						rsArr = append(rsArr, rs)
						return
					}else if rwType == node.HOST_READ{
						//for update
						if selectStmt,ok := pMainStmt.(*sqlparser.Select);ok{
							if len(selectStmt.Lock) > 0 {
								rs, err :=this.execTransactionTx(nodedb,sql,mctx)
								if err != nil{
									if this.needReconnect(err){
										nodedb.UpStatus(false)
									}
									cancel()
									execErr = err
								}
								rsArr = append(rsArr, rs)
							}
						}
					}
				}
				//
				glog.Query("Exec: ", sql)
				rs, err := nodedb.ExecContext(mctx, sql)
				if err != nil {
					if this.needReconnect(err){
						nodedb.UpStatus(false)
					}
					cancel()
					glog.Error(err)
					execErr = err
				}
				rsArr = append(rsArr, rs)
				return
			}(querySql, this, ctx,mainStmt)
		}
	}
	wg.Wait()
	if execErr != nil {
		return sqltypes.Result{}, execErr
	}
	//
	if mainStmt != nil {
		if selectStmt, ok := mainStmt.(*sqlparser.Select); ok {
			selectResult := hresult.NewSelectResult(selectStmt)
			selectResult.AddResult(rsArr...)
			rs, err := selectResult.BuildNewResult()
			return *rs, err
		}
	}
	//
	if len(rsArr) > 0 {
		affectedRows := uint64(0)
		lastId := uint64(0)
		for _, rs := range rsArr {
			affectedRows += rs.RowsAffected
			if rs.InsertID > lastId {
				lastId = rs.InsertID
			}
		}
		//
		rsArr[0].RowsAffected = affectedRows
		rsArr[0].InsertID = lastId
		//
		if lastId > 0 {
			this.lastInsertId = lastId
		}
		//
		return rsArr[0], execErr
	}
	return sqltypes.Result{}, fmt.Errorf("no result")
}

//
func (this *Connector)  execTransactionTx(nodedb *backend.Client,sql string, mctx context.Context) (sqltypes.Result, error) {
	dsn := nodedb.GetDSN()
	var execErr error
	this.mu.Lock()
	defer this.mu.Unlock()
	tx := this.getTransactionTx(dsn)
	if tx == nil{
		tx, execErr = nodedb.BeginContext(mctx)
		if execErr != nil{
			return sqltypes.Result{},execErr
		}
		this.addTransactionTx(dsn, tx)
	}
	return tx.ExecContext(mctx, sql)
}

//
func (this *Connector) needReconnect(err error) bool{
	if sqlerr, ok := err.(*mysql.SQLError); ok {
		if 	sqlerr.Num == mysql.CRServerGone || sqlerr.Num == mysql.CRServerLost{
			return true
		}
	}
	return false
}