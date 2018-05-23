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
)

const (
	EXECUTE_TIMEOUT = 30 // the timeout of total execute
)

//
type Connector struct {
	MyConn         *mysql.Conn
	DbName         string
	InTransaction  bool                 // turn on the transaction else
	transactionMap map[string]*mysql.Tx //[dsn]
	mu             *sync.Mutex
	// cancel is called after done
	cancel func()
	// ctx lives for the life of the Connector.
	ctx context.Context
	//  active Time
	lastActiveTime time.Time
	//
	extStmtQuerys []func(pStmt sqlparser.Statement,query string)(rs sqltypes.Result,err error,ok bool)
}

//just used for sys auto create table
func NewDefaultConnector() *Connector {
	return NewConnector(&mysql.Conn{})
}

//
func NewConnector(c *mysql.Conn) *Connector {
	conn := &Connector{
		MyConn: c,
		mu:     new(sync.Mutex),
		transactionMap:make(map[string]*mysql.Tx),
		lastActiveTime:time.Now(),
	}
	conn.ctx, conn.cancel = context.WithCancel(core.App().Context)
	//register ext function
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.showDatebases)
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.showTables)
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.showKeys)
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.showFields)
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.explain)
	conn.extStmtQuerys = append(conn.extStmtQuerys,conn.describe)
	//
	return conn;
}

//Verify database permissions
func (this *Connector) VerifyDatabaseAuth(dbName string) bool {
	dbs := this.MyConn.GetDatabases()
	if dbs == nil{
		return true
	}
	for _,db := range dbs{
		if db == "*" || db == dbName{
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
		go func(pTx *mysql.Tx, mctx context.Context){
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
		}(tx,ctx)
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
		go func(pTx *mysql.Tx, mctx context.Context){
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
		}(tx,ctx)
	}
	return execErr
}

//
func (this *Connector) clearTransactionTx() {
	this.transactionMap = make(map[string]*mysql.Tx)
}
//
func (this *Connector) addTransactionTx(dsn string, tx *mysql.Tx) {
	this.transactionMap[dsn] = tx
}

//
func (this *Connector) getTransactionTx(dsn string) *mysql.Tx {
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
func (this *Connector) UpActiveTime()  {
	this.lastActiveTime =time.Now()
}
//
func (this *Connector) GetLastActiveTime() time.Time  {
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
//
func (this *Connector) ComQuery(stmt sqlparser.Statement, query string) (sqltypes.Result, error) {
	rwType := node.HOST_WRITE
	switch nStmt := stmt.(type) {
	case *sqlparser.Select:
		rwType = node.HOST_READ
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
		if this.VerifyDatabaseAuth(dbName){
			this.UseDataBase(nStmt.DBName.String())
			return sqltypes.Result{RowsAffected: 1}, nil
		}
		return sqltypes.Result{},fmt.Errorf("Access denied for user '%s' to database '%s'",this.MyConn.User,dbName)
	//case *sqlparser.Show:
	//case *sqlparser.OtherRead: //explain
	case *sqlparser.Update,*sqlparser.Insert,*sqlparser.Delete:
	case *sqlparser.DDL:
		glog.Info("unKnow DDL, not support:", nStmt)
	default:
		var rs sqltypes.Result
		var err error
		var ok bool
		for _,f := range this.extStmtQuerys{
			rs,err,ok = f(stmt,query)
			if err != nil || ok{
				return rs, err
			}
		}
		glog.Info("unKnow, not support:", nStmt)
		return sqltypes.Result{}, nil
	}
	//
	dbName := this.GetDB()
	if len(dbName) <= 0{
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
		return this.execProxyPlan(db, stmt,query, rwType)
	} else if err != nil {
		return sqltypes.Result{}, err
	}
	return this.execSchemaPlans(stmt, plans, rwType)
}
//
func (this *Connector) Close() (err error){
	if len(this.transactionMap) > 0{
		err = this.TxRollback()
		this.clearTransactionTx()
	}
	if !this.MyConn.IsClosed(){
		this.MyConn.Close()
	}
	if this.cancel != nil{
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
		if len(stmt.From) > 1 || len(stmt.From) < 1 { // not support multiple table
			return nil, nil
		}
		tb, _ := this.getSchemaTable(stmt.From[0])
		if tb == nil {
			return nil, nil
		}
		//
		return select_plan.BuildSelectPlan(tb, stmt, core.App().GetRuleManager())
	case *sqlparser.Update:
		if len(stmt.TableExprs) > 1 || len(stmt.TableExprs) < 1 { // not support multiple table
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
		return insert_plan.BuildInsertPlan(tb, stmt, core.App().GetRuleManager(),this.GetDB())
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
		if stmt.From != nil{
			for _,from := range stmt.From{
				if expr,ok := from.(*sqlparser.AliasedTableExpr);ok{
					if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
						if !tbn.Qualifier.IsEmpty() && tbn.Qualifier.String() == this.GetDB(){
							newTb := tbn.ToViewName()
							if !tbn.Qualifier.IsEmpty(){
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
		if !tbn.IsEmpty() && tbn.Qualifier.String() == this.GetDB(){
			if !tbn.Qualifier.IsEmpty(){
				tbn.Qualifier = sqlparser.NewTableIdent(proxyDbName)
			}
		}
		stmt.Table = tbn
	case *sqlparser.Update:
		if stmt.TableExprs != nil{
			for _,tbExpr := range stmt.TableExprs{
				if expr,ok := tbExpr.(*sqlparser.AliasedTableExpr);ok{
					if tbn, ok := expr.Expr.(sqlparser.TableName); ok {
						if !tbn.Qualifier.IsEmpty() && tbn.Qualifier.String() == this.GetDB(){
							newTb := tbn.ToViewName()
							if !tbn.Qualifier.IsEmpty(){
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
	return myClient.Exec(query)
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
			go func(sql string, this *Connector, mctx context.Context) {
				defer wg.Done()
				select {
				case <-mctx.Done():
					execErr = mctx.Err()
					return
				default:
				}
				//transaction, just enable for write only
				if this.InTransaction && rwType == node.HOST_WRITE {
					dsn := nodedb.GetDSN()
					//
					this.mu.Lock()
					tx := this.getTransactionTx(dsn)
					if tx == nil {
						tx, execErr = nodedb.BeginContext(mctx)
						if execErr != nil {
							this.mu.Unlock()
							cancel()
							return
						}
						this.addTransactionTx(dsn, tx)
					}
					this.mu.Unlock()
					//
					rs, err := tx.ExecContext(mctx, sql)
					if err != nil {
						cancel()
						execErr = err
					}
					rsArr = append(rsArr, rs)
					return
				}
				//
				glog.Query("Exec: ", sql)
				rs, err := nodedb.ExecContext(mctx, sql)
				if err != nil {
					cancel()
					glog.Error(err)
					execErr = err
				}
				rsArr = append(rsArr, rs)
				return
			}(querySql, this, ctx)
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
		return rsArr[0], execErr
	}
	return sqltypes.Result{}, fmt.Errorf("no result")
}
