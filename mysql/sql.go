package mysql

import (
	"github.com/sgoby/sqlparser/sqltypes"
	"sync"
	"context"
	"database/sql/driver"
	"time"
	"errors"
	"fmt"
)
var nowFunc = time.Now
var putConnHook func(*Client, *driverConn)
// This is the size of the connectionOpener request chan (DB.openerCh).
// This value should be larger than the maximum typical value
// used for db.maxOpen. If maxOpen is significantly larger than
// connectionRequestQueueSize then it is possible for ALL calls into the *DB
// to block until the connectionOpener can satisfy the backlog of requests.
var connectionRequestQueueSize = 1000000
//
var errDBClosed = errors.New("sql: database is closed")
const debugGetPut = false
const defaultMaxIdleConns = 2
const (
	// alwaysNewConn forces a new connection to the database.
	alwaysNewConn connReuseStrategy = iota
	// cachedOrNewConn returns a cached connection, if available, else waits
	// for one to become available (if MaxOpenConns has been reached) or
	// creates a new database connection.
	cachedOrNewConn
)
// maxBadConnRetries is the number of maximum retries if the driver returns
// driver.ErrBadConn to signal a broken connection before forcing a new
// connection to be opened.
const maxBadConnRetries = 2

// Various isolation levels that drivers may support in BeginTx.
// If a driver does not support a given isolation level an error may be returned.
//
// See https://en.wikipedia.org/wiki/Isolation_(database_systems)#Isolation_levels.
const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)





type connReuseStrategy uint8

type Client struct {
	//
	connParams *ConnParams
	//
	dsn    string
	// numClosed is an atomic counter which represents a total number of
	// closed connections. Stmt.openStmt checks it before cleaning closed
	// connections in Stmt.css.
	numClosed uint64

	mu           sync.Mutex // protects following fields
	freeConn     []*driverConn
	connRequests map[uint64]chan connRequest
	nextRequest  uint64 // Next key to use in connRequests.
	numOpen      int    // number of opened and pending open connections
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during db.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh    chan struct{}
	closed      bool
	//dep         map[finalCloser]depSet  //prepared statement
	lastPut     map[*driverConn]string // stacktrace of last conn's put; debug only
	maxIdle     int                    // zero means defaultMaxIdleConns; negative means 0
	maxOpen     int                    // <= 0 means unlimited
	maxLifetime time.Duration          // maximum amount of time a connection may be reused
	cleanerCh   chan struct{}
}
// connRequest represents one request for a new connection
// When there are no idle connections available, DB.conn will create
// a new connRequest and put it on the db.connRequests list.
type connRequest struct {
	conn *driverConn
	err  error
}
//==========================================================================
//
func Open(pConnParams *ConnParams,dataSourceName string)(*Client,error){
	db := &Client{
		dsn:          dataSourceName,
		connParams:   pConnParams,
		openerCh:     make(chan struct{}, connectionRequestQueueSize),
		lastPut:      make(map[*driverConn]string),
		connRequests: make(map[uint64]chan connRequest),
	}
	go db.connectionOpener()
	return db, nil
}
//
func (this *Client) GetDSN() string {
	if len(this.dsn) > 0{
		return this.dsn
	}
	return this.connParams.ToDSN()
}
// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (this *Client) PingContext(ctx context.Context) error {
	var dc *driverConn
	var err error

	for i := 0; i < maxBadConnRetries; i++ {
		dc, err = this.conn(ctx, cachedOrNewConn)
		if err != driver.ErrBadConn {
			break
		}
	}
	if err == driver.ErrBadConn {
		dc, err = this.conn(ctx, alwaysNewConn)
	}
	if err != nil {
		return err
	}

	return this.pingDC(ctx, dc, dc.releaseConn)
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (this *Client) Ping() error {
	return this.PingContext(context.Background())
}
// Close closes the database, releasing any open resources.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (this *Client) Close() error {
	this.mu.Lock()
	if this.closed { // Make DB.Close idempotent
		this.mu.Unlock()
		return nil
	}
	close(this.openerCh)
	if this.cleanerCh != nil {
		close(this.cleanerCh)
	}
	var err error
	fns := make([]func() error, 0, len(this.freeConn))
	for _, dc := range this.freeConn {
		fns = append(fns, dc.closeDBLocked())
	}
	this.freeConn = nil
	this.closed = true
	for _, req := range this.connRequests {
		close(req)
	}
	this.mu.Unlock()
	for _, fn := range fns {
		err1 := fn()
		if err1 != nil {
			err = err1
		}
	}
	return err
}
//
func (this *Client)  Exec(query string, args ...interface{}) (sqltypes.Result, error){
	return this.ExecContext(context.Background(), query, args...)
	//return sqltypes.Result{},nil
}
//
func (this *Client) UseDB(dbName string) error {
	_, err := this.Exec("use "+dbName)
	return err
}
/*
//
func (this *Client) Begin() error {
	_, err := this.Exec("begin")
	return err
}
//
func (this *Client) Commit() error {
	_, err := this.Exec("commit")
	return err
}
//
func (this *Client) Rollback() error {
	_, err := this.Exec("rollback")
	return err
}
*/
//
func (this *Client) SetAutoCommit(n uint8) error {
	return nil
}
//
func (this *Client) SetCharset(charset string) error {
   return nil
}
//
func (this *Client) SetMaxLifeTime(ts int){
	this.maxLifetime = time.Duration(ts) * time.Second
}
// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (this *Client) SetMaxOpenConns(n int) {
	this.mu.Lock()
	this.maxOpen = n
	if n < 0 {
		this.maxOpen = 0
	}
	syncMaxIdle := this.maxOpen > 0 && this.maxIdleConnsLocked() > this.maxOpen
	this.mu.Unlock()
	if syncMaxIdle {
		this.SetMaxIdleConns(n)
	}
}
// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit
//
// If n <= 0, no idle connections are retained.
func (this *Client) SetMaxIdleConns(n int) {
	this.mu.Lock()
	if n > 0 {
		this.maxIdle = n
	} else {
		// No idle connections.
		this.maxIdle = -1
	}
	// Make sure maxIdle doesn't exceed maxOpen
	if this.maxOpen > 0 && this.maxIdleConnsLocked() > this.maxOpen {
		this.maxIdle = this.maxOpen
	}
	var closing []*driverConn
	idleCount := len(this.freeConn)
	maxIdle := this.maxIdleConnsLocked()
	if idleCount > maxIdle {
		closing = this.freeConn[maxIdle:]
		this.freeConn = this.freeConn[:maxIdle]
	}
	this.mu.Unlock()
	for _, c := range closing {
		c.Close()
	}
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (this *Client) ExecContext(ctx context.Context, query string, args ...interface{}) (sqltypes.Result, error) {
	var res sqltypes.Result
	var err error
	for i := 0; i < maxBadConnRetries; i++ {
		res, err = this.exec(ctx, query, args, cachedOrNewConn)
		if err != driver.ErrBadConn {
			break
		}
	}
	if err == driver.ErrBadConn {
		return this.exec(ctx, query, args, alwaysNewConn)
	}
	return res, err
}
//======================================================
func (this *Client) exec(ctx context.Context, query string, args []interface{}, strategy connReuseStrategy) (sqltypes.Result, error) {
	dc, err := this.conn(ctx, strategy)
	if err != nil {
		return sqltypes.Result{}, err
	}
	return this.execDC(ctx, dc, dc.releaseConn, query, args)
}

func (this *Client) execDC(ctx context.Context, dc *driverConn, release func(error), query string, args []interface{}) (res sqltypes.Result, err error) {
	defer func() {
		release(err)
	}()
	withLock(dc, func() {
		result,ExecuErr :=  dc.ci.ExecuteFetch(query,0,true)
		if ExecuErr == nil{
			res = *result
		}
		err = ExecuErr
	})
	return res,err
}
func (this *Client) query(ctx context.Context, query string, args []interface{}, strategy connReuseStrategy) (sqltypes.Result, error) {
	dc, err := this.conn(ctx, strategy)
	if err != nil {
		return sqltypes.Result{}, err
	}
	//====================
	defer func() {
		dc.releaseConn(err)
	}()
	rs := sqltypes.Result{}
	withLock(dc, func() {
		result,err :=  dc.ci.ExecuteFetch(query,0,false)
		if err == nil{
			rs = *result
		}
	})
	return rs,err
}
// queryDC executes a query on the given connection.
// The connection gets released by the releaseConn function.
// The ctx context is from a query method and the txctx context is from an
// optional transaction context.
func (this *Client) queryDC(ctx, txctx context.Context, dc *driverConn, releaseConn func(error), query string, args []interface{}) (*sqltypes.Result, error) {
	return nil,nil
}

// conn returns a newly-opened or cached *driverConn.
func (this *Client) conn(ctx context.Context, strategy connReuseStrategy) (*driverConn, error) {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return nil, errDBClosed
	}
	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		this.mu.Unlock()
		return nil, ctx.Err()
	}
	lifetime := this.maxLifetime
	// Prefer a free connection, if possible.
	numFree := len(this.freeConn)
	if strategy == cachedOrNewConn && numFree > 0 {
		conn := this.freeConn[0]
		copy(this.freeConn, this.freeConn[1:])
		this.freeConn = this.freeConn[:numFree-1]
		conn.inUse = true
		this.mu.Unlock()
		if conn.expired(lifetime) {
			conn.Close()
			return nil, driver.ErrBadConn
		}
		return conn, nil
	}
	// Out of free connections or we were asked not to use one. If we're not
	// allowed to open any more connections, make a request and wait.
	if this.maxOpen > 0 && this.numOpen >= this.maxOpen {
		// Make the connRequest channel. It's buffered so that the
		// connectionOpener doesn't block while waiting for the req to be read.
		req := make(chan connRequest, 1)
		reqKey := this.nextRequestKeyLocked()
		this.connRequests[reqKey] = req
		this.mu.Unlock()

		// Timeout the connection request with the context.
		select {
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			this.mu.Lock()
			delete(this.connRequests, reqKey)
			this.mu.Unlock()
			select {
			default:
			case ret, ok := <-req:
				if ok {
					this.putConn(ret.conn, ret.err)
				}
			}
			return nil, ctx.Err()
		case ret, ok := <-req:
			if !ok {
				return nil, errDBClosed
			}
			if ret.err == nil && ret.conn.expired(lifetime) {
				ret.conn.Close()
				return nil, driver.ErrBadConn
			}
			return ret.conn, ret.err
		}
	}
	//
	this.numOpen++ // optimistically
	this.mu.Unlock()
	//func Connect(ctx context.Context, params *ConnParams) (*Conn, error) { // client.go
	ci, err := Connect(ctx, this.connParams)
	if err != nil {
		this.mu.Lock()
		this.numOpen-- // correct for earlier optimism
		//this.maybeOpenNewConnections()
		this.mu.Unlock()
		return nil, err
	}
	this.mu.Lock()
	dc := &driverConn{
		db:        this,
		createdAt: nowFunc(),
		ci:        *ci,
		inUse:     true,
	}
	//this.addDepLocked(dc, dc)
	this.mu.Unlock()
	return dc, nil
}
// putConn adds a connection to the db's free pool.
// err is optionally the last error that occurred on this connection.
func (this *Client) putConn(dc *driverConn, err error) {
	this.mu.Lock()
	if !dc.inUse {
		if debugGetPut {
			fmt.Printf("putConn(%v) DUPLICATE was: %s\n\nPREVIOUS was: %s", dc, stack(), this.lastPut[dc])
		}
		panic("sql: connection returned that was never out")
	}
	if debugGetPut {
		this.lastPut[dc] = stack()
	}
	dc.inUse = false

	for _, fn := range dc.onPut {
		fn()
	}
	dc.onPut = nil

	if err == driver.ErrBadConn {
		// Don't reuse bad connections.
		// Since the conn is considered bad and is being discarded, treat it
		// as closed. Don't decrement the open count here, finalClose will
		// take care of that.
		this.maybeOpenNewConnections()
		this.mu.Unlock()
		dc.Close()
		return
	}
	if putConnHook != nil {
		putConnHook(this, dc)
	}
	added := this.putConnDBLocked(dc, nil)
	this.mu.Unlock()

	if !added {
		dc.Close()
	}
}
// Satisfy a connRequest or put the driverConn in the idle pool and return true
// or return false.
// putConnDBLocked will satisfy a connRequest if there is one, or it will
// return the *driverConn to the freeConn list if err == nil and the idle
// connection limit will not be exceeded.
// If err != nil, the value of dc is ignored.
// If err == nil, then dc must not equal nil.
// If a connRequest was fulfilled or the *driverConn was placed in the
// freeConn list, then true is returned, otherwise false is returned.
func (this *Client) putConnDBLocked(dc *driverConn, err error) bool {
	if this.closed {
		return false
	}
	if this.maxOpen > 0 && this.numOpen > this.maxOpen {
		return false
	}
	if c := len(this.connRequests); c > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range this.connRequests {
			break
		}
		delete(this.connRequests, reqKey) // Remove from pending requests.
		if err == nil {
			dc.inUse = true
		}
		req <- connRequest{
			conn: dc,
			err:  err,
		}
		return true
	} else if err == nil && !this.closed && this.maxIdleConnsLocked() > len(this.freeConn) {
		this.freeConn = append(this.freeConn, dc)
		this.startCleanerLocked()
		return true
	}
	return false
}

// startCleanerLocked starts connectionCleaner if needed.
func (this *Client) startCleanerLocked() {
	if this.maxLifetime > 0 && this.numOpen > 0 && this.cleanerCh == nil {
		this.cleanerCh = make(chan struct{}, 1)
		go this.connectionCleaner(this.maxLifetime)
	}
}
//
func (this *Client) maxIdleConnsLocked() int {
	n := this.maxIdle
	switch {
	case n == 0:
		// TODO(bradfitz): ask driver, if supported, for its default preference
		return defaultMaxIdleConns
	case n < 0:
		return 0
	default:
		return n
	}
}
//
// nextRequestKeyLocked returns the next connection request key.
// It is assumed that nextRequest will not overflow.
func (this *Client) nextRequestKeyLocked() uint64 {
	next := this.nextRequest
	this.nextRequest++
	return next
}
//
// Assumes db.mu is locked.
// If there are connRequests and the connection limit hasn't been reached,
// then tell the connectionOpener to open new connections.
func (this *Client) maybeOpenNewConnections() {
	numRequests := len(this.connRequests)
	if this.maxOpen > 0 {
		numCanOpen := this.maxOpen - this.numOpen
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		this.numOpen++ // optimistically
		numRequests--
		if this.closed {
			return
		}
		this.openerCh <- struct{}{}
	}
}

func (this *Client) connectionCleaner(d time.Duration) {
	const minInterval = time.Second

	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-this.cleanerCh: // maxLifetime was changed or db was closed.
		}

		this.mu.Lock()
		d = this.maxLifetime
		if this.closed || this.numOpen == 0 || d <= 0 {
			this.cleanerCh = nil
			this.mu.Unlock()
			return
		}

		expiredSince := nowFunc().Add(-d)
		var closing []*driverConn
		for i := 0; i < len(this.freeConn); i++ {
			c := this.freeConn[i]
			if c.createdAt.Before(expiredSince) {
				closing = append(closing, c)
				last := len(this.freeConn) - 1
				this.freeConn[i] = this.freeConn[last]
				this.freeConn[last] = nil
				this.freeConn = this.freeConn[:last]
				i--
			}
		}
		this.mu.Unlock()

		for _, c := range closing {
			c.Close()
		}

		if d < minInterval {
			d = minInterval
		}
		t.Reset(d)
	}
}
// Runs in a separate goroutine, opens new connections when requested.
func (this *Client) connectionOpener() {
	for range this.openerCh {
		this.openNewConnection()
	}
}
// Open one new connection
func (this *Client) openNewConnection() {
	// maybeOpenNewConnctions has already executed db.numOpen++ before it sent
	// on db.openerCh. This function must execute db.numOpen-- if the
	// connection fails or is closed before returning.
	ctx := context.Background()
	ci, err := Connect(ctx, this.connParams)//db.driver.Open(db.dsn)
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closed {
		if err == nil {
			ci.Close()
		}
		this.numOpen--
		return
	}
	if err != nil {
		this.numOpen--
		this.putConnDBLocked(nil, err)
		this.maybeOpenNewConnections()
		return
	}
	dc := &driverConn{
		db:        this,
		createdAt: nowFunc(),
		ci:        *ci,
	}
	if this.putConnDBLocked(dc, err) {
		//this.addDepLocked(dc, dc)
	} else {
		this.numOpen--
		ci.Close()
	}
}
func (this *Client) pingDC(ctx context.Context, dc *driverConn, release func(error)) error {
	var err error
	withLock(dc, func() {
		err =  dc.ci.Ping(ctx)
	})
	release(err)
	return err
}

//====================================================================================================
// BeginTx starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (this *Client) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error) {
	var tx *Tx
	var err error
	for i := 0; i < maxBadConnRetries; i++ {
		tx, err = this.begin(ctx, opts, cachedOrNewConn)
		if err != driver.ErrBadConn {
			break
		}
	}
	if err == driver.ErrBadConn {
		return this.begin(ctx, opts, alwaysNewConn)
	}
	return tx, err
}

func (this *Client) BeginContext(ctx context.Context) (*Tx, error) {
	return this.BeginTx(ctx, nil)
}
// Begin starts a transaction. The default isolation level is dependent on
// the driver.
func (this *Client) Begin() (*Tx, error) {
	return this.BeginTx(context.Background(), nil)
}

func (this *Client) begin(ctx context.Context, opts *TxOptions, strategy connReuseStrategy) (tx *Tx, err error) {
	dc, err := this.conn(ctx, strategy)
	if err != nil {
		return nil, err
	}
	return this.beginDC(ctx, dc, dc.releaseConn, opts)
}

// beginDC starts a transaction. The provided dc must be valid and ready to use.
func (this *Client) beginDC(ctx context.Context, dc *driverConn, release func(error), opts *TxOptions) (tx *Tx, err error) {
	var txi driver.Tx
	withLock(dc, func() {
		txi, err = ctxDriverBegin(ctx, opts, dc.ci)
	})
	if err != nil {
		release(err)
		return nil, err
	}

	// Schedule the transaction to rollback when the context is cancelled.
	// The cancel function in Tx will be called after done is set to true.
	ctx, cancel := context.WithCancel(ctx)
	tx = &Tx{
		db:          this,
		dc:          dc,
		releaseConn: release,
		txi:         txi,
		cancel:      cancel,
		ctx:         ctx,
	}
	go tx.awaitDone()
	return tx, nil
}