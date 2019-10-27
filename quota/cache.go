package quota

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type QuotaCache struct {
	userQuotaCache  map[string]*QuotaState
	tableQuotaCache map[string]*QuotaState

	userCacheLock  sync.Mutex
	tableCacheLock sync.Mutex

	quitC   chan struct{}
	stopped bool
}

func NewQuotaCache() *QuotaCache {
	return &QuotaCache{
		userQuotaCache:  make(map[string]*QuotaState),
		tableQuotaCache: make(map[string]*QuotaState),
		quitC:           make(chan struct{}),
	}
}

func (qc *QuotaCache) Start() error {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				qc.refresh()
			case <-qc.quitC:
				return
			}
		}
	}()
	return nil
}

func (qc *QuotaCache) refresh() {
	// log.Printf("fresh!")
}

func (qc *QuotaCache) GetLimit4Test(user string, throttleType ThrottleType) int64 {
	return qc.GetUserQuotaState(user).globalLimiter.limiters[throttleType].GetLimit()
}

func (qc *QuotaCache) AddUserQuotas(user string, quotas *Quotas) error {
	qc.userCacheLock.Lock()
	defer qc.userCacheLock.Unlock()
	quotaState := NewQuotaState(time.Now().UnixNano() / int64(1e6))
	quotaState.SetQuotes(quotas)
	qc.userQuotaCache[user] = quotaState
	return nil
}

func (qc *QuotaCache) AddQuotas(user, table string, quotas *Quotas) {
	if user != "" {
		if quotaState := qc.GetUserQuotaState(user); quotaState != nil {
			quotaState.SetQuotes(quotas)
			qc.userQuotaCache[user] = quotaState
		} else {
			quotaState := NewQuotaState(time.Now().UnixNano() / int64(1e6))
			quotaState.SetQuotes(quotas)
			qc.userQuotaCache[user] = quotaState
		}
	}

	if table != "" {
		if quotaState := qc.GetTableQuotaState(user); quotaState != nil {
			quotaState.SetQuotes(quotas)
			qc.tableQuotaCache[table] = quotaState
		} else {
			quotaState := NewQuotaState(time.Now().UnixNano() / int64(1e6))
			quotaState.SetQuotes(quotas)
			qc.tableQuotaCache[table] = quotaState
		}
	}
}

func (qc *QuotaCache) AddTableQuotas(table string, quotas *Quotas) error {
	qc.tableCacheLock.Lock()
	defer qc.tableCacheLock.Unlock()
	quotaState := NewQuotaState(time.Now().UnixNano() / int64(1e6))
	quotaState.SetQuotes(quotas)
	qc.tableQuotaCache[table] = quotaState
	return nil
}

func (qc *QuotaCache) GetUserQuotaState(user string) *QuotaState {
	qc.userCacheLock.Lock()
	defer qc.userCacheLock.Unlock()
	return qc.userQuotaCache[user]
}

func (qc *QuotaCache) GetTableQuotaState(table string) *QuotaState {
	qc.tableCacheLock.Lock()
	defer qc.tableCacheLock.Unlock()
	return qc.tableQuotaCache[table]
}

func (qc *QuotaCache) GetUserLimiter(user string) QuotaLimiter {
	// qc.userCacheLock.Lock()
	// defer qc.userCacheLock.Unlock()
	userQuotaState := qc.GetUserQuotaState(user)
	if userQuotaState != nil {
		return userQuotaState.GetGlobalLimiter()
	}
	return nil
}

func (qc *QuotaCache) GetTableLimiter(table string) QuotaLimiter {
	// qc.userCacheLock.Lock()
	// defer qc.userCacheLock.Unlock()
	tableQuotaState := qc.GetTableQuotaState(table)
	if tableQuotaState != nil {
		return tableQuotaState.GetGlobalLimiter()
	}
	return nil
}

func (qc *QuotaCache) Stop() error {
	close(qc.quitC)
	return nil
}

func (qc *QuotaCache) LoadQuotaTable(ctx sessionctx.Context) error {
	sql := "SELECT * FROM mysql.quota"
	return qc.loadTable(ctx, sql, qc.decodeTableQuota)
}

func (qc *QuotaCache) loadTable(sctx sessionctx.Context, sql string,
	decodeTableRow func(chunk.Row, []*ast.ResultField) error) error {
	ctx := context.Background()
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return errors.Trace(err)
	}
	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	req := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err = decodeTableRow(row, fs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req = chunk.Renew(req, sctx.GetSessionVars().MaxChunkSize)
	}
}

func (qc *QuotaCache) decodeTableQuota(row chunk.Row, fs []*ast.ResultField) error {
	var username string
	var tableName string
	//var operationType OperationType
	var throttleType ThrottleType
	var quotaLimit int64
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "user_name":
			fmt.Println("user_name", row.GetString(i))
			username = row.GetString(i)
		case f.ColumnAsName.L == "table_name":
			fmt.Println("table_name", row.GetString(i))
			tableName = row.GetString(i)
		//case f.ColumnAsName.L == "quota_op":
		//	fmt.Println("quota_op", row.GetInt64(i))
		//	operationType = OperationType(int32(row.GetInt64(i)))
		case f.ColumnAsName.L == "trottle_type":
			fmt.Println("trottle_type", row.GetString(i))
			s := row.GetString(i)
			switch s {
			case "request-number":
				throttleType = RequestNumber
			case "request-size":
				throttleType = RequestSize
			case "read-number":
				throttleType = ReadNumber
			case "read-size":
				throttleType = ReadSize
			}
		case f.ColumnAsName.L == "quota_limit":
			fmt.Println("quota_limit", row.GetInt64(i))
			quotaLimit = row.GetInt64(i)
		}
	}
	qt := &Quotas{}
	qt.Set(throttleType, quotaLimit, QUARTERMINUTE)
	/*
		if username != "" {
			qc.AddUserQuotas(username, qt)
		}
		if tableName != "" {
			qc.AddTableQuotas(tableName, qt)
		}
	*/
	qc.AddQuotas(username, tableName, qt)
	return nil
}
