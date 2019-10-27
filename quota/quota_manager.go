package quota

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
)

var SkipCheckQuota = false

var _ Manager = (*QuotaManager)(nil)

type QuotaManager struct {
	*Handle
}

func (q *QuotaManager) CheckQuota(ctx sessionctx.Context, table string, amount int64, oper OperationType) error {
	var err error
	user := ctx.GetSessionVars().User.Username
	operQ := q.getQuota(user, table)
	if err = operQ.CheckQuota(&OperationReq{oper, amount}); err != nil {
		return err
	}
	ctx.SetValue(operQuotakey, operQ)

	return err
}

func (q *QuotaManager) CheckQuotaWithUser(ctx sessionctx.Context, table string, amount int64, oper OperationType) error {
	var err error
	user := ctx.GetSessionVars().User.Username
	operQ := q.getQuota(user, table)
	if err = operQ.CheckQuota(&OperationReq{oper, amount}); err != nil {
		return err
	}
	ctx.SetValue(operQuotakey, operQ)
	return err
}

func (q *QuotaManager) GetQuotaOperation(ctx sessionctx.Context) OperationQuota {
	if operQ, ok := ctx.Value(operQuotakey).(OperationQuota); ok {
		return operQ
	}
	return nil
}

func (qm *QuotaManager) AddUserQuota(user string, quotas *Quotas) error {
	quotaCache := qm.Get()
	quotaCache.AddUserQuotas(user, quotas)
	return nil
}

func (qm *QuotaManager) AddTableQuota(table string, quotas *Quotas) error {
	quotaCache := qm.Get()
	quotaCache.AddTableQuotas(table, quotas)
	return nil
}

func (qm *QuotaManager) getQuota(user, table string) OperationQuota {
	quotaCache := qm.Get()
	userLimiter := quotaCache.GetUserLimiter(user)
	tableLimiter := quotaCache.GetTableLimiter(table)
	limiters := []QuotaLimiter{}
	if userLimiter != nil && !userLimiter.IsBypass() {
		limiters = append(limiters, userLimiter)
	}
	if tableLimiter != nil && !tableLimiter.IsBypass() {
		limiters = append(limiters, tableLimiter)
	}
	if len(limiters) > 0 {
		return NewDefaultOperationQuota(limiters...)
	}
	return &NoopOperationQuota{}
}

func (q *QuotaManager) Start() error {
	return nil
}

func (q *QuotaManager) Stop() error {
	return nil
}

func (q *QuotaManager) LoadAll(ctx sessionctx.Context) error {

	return nil
}

// Handle wraps MySQLPrivilege providing thread safe access.
type Handle struct {
	qc atomic.Value
}

// NewHandle returns a Handle.
func NewHandle() *Handle {
	return &Handle{}
}

// Get the MySQLPrivilege for read.
func (h *Handle) Get() *QuotaCache {
	return h.qc.Load().(*QuotaCache)
}

// Update loads all the privilege info from kv storage.
func (h *Handle) Update(ctx sessionctx.Context) error {
	qc := NewQuotaCache()
	err := qc.LoadQuotaTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	h.qc.Store(qc)
	return nil
}
