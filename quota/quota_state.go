package quota

import (
	"sync"
	"time"
)

type QuotaState struct {
	LastUpdate    int64
	LastQuery     int64
	globalLimiter *TimeBaseLimiter
	isBybass      bool
	sync.Mutex
}

func NewQuotaState(updateTs int64) *QuotaState {
	return &QuotaState{
		LastUpdate:    updateTs,
		LastQuery:     0,
		globalLimiter: CreateQuotaLimiter(),
	}
}

func (qs *QuotaState) SetQuotes(quotas *Quotas) {
	if quotas.Bypass {
		qs.isBybass = quotas.Bypass
		return
	}
	if quotas.ReqNum != nil {
		qs.globalLimiter.SetFromTimedQuota(quotas.ReqNum, RequestNumber)
	}

	if quotas.ReqSize != nil {
		qs.globalLimiter.SetFromTimedQuota(quotas.ReqSize, RequestSize)
	}

	if quotas.ReadSize != nil {
		qs.globalLimiter.SetFromTimedQuota(quotas.ReadSize, ReadSize)
	}

	if quotas.ReadNum != nil {
		qs.globalLimiter.SetFromTimedQuota(quotas.ReadNum, ReadNumber)
	}
}

func (qs *QuotaState) Update(other QuotaState) {
	qs.Lock()
	defer qs.Unlock()
	qs.globalLimiter.Update(other.globalLimiter)
	qs.LastUpdate = other.LastUpdate
}

func (qs *QuotaState) SetLastQuery(lastQuery int64) {
	qs.Lock()
	qs.LastQuery = lastQuery
	qs.Unlock()
}

func (qs *QuotaState) GetGlobalLimiter() QuotaLimiter {
	qs.Lock()
	defer qs.Unlock()
	qs.LastQuery = time.Now().UnixNano() / int64(1e6)
	return qs.globalLimiter
}
