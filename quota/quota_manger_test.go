package quota

import (
	"fmt"
	"testing"
)

func createManager() *QuotaManager {
	qm := &QuotaManager{
		&Handle{},
	}
	cache := NewQuotaCache()
	fmt.Printf("%p\n", cache)
	qm.Handle.qc.Store(cache)
	return qm
}

func TestAddTableQuota(t *testing.T) {
	qm := createManager()
	quotas := &Quotas{}

	tt, limit, tunit, _ := ParseLimit("10req/sec")
	quotas.Set(ReadNumber,  limit, tunit )
	tt, limit, tunit, _ = ParseLimit("10K/sec")
	quotas.Set(ReadSize, limit, tunit)
	quotas.Set(tt, limit, tunit)

	table := "test_table"
	qm.AddTableQuota("test_table", quotas)
	cache := qm.Handle.Get()
	fmt.Printf("%p\n", cache)
 	vlimit := cache.tableQuotaCache[table].globalLimiter.limiters[ReadNumber].GetLimit()
 	if vlimit != 10 {
 		t.Errorf("read-number limit error %v\n", vlimit)
    }

	vlimit = cache.tableQuotaCache[table].globalLimiter.limiters[ReadSize].GetLimit()
	if vlimit != 10 {
		t.Errorf("read-size limit error %v\n", vlimit)
	}

}

func TestAddUserQuota(t *testing.T) {

}
