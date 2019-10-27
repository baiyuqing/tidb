package quota_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/quota"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
)

var _ = Suite(&testCacheSuite{})

type testCacheSuite struct {
	store  kv.Storage
	dbName string
	domain *domain.Domain
}

func (s *testCacheSuite) SetUpSuite(c *C) {
	store, err := mockstore.NewMockTikvStore()
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	s.domain, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testCacheSuite) TearDownSuit(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testCacheSuite) TestLoadQuotaTable(c *C) {
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	defer se.Close()
	mustExec(c, se, "use mysql;")
	mustExec(c, se, "truncate table quota;")

	q := quota.NewQuotaCache()
	err = q.LoadQuotaTable(se)
	c.Assert(err, IsNil)

	mustExec(c, se, `INSERT INTO mysql.quota (user_name, table_name, quota_op, trottle_type, quota_limit) VALUES ("root", "", 1, "read-size", 100)`)
	q = quota.NewQuotaCache()
	err = q.LoadQuotaTable(se)
	c.Assert(err, IsNil)
}

func (s *testCacheSuite) TestAddUserQuotas(c *C) {
	qt := &quota.Quotas{}
	qc := quota.NewQuotaCache()
	username := "test"
	qt.Set(quota.ReadNumber, 0, quota.SECONDS)
	qc.AddUserQuotas(username, qt)
	limit := qc.GetLimit4Test(username, quota.ReadNumber)
	c.Assert(limit, Equals, int64(0))
}

func mustExec(c *C, se session.Session, sql string) {
	_, err := se.Execute(context.Background(), sql)
	c.Assert(err, IsNil)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}
