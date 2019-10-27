package quota

import (
	"github.com/pingcap/tidb/sessionctx"
)

type keyType int

func (k keyType) String() string {
	return "quota-key"
}

// Manager is the interface for providing privilege related operations.
type Manager interface {

	// CheckQuota
	CheckQuota(ctx sessionctx.Context, table string, amount int64, oper OperationType) error
	// amount 可以先传 key-range
	CheckQuotaWithUser(ctx sessionctx.Context, table string, amount int64, oper OperationType) error
	GetQuotaOperation(ctx sessionctx.Context) OperationQuota
}

const key keyType = 0

// BindQuotaManager binds Manager to context.
func BindQuotaManager(ctx sessionctx.Context, pc Manager) {
	ctx.SetValue(key, pc)
}

// GetQuotaManager gets Checker from context.
func GetQuotaManager(ctx sessionctx.Context) Manager {
	if v, ok := ctx.Value(key).(Manager); ok {
		return v
	}
	return nil
}
