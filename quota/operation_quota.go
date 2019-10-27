package quota

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

type OperationType uint32

const (
	READ OperationType = iota
	WRITE
)

type OperationReq struct {
	T      OperationType
	Amount int64
}

type OperationQuotaKey int

const operQuotakey OperationQuotaKey = 0

func (k OperationQuotaKey) String() string {
	return "operation-quota-key"
}

type OperationQuota interface {
	CheckQuota(req *OperationReq) error
	// CheckQuota(numWrites, numReads int64) error
	Close()
	AddReadResult(result interface{})
	AddWriteResult(result interface{})
	AddResult(opt OperationType, result interface{})
	GetAvailable(opt OperationType) map[ThrottleType]int64
	GetReadAvailable() int64
	GetWriteAvailable() int64
}

type DefaultOperationQuota struct {
	limiters       []QuotaLimiter
	writeConsumed  int64
	readConsumed   int64
	writeAvailable int64
	readAvailable  int64

	operationSize     map[ThrottleType]int64
	resourceConsumed  map[ThrottleType]int64
	resourceAvailable map[ThrottleType]int64

	oper OperationType

	sync.Mutex
}

func NewDefaultOperationQuota(limiters ...QuotaLimiter) *DefaultOperationQuota {
	oper := &DefaultOperationQuota{
		resourceConsumed:  make(map[ThrottleType]int64),
		resourceAvailable: make(map[ThrottleType]int64),
		operationSize:     make(map[ThrottleType]int64),
	}
	if len(limiters) > 0 {
		oper.limiters = limiters
	} else {
		oper.limiters = make([]QuotaLimiter, 0)
	}
	return oper
}

func (o *DefaultOperationQuota) CheckQuota(operReq *OperationReq) error {
	// o.writeConsumed = estimateConsume(WRITE, , 100)
	// o.readConsumed = estimateConsume(READ, numReads, 100)

	// if _, ok := o.resourceConsumed[req.T]; ok {
	quotaReqs, tts := Convert2Throttle(operReq)
	// log.Printf("tts %+v\n", tts)
	for _, qr := range quotaReqs {
		// o.resourceConsumed[qr.Throttle] = estimateConsume(qr.Throttle, qr.Amount, 100)
		o.resourceConsumed[qr.Throttle] = qr.Amount

		o.resourceAvailable[qr.Throttle] = math.MaxInt64
		o.operationSize[qr.Throttle] = 0
	}

	// o.writeAvailable = math.MaxInt64
	// o.readAvailable = math.MaxInt64

	for _, limiter := range o.limiters {
		if limiter.IsBypass() {
			continue
		}

		if err := limiter.CheckQuota(quotaReqs...); err != nil {
			throttleErr := err.(ThrottlingLimitedError)
			throttleErr.OperType = operReq.T
			return throttleErr
		}
		for _, tt := range tts {
			o.resourceAvailable[tt] = min(o.resourceAvailable[tt], limiter.GetAvailable(tt))
		}

		// if err := limiter.CheckQuota(o.writeConsumed, o.readConsumed); err != nil {
		// 	return err
		// }

		// o.writeAvailable = min(o.writeAvailable, limiter.GetWriteAvailable())
		// o.readAvailable = min(o.readAvailable, limiter.GetReadAvailable())

	}
	for _, limiter := range o.limiters {
		for _, tt := range tts {
			limiter.GrabQuota(tt, o.resourceConsumed[tt])
		}
	}
	return nil
}

func (o *DefaultOperationQuota) Close() {
	o.Lock()
	for tt := range o.operationSize {
		resourceDiff := o.operationSize[tt] - o.resourceConsumed[tt]
		for _, limiter := range o.limiters {
			if resourceDiff != 0 {
				fmt.Printf("[Close] %s %v\n", tt, resourceDiff)
				limiter.Consume(tt, resourceDiff)
			}
		}
	}
	o.Unlock()
	// writeDiff := o.operationSize[WRITE] - o.writeConsumed
	// readDiff := o.operationSize[READ] - o.readConsumed

	// for _, limiter := range o.limiters {
	// 	if writeDiff != 0 {
	// 		limiter.ConsumeWrite(writeDiff)
	// 	}
	// 	if readDiff != 0 {
	// 		limiter.ConsumeRead(readDiff)
	// 	}
	// }
}

func Convert2Throttle(req *OperationReq) ([]*QuotaRequest, []ThrottleType) {
	qreqs := []*QuotaRequest{}
	tts := []ThrottleType{}
	switch req.T {
	case READ:
		qreqs = append(qreqs, &QuotaRequest{RequestNumber, 1})
		qreqs = append(qreqs, &QuotaRequest{RequestSize, req.Amount})
		qreqs = append(qreqs, &QuotaRequest{ReadNumber, 1})
		qreqs = append(qreqs, &QuotaRequest{ReadSize, req.Amount})

		tts = append(tts, RequestNumber, RequestSize, ReadNumber, ReadSize)

	case WRITE:
		qreqs = append(qreqs, &QuotaRequest{RequestNumber, 1}, &QuotaRequest{RequestSize, req.Amount})
		tts = append(tts, RequestNumber, RequestSize)
	}
	return qreqs, tts
}

func (o *DefaultOperationQuota) AddResult(oper OperationType, result interface{}) {
	o.Lock()
	// calc result data
	switch oper {
	case READ:
		switch result.(type) {
		case kv.ResultSubset:
			resultSet := result.(kv.ResultSubset)
			details := resultSet.GetExecDetails()
			affectRows := details.TotalKeys
			respDataSize := len(resultSet.GetData())
			o.operationSize[RequestNumber] = affectRows
			o.operationSize[ReadNumber] = affectRows
			o.operationSize[RequestSize] = int64(respDataSize)
			o.operationSize[ReadSize] = int64(respDataSize)
			logutil.Logger(context.Background()).Info("add result rows & data",
				zap.Int64("affectRows", affectRows), zap.Int("respDataSize", respDataSize))
			fmt.Printf("affectRows: %v  processKey %v respDataSize: %v \n", affectRows, respDataSize)
		case *tipb.SelectResponse:
			selectResp := result.(*tipb.SelectResponse)
			affectRows := int64(len(selectResp.Rows))
			respDataSize := int64(selectResp.Size())
			o.operationSize[RequestNumber] = affectRows
			o.operationSize[ReadNumber] = affectRows
			o.operationSize[RequestSize] = respDataSize
			o.operationSize[ReadSize] = respDataSize
			logutil.Logger(context.Background()).Info("add result rows & data",
				zap.Int64("affectRows", affectRows), zap.Int64("respDataSize", respDataSize))
			fmt.Printf("affectRows: %v  processKey %v respDataSize: %v \n", affectRows, respDataSize)
		}

	case WRITE:
		reqs, tts := Convert2Throttle(&OperationReq{oper, 0})
		for i, tt := range tts {
			o.operationSize[tt] = reqs[i].Amount
		}
	}
	o.Unlock()
}

func (o *DefaultOperationQuota) GetAvailable(opt OperationType) map[ThrottleType]int64 {
	return o.resourceAvailable
}

func (o *DefaultOperationQuota) AddReadResult(result interface{}) {
	o.AddResult(READ, result)
}

func (o *DefaultOperationQuota) AddWriteResult(result interface{}) {
	o.AddResult(WRITE, result)
}

func (o *DefaultOperationQuota) GetReadAvailable() int64 {
	return math.MaxInt64
}
func (o *DefaultOperationQuota) GetWriteAvailable() int64 {
	return math.MaxInt64
}

func estimateConsume(tt ThrottleType, numReqs int64, avgSize int64) int64 {
	if numReqs > 0 {
		return avgSize * numReqs
	}
	return 0
}

type NoopOperationQuota struct {
}

func (o *NoopOperationQuota) CheckQuota(req *OperationReq) error {
	return nil
}

// func (o *NoopOperationQuota) CheckQuota(opt OperationType, amount int64) error {
// 	return nil
// }

func (o *NoopOperationQuota) Close() {
}
func (o *NoopOperationQuota) AddResult(oper OperationType, result interface{}) {

}
func (o *NoopOperationQuota) GetReadAvailable() int64 {
	return math.MaxInt64
}
func (o *NoopOperationQuota) GetWriteAvailable() int64 {
	return math.MaxInt64
}

func (o *NoopOperationQuota) AddReadResult(result interface{}) {
}

func (o *NoopOperationQuota) AddWriteResult(result interface{}) {

}

func (o *NoopOperationQuota) GetAvailable(opt OperationType) map[ThrottleType]int64 {
	_, tts := Convert2Throttle(&OperationReq{opt, 0})
	results := make(map[ThrottleType]int64)
	for _, tt := range tts {
		results[tt] = math.MaxInt64
	}
	return results
}
