package quota

import "math"

type QuotaRequest struct {
	Throttle ThrottleType
	Amount   int64
}

type QuotaLimiter interface {
	// CheckQuota(estimateWriteSize, estimateReadSize int64) error
	CheckQuota(reqs ...*QuotaRequest) error
	/**
	 * Removes the specified write and read amount from the quota.
	 * At this point the write and read amount will be an estimate,
	 * that will be later adjusted with a consumeWrite()/consumeRead() call.
	 */
	// GrabQuota(writeSize, readSize int64)
	// GrabQuota(reqs ...*QuotaRequest)
	GrabQuota(tt ThrottleType, amount int64)

	ConsumeWrite(size int64)
	ConsumeRead(size int64)
	// Consume(reqs ...*QuotaRequest)
	Consume(tt ThrottleType, amount int64)
	GetAvailable(tt ThrottleType) int64
	IsBypass() bool
	GetReadAvailable() int64
	GetWriteAvailable() int64
}

func CreateQuotaLimiter() *TimeBaseLimiter {
	// if quotas.Bypass {
	// 	return &NoopQuotaLimiter{}
	// }
	limiter := NewTimeBaseLimiter()
	limiter.limiters[RequestNumber] = NewFixedIntervalRateLimiter()
	limiter.limiters[RequestSize] = NewFixedIntervalRateLimiter()
	limiter.limiters[ReadNumber] = NewFixedIntervalRateLimiter()
	limiter.limiters[ReadSize] = NewFixedIntervalRateLimiter()
	return limiter
}

type TimeBaseLimiter struct {
	limiters map[ThrottleType]RateLimiter
	// reqsLimiter    RateLimiter
	// reqSizeLimiter RateLimiter
}

func NewTimeBaseLimiter() *TimeBaseLimiter {
	return &TimeBaseLimiter{
		limiters: make(map[ThrottleType]RateLimiter),
	}
}

func (tl *TimeBaseLimiter) SetFromTimedQuota(timedQuota *TimedQuota, t ThrottleType) {
	if _, ok := tl.limiters[t]; ok {
		tl.limiters[t].Set(timedQuota.SoftLimit, timedQuota.TimeUnit)
	}
}

func (tl *TimeBaseLimiter) Update(other *TimeBaseLimiter) {
	for tt, limiter := range other.limiters {
		if _, ok := tl.limiters[tt]; ok {
			tl.limiters[tt].Update(limiter)
		} else {
			tl.limiters[tt] = limiter
		}
	}
}

// func (tl *TimeBaseLimiter) CheckQuota(throttle ThrottleType, estimateSize int64) error {
func (tl *TimeBaseLimiter) CheckQuota(quotareqs ...*QuotaRequest) error {
	for _, req := range quotareqs {
		rl, ok := tl.limiters[req.Throttle]
		if !ok {
			continue
		}
		if !rl.CanExcute(req.Amount) {
			return ThrottlingLimitedError{
				Throttle: req.Throttle,
				Limit:    rl.GetLimit(),
				Current:  req.Amount,
			}
		}
	}

	// if !tl.reqsLimiter.CanExcute(1) {

	// }
	// if !tl.reqSizeLimiter.CanExcute(estimateReadSize + estimateWriteSize) {
	// 	return ErrThrottlingLimitExceed
	// }
	return nil
}

func (tl *TimeBaseLimiter) Consume(tt ThrottleType, amount int64) {
	// tl.reqsLimiter.Consume(1)
	// tl.reqSizeLimiter.Consume(size)
	// for _, req := range reqs {
	// 	if rl, ok := tl.limiters[req.Throttle]; ok {
	// 		rl.Consume(req.Amount)
	// 	}
	// 	// tl.limiters[req.Throttle].Consume(req.Amount)
	// }
	if rl, ok := tl.limiters[tt]; ok {
		rl.Consume(amount)
	}
}

func (tl *TimeBaseLimiter) GrabQuota(tt ThrottleType, amount int64) {

	// for _, req := range reqs {
	if rl, ok := tl.limiters[tt]; ok {
		rl.Consume(amount)
	}
	// tl.limiters[req.Throttle].Consume(req.Amount)
	// }
	// tl.reqsLimiter.Consume(1)
	// tl.reqSizeLimiter.Consume(writeSize + readSize)
}

func (tl *TimeBaseLimiter) GetAvailable(tt ThrottleType) int64 {
	if _, ok := tl.limiters[tt]; ok {
		return tl.limiters[tt].GetAvailable()
	}
	return 0
}

func (tl *TimeBaseLimiter) GetReadAvailable() int64 {
	// return tl.reqSizeLimiter.GetAvailable()
	return math.MaxInt64
}
func (tl *TimeBaseLimiter) GetWriteAvailable() int64 {
	// return tl.reqSizeLimiter.GetAvailable()
	return math.MaxInt64
}

func (tl *TimeBaseLimiter) ConsumeRead(size int64) {
	// tl.reqsLimiter.Consume(1)
	// tl.reqSizeLimiter.Consume(size)
	// return
}

func (tl *TimeBaseLimiter) ConsumeWrite(size int64) {
	// tl.reqsLimiter.Consume(1)
	// tl.reqSizeLimiter.Consume(size)
	// return
}

func (tl *TimeBaseLimiter) IsBypass() bool {
	return false
}

// type NoopQuotaLimiter struct {
// }

// func (nl *NoopQuotaLimiter) CheckQuota(int64, int64) error {
// 	return nil
// }

// func (nl *NoopQuotaLimiter) GrabQuota(int64, int64) {
// 	return
// }

// func (nl *NoopQuotaLimiter) Consume(size int64) {
// 	return
// }

// func (nl *NoopQuotaLimiter) ConsumeRead(size int64) {
// 	return
// }

// func (nl *NoopQuotaLimiter) ConsumeWrite(size int64) {
// 	return
// }
// func (nl *NoopQuotaLimiter) IsBypass() bool {
// 	return true
// }

// func (tl *NoopQuotaLimiter) GetReadAvailable() int64 {
// 	return math.MaxInt64
// }
// func (tl *NoopQuotaLimiter) GetWriteAvailable() int64 {
// 	return math.MaxInt64
// }
