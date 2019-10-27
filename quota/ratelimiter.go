package quota

import (
	"math"
	"sync"
	"time"
)

type RateLimiter interface {
	Refill(limit int64) int64
	GetWaitInterval(limit, available, amount int64) int64
	Set(limit int64, timeUnit TimeUnit)
	Update(other RateLimiter)
	IsBypass() bool
	GetLimit() int64
	GetAvailable() int64
	GetTimeUnitInMillis() int64
	CanExcute(amount int64) bool
	Consume(amount int64)
	WaitInterval(amount int64) int64
	SetNextRefillTime(nextRefillTime int64)
	GetNextRefillTime() int64
}

//FixedIntervalRateLimiter With this limiter resources will be refilled only after a fixed interval of time.
type FixedIntervalRateLimiter struct {
	tunit          int64
	limit          int64
	avail          int64
	nextRefillTime int64
	sync.Mutex
}

func NewFixedIntervalRateLimiter() *FixedIntervalRateLimiter {
	return &FixedIntervalRateLimiter{
		tunit:          1000,
		limit:          math.MaxInt64,
		avail:          math.MaxInt64,
		nextRefillTime: -1,
	}
}

func (l *FixedIntervalRateLimiter) Set(limit int64, timeUnit TimeUnit) {
	l.Lock()
	switch timeUnit {
	case MILLISECONDS:
		l.tunit = 1
	case SECONDS:
		l.tunit = 1000
	case QUARTERMINUTE:
		l.tunit = 15 * 1000
	case HALFMINUTE:
		l.tunit = 30 * 1000
	case MINUTES:
		l.tunit = 60 * 1000
	case HOURS:
		l.tunit = 60 * 60 * 1000
	case DAYS:
		l.tunit = 24 * 60 * 60 * 1000
	}
	l.limit = limit
	l.avail = limit
	l.Unlock()
}

func (l *FixedIntervalRateLimiter) Update(other RateLimiter) {
	l.Lock()
	l.tunit = other.GetTimeUnitInMillis()
	if l.limit < other.GetLimit() {
		// If avail is capped to this.limit, it will never overflow,
		// otherwise, avail may overflow, just be careful here.
		diff := other.GetLimit() - l.limit
		if l.avail <= math.MaxInt64-diff {
			l.avail += diff
			l.avail = min(l.avail, other.GetLimit())
		} else {
			l.avail = other.GetAvailable()
		}
	}
	l.limit = other.GetLimit()
	l.Unlock()
}

func (l *FixedIntervalRateLimiter) IsBypass() bool {
	return l.limit == math.MaxInt64
}
func (l *FixedIntervalRateLimiter) GetLimit() int64 {
	return l.limit
}
func (l *FixedIntervalRateLimiter) GetAvailable() int64 {
	return l.avail
}
func (l *FixedIntervalRateLimiter) GetTimeUnitInMillis() int64 {
	return l.tunit
}

func (l *FixedIntervalRateLimiter) CanExcute(amount int64) bool {
	if l.IsBypass() {
		return true
	}
	refillAmount := l.Refill(l.limit)
	if refillAmount == 0 && l.avail < amount {
		return false
	}
	// check for positive overflow
	if l.avail < math.MaxInt64-refillAmount {
		l.avail = max(0, min(l.avail+refillAmount, l.limit))
	} else {
		l.avail = max(0, l.limit)
	}
	if l.avail > amount {
		return true
	}
	return false
}

func (l *FixedIntervalRateLimiter) Consume(amount int64) {
	l.Lock()
	defer l.Unlock()
	if l.IsBypass() {
		return
	}
	if amount > 0 {
		l.avail -= amount
		if l.avail < 0 {
			l.avail = 0
		}
	} else {
		if l.avail <= math.MaxInt64+amount {
			l.avail -= amount
			l.avail = min(l.avail, l.limit)
		} else {
			l.avail = l.limit
		}
	}
}
func (l *FixedIntervalRateLimiter) WaitInterval(amount int64) int64 {
	l.Lock()
	defer l.Unlock()
	if amount <= l.avail {
		return 0
	}
	return l.GetWaitInterval(l.limit, l.avail, amount)
}

func (l *FixedIntervalRateLimiter) SetNextRefillTime(nextRefillTime int64) {
	l.nextRefillTime = nextRefillTime
}
func (l *FixedIntervalRateLimiter) GetNextRefillTime() int64 {
	return l.nextRefillTime
}

func (l *FixedIntervalRateLimiter) Refill(limit int64) int64 {
	now := time.Now().UnixNano() / int64(1e6)
	if now < l.nextRefillTime {
		return 0
	}
	l.nextRefillTime = now + l.GetTimeUnitInMillis()
	return l.limit
}

func (l *FixedIntervalRateLimiter) GetWaitInterval(limit, available, amount int64) int64 {
	if l.nextRefillTime == -1 {
		return 0
	}
	now := time.Now().UnixNano() / int64(1e6)
	return l.nextRefillTime - now
}

func min(a int64, b ...int64) int64 {
	if len(b) == 0 {
		return a
	}
	_min := a
	for _, x := range b {
		if _min > x {
			_min = x
		}
	}
	return _min
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

//AverageIntervalRateLimiter With this limiter resources will be refilled only after a fixed interval of time.
type AverageIntervalRateLimiter struct {
	tunit          int64
	limit          int64
	avail          int64
	nextRefillTime int64
	sync.Mutex
}

func NewAverageIntervalRateLimiter() *AverageIntervalRateLimiter {
	return &AverageIntervalRateLimiter{
		tunit:          1000,
		limit:          math.MaxInt64,
		avail:          math.MaxInt64,
		nextRefillTime: -1,
	}
}

func (l *AverageIntervalRateLimiter) Set(limit int64, timeUnit TimeUnit) {
	l.Lock()
	switch timeUnit {
	case MILLISECONDS:
		l.tunit = 1
	case SECONDS:
		l.tunit = 1000
	case MINUTES:
		l.tunit = 60 * 1000
	case HOURS:
		l.tunit = 60 * 60 * 1000
	case DAYS:
		l.tunit = 24 * 60 * 60 * 1000
	}
	l.limit = limit
	l.avail = limit
	l.Unlock()
}

func (l *AverageIntervalRateLimiter) Update(other RateLimiter) {
	l.Lock()
	l.tunit = other.GetTimeUnitInMillis()
	if l.limit < other.GetLimit() {
		// If avail is capped to this.limit, it will never overflow,
		// otherwise, avail may overflow, just be careful here.
		diff := other.GetLimit() - l.limit
		if l.avail <= math.MaxInt64-diff {
			l.avail += diff
			l.avail = min(l.avail, other.GetLimit())
		} else {
			l.avail = other.GetAvailable()
		}
	}
	l.limit = other.GetLimit()
	l.Unlock()
}

func (l *AverageIntervalRateLimiter) IsBypass() bool {
	return l.limit == math.MaxInt64
}
func (l *AverageIntervalRateLimiter) GetLimit() int64 {
	return l.limit
}
func (l *AverageIntervalRateLimiter) GetAvailable() int64 {
	return l.avail
}
func (l *AverageIntervalRateLimiter) GetTimeUnitInMillis() int64 {
	return l.tunit
}

func (l *AverageIntervalRateLimiter) CanExcute(amount int64) bool {
	if l.IsBypass() {
		return true
	}
	refillAmount := l.Refill(l.limit)
	if refillAmount == 0 && l.avail < amount {
		return false
	}

	// check for positive overflow
	if l.avail <= math.MaxInt64-refillAmount {
		l.avail = max(0, min(l.avail+refillAmount, l.limit))
	} else {
		l.avail = max(0, l.limit)
	}
	if l.avail >= amount {
		return true
	}
	return false
}

func (l *AverageIntervalRateLimiter) Consume(amount int64) {
	// log.Println("l.avail: ", l.avail)
	l.Lock()
	defer l.Unlock()
	if l.IsBypass() {
		return
	}
	if amount > 0 {
		l.avail = l.avail - amount
		if l.avail < 0 {
			l.avail = 0
		}

	} else {
		if l.avail <= math.MaxInt64+amount {
			l.avail = l.avail - amount
			l.avail = min(l.avail, l.limit)
		} else {
			l.avail = l.limit
		}

	}

}
func (l *AverageIntervalRateLimiter) WaitInterval(amount int64) int64 {
	l.Lock()
	defer l.Unlock()
	if amount <= l.avail {
		return 0
	}
	return l.GetWaitInterval(l.limit, l.avail, amount)
}

func (l *AverageIntervalRateLimiter) SetNextRefillTime(nextRefillTime int64) {
	l.nextRefillTime = nextRefillTime
}
func (l *AverageIntervalRateLimiter) GetNextRefillTime() int64 {
	return l.nextRefillTime
}

func (l *AverageIntervalRateLimiter) Refill(limit int64) int64 {
	now := time.Now().UnixNano() / int64(1e6)

	if l.nextRefillTime == -1 {
		l.nextRefillTime = now
		return limit
	}
	timeInterval := now - l.nextRefillTime
	delta := int64(0)
	if timeInterval >= l.tunit {
		delta = limit
	} else if timeInterval > 0 {
		r := (float64(timeInterval) / float64(l.tunit)) * float64(l.limit)
		delta = int64(r)
	}
	if delta > 0 {
		l.nextRefillTime = now
	}
	return delta
}

func (l *AverageIntervalRateLimiter) GetWaitInterval(limit, available, amount int64) int64 {
	if l.nextRefillTime == -1 {
		return 0
	}
	r := float64(amount-available) * float64(l.tunit) / float64(limit)
	return int64(r)
}
