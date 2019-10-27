package quota

type ThrottleType int32

const (
	ThrottleUnknown ThrottleType = iota
	RequestNumber
	RequestSize
	WriteNumber
	WriterSize
	ReadNumber
	ReadSize
)

func (t ThrottleType) String() string {
	switch t {
	case RequestNumber:
		return "request-number"
	case RequestSize:
		return "request-size"
	case WriteNumber:
		return "write-number"
	case WriterSize:
		return "write-size"
	case ReadNumber:
		return "read-number"
	case ReadSize:
		return "read-size"
	}
	return ""
}

type TimeUnit int32

const (
	NANOSECONDS TimeUnit = iota + 1
	MICROSECONDS
	MILLISECONDS
	SECONDS
	QUARTERMINUTE
	HALFMINUTE
	MINUTES
	HOURS
	DAYS
)

func (t TimeUnit) String() string {
	switch t {
	case SECONDS:
		return "sec"
	case QUARTERMINUTE:
		return "quarter-min"
	case HALFMINUTE:
		return "half-min"
	case MINUTES:
		return "min"
	case HOURS:
		return "hour"
	case DAYS:
		return "day"
	}
	return ""
}

type TimedQuota struct {
	TimeUnit  TimeUnit
	SoftLimit int64
}

type Quotas struct {
	Bypass bool
	// Throttle *Throttle
	ReqNum    *TimedQuota
	ReqSize   *TimedQuota
	WriteNum  *TimedQuota
	WriteSize *TimedQuota
	ReadNum   *TimedQuota
	ReadSize  *TimedQuota
}

func (q *Quotas) Set(tt ThrottleType, limit int64, timeUnit TimeUnit) {
	switch tt {
	case RequestNumber:
		q.ReqNum = &TimedQuota{timeUnit, limit}
	case RequestSize:
		q.ReqSize = &TimedQuota{timeUnit, limit}
	// case WriteNumber:
	// 	return "write-number"
	// case WriterSize:
	// 	return "write-size"
	case ReadNumber:
		q.ReadNum = &TimedQuota{timeUnit, limit}
	case ReadSize:
		q.ReadSize = &TimedQuota{timeUnit, limit}
	}
}
