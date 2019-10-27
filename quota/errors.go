package quota

import (
	"errors"
	"fmt"
)

var (
	ErrThrottlingLimitExceed = errors.New("throttling limit exceed")
)

type ThrottlingLimitedError struct {
	OperType OperationType
	Throttle ThrottleType
	Limit    int64
	Current  int64
}

func NewThrottlingLimitedError() ThrottlingLimitedError {
	return ThrottlingLimitedError{}
}

func (t ThrottlingLimitedError) Error() string {
	return fmt.Sprintf("operation %v throttling limit exceed on %v", t.OperType, t.Throttle)
}
