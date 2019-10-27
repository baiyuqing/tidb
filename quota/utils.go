package quota

import (
	"errors"
	"github.com/spf13/cast"
	"regexp"
	"strings"
)

func ParseLimit(strLimit string) (ThrottleType, int64, TimeUnit, error) {
	strLimit = strings.ToLower(strLimit)
	pattern := "(\\d+)(req|[bkmgtp])\\/(sec|min|hour|day)"
	ok, err := regexp.MatchString(pattern, strLimit)
	if err != nil {
		return -1, -1, -1, err
	}
	if !ok {
		return -1, -1, -1, errors.New("no match")
	}
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return -1, -1, -1, err
	}
	groups := reg.FindAllStringSubmatch(strLimit, 1)
	if len(groups[0]) < 4 {
		return -1, -1, -1, errors.New("no match")
	}
	var (
		throttleType ThrottleType
		limit        int64
		timeUnit     TimeUnit
	)
	if groups[0][2] == "req" {
		throttleType = RequestNumber
		limit = cast.ToInt64(groups[0][1])
	} else {
		throttleType = RequestSize
		limit = sizeFromStr(cast.ToInt64(groups[0][1]), groups[0][2])
	}
	switch groups[0][3] {
	case "sec":
		timeUnit = SECONDS
	case "min":
		timeUnit = MINUTES
	case "hour":
		timeUnit = HOURS
	case "day":
		timeUnit = DAYS
	}

	return throttleType, limit, timeUnit, nil
}

func sizeFromStr(value int64, suffix string) int64 {
	switch suffix {
	case "k":
		value = value << 10
	case "m":
		value = value << 20
	case "g":
		value = value << 30
	case "t":
		value = value << 40
	case "p":
		value = value << 50
	}
	return value
}
