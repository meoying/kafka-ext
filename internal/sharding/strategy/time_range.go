package strategy

import (
	"errors"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"strconv"
	"time"
)

// TimeRange 基于时间范围的分库分表
type TimeRange struct {
	Range
	startTime time.Time
	// 返回当前时间。
	// 为了测试弄的，不要修改
	Now func() time.Time
}

func NewTimeRange(db string, tablePattern string, startTime time.Time, interval int) TimeRange {
	return TimeRange{
		Range:     NewRange(db, tablePattern, interval),
		startTime: startTime,
		Now: func() time.Time {
			return time.Now()
		},
	}
}

func (t TimeRange) Name() string {
	return "time_range"
}

func (t TimeRange) Sharding(keys []string) (sharding2.DST, error) {
	// 距离开始时间过了多少天
	now := t.Now()
	duration := now.Sub(t.startTime)
	if duration < 0 {
		return sharding2.DST{}, errors.New("time_range: 时间错误，当前时间小于开始时间")
	}
	days := int(duration.Hours() / 24)
	return t.Range.Sharding([]string{strconv.Itoa(days)})
}

type TimeRangePattern struct {
	RangePattern
	StartTime string `json:"startTime" yaml:"startTime"`
}
