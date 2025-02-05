package strategy

import (
	"fmt"
	"github.com/meoying/kafka-ext/internal/sharding"
	"time"
)

// TimeRange 时间范围分库分表算法
type TimeRange struct {
	db          string
	tablePrefix string
	format      string
	retention   int
	interval    int
}

func NewTimeRange(db string, tablePrefix string, retention int, interval int) *TimeRange {
	return &TimeRange{
		db: db, tablePrefix: tablePrefix,
		retention: retention, interval: interval,
		format: "20060102",
	}
}

func (r TimeRange) Name() string {
	return "range"
}

func (r TimeRange) Sharding(keys []string) sharding.DST {
	timestamp, err := time.Parse("20060102", keys[0])
	if err != nil {
		timestamp = time.Now()
	}
	shardStart := r.shardingStartTime(timestamp)
	// 每个分片使用开始时间作为名称
	table := fmt.Sprintf(r.tablePrefix, shardStart.Format(r.format))
	return sharding.DST{
		DB:    r.db,
		Table: table,
	}
}

func (r TimeRange) shardingStartTime(timestamp time.Time) time.Time {
	baseTime := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(),
		0, 0, 0, 0, timestamp.Location())
	_, _, day := baseTime.Date()
	// 计算偏移量，也就是距离分片的开始过了多少天: [0, intervalDays - 1]
	daysOffset := (day - 1) % r.interval
	// 当前分片的开始时间
	start := baseTime.AddDate(0, 0, -daysOffset)
	return start
}

func (r TimeRange) EffectiveTables() []sharding.DST {
	// 计算一下总共有多少个有效分片
	num := r.retention / r.interval
	if r.retention%r.interval != 0 {
		num += 1
	}

	dsts := make([]sharding.DST, 0, num)
	shardStart := r.shardingStartTime(time.Now())
	for i := 0; i < num; i++ {
		// 计算出每个分片的开始时间，往前算 i * r.interval 天
		preSharding := shardStart.AddDate(0, 0, -i*r.interval)
		table := fmt.Sprintf(r.tablePrefix, preSharding.Format(r.format))
		dsts = append(dsts, sharding.DST{
			DB:    r.db,
			Table: table,
		})
	}
	return dsts
}
