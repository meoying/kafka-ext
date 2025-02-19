package strategy

import (
	"github.com/meoying/kafka-ext/internal/sharding"
)

// Range 通用的范围分库分表
type Range struct {
	tableStep int
	tableName string
}

func (r Range) Name() string {
	return "range"
}

func (r Range) Sharding(keys []string) sharding.DST {
	//TODO implement me
	panic("implement me")
}

func (r Range) EffectiveTables() []sharding.DST {
	//TODO implement me
	panic("implement me")
}
