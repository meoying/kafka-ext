package strategy

import (
	"fmt"
	"github.com/ecodeclub/ekit/syncx/atomicx"
	"github.com/meoying/kafka-ext/internal/sharding"
	"github.com/pkg/errors"
	"strconv"
	"sync"
)

// Range 通用的范围分库分表
type Range struct {
	// 数据库名称，如 kafka_ext_db
	db string
	// 表名，如 kafka_ext_table_%d
	tablePattern string
	// 分表间隔
	interval int
	// 最新分片的序号
	maxSeq *atomicx.Value[int]
	mu     *sync.Mutex
}

func NewRange(db string, tablePattern string, interval int) Range {
	if interval <= 0 {
		interval = 1
	}
	return Range{
		db:           db,
		tablePattern: tablePattern,
		interval:     interval,
		maxSeq:       atomicx.NewValueOf[int](0),
		mu:           &sync.Mutex{},
	}
}

func (r Range) Name() string {
	return "range"
}

func (r Range) Sharding(keys []string) (sharding.DST, error) {
	if len(keys) < 1 {
		return sharding.DST{}, errors.New("range: 分片 key 错误")
	}
	key := keys[0]
	num, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return sharding.DST{}, fmt.Errorf("rang: 分片 key 类型转换错误 %w, key=%s", err, key)
	}
	// 初始化的时候已经确保了 interval 不会是 0
	seq := int(num / int64(r.interval))
	if seq > r.maxSeq.Load() {
		r.mu.Lock()
		if seq > r.maxSeq.Load() {
			r.maxSeq.Store(seq)
		}
		r.mu.Unlock()
	}
	return sharding.DST{
		DB:    r.db,
		Table: fmt.Sprintf(r.tablePattern, seq),
	}, nil
}

func (r Range) EffectiveTables() []sharding.DST {
	seq := r.maxSeq.Load()
	res := make([]sharding.DST, 0, seq)
	for i := 0; i <= seq; i++ {
		yield := sharding.DST{
			DB:    r.db,
			Table: fmt.Sprintf(r.tablePattern, i),
		}
		res = append(res, yield)
	}
	return res
}

type RangePattern struct {
	DB           string `json:"db" yaml:"db"`
	TablePattern string `json:"tablePattern" yaml:"tablePattern"`
	Interval     int    `json:"interval" yaml:"interval"`
}
