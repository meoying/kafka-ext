package sharding

import (
	"errors"
	"fmt"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"time"
)

type Dispatcher struct {
	// biz => strategy name 每个 biz 使用的 strategy
	bizStrategy map[string]string
	strategies  map[string]Strategy
}

func NewDispatcher(bizStrategy map[string]string, strategies map[string]Strategy) *Dispatcher {
	return &Dispatcher{bizStrategy: bizStrategy, strategies: strategies}
}

func (d *Dispatcher) Name() string {
	return "dispatch"
}

func (d *Dispatcher) Dispatch(msg msg2.DelayMessage) (DST, error) {
	name, ok := d.bizStrategy[msg.Biz]
	if !ok {
		return DST{}, fmt.Errorf("找不到 biz 使用的策略，biz = %s", msg.Biz)
	}

	strategy, ok := d.strategies[name]
	if !ok {
		return DST{}, fmt.Errorf("找不到 strategy = %s", name)
	}

	keys, err := d.shardingKeys(name, msg)
	if err != nil {
		return DST{}, err
	}
	return strategy.Sharding(keys)
}

func (d *Dispatcher) GetEffectiveTables() []DST {
	dsts := make([]DST, 0, 9)
	for _, strategy := range d.strategies {
		dsts = append(dsts, strategy.EffectiveTables()...)
	}
	return dsts
}

func (d *Dispatcher) shardingKeys(name string, msg msg2.DelayMessage) ([]string, error) {
	switch name {
	case "hash":
		// 默认使用 biz 和 bizID 作为分库分表字段
		return []string{msg.Biz, msg.Key}, nil
	case "time_range":
		// 默认使用时间作为分库分表字段
		// 我只会把当前的时间戳传递进入，具体的处理逻辑，交给策略自己处理
		return []string{time.Now().Format("20060102")}, nil
	default:
		return nil, errors.New("未知的分库分表策略")
	}
}
