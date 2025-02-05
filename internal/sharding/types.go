package sharding

import (
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"log/slog"
	"time"
)

type Sharding struct {
	Logger   *slog.Logger
	strategy Strategy
}

func NewSharding(algorithm Strategy) *Sharding {
	return &Sharding{
		strategy: algorithm,
		Logger:   slog.Default(),
	}
}

func (s *Sharding) Route(msg msg2.DelayMessage) DST {
	switch s.strategy.Name() {
	case "hash":
		// 默认使用 biz 和 bizID 作为分库分表字段
		return s.strategy.Sharding([]string{msg.Biz, msg.Key})
	case "range":
		// 默认使用时间作为分库分表字段
		// 我只会把当前的时间戳传递进入，具体的处理逻辑，交给策略自己处理
		return s.strategy.Sharding([]string{time.Now().Format("20060102")})
	default:
		s.Logger.Error("未知的分库分表策略", slog.String("strategy", s.strategy.Name()))
		return DST{}
	}
}

func (s *Sharding) GetEffectiveTables() []DST {
	return s.strategy.EffectiveTables()
}

// Strategy 分库分表策略
type Strategy interface {
	Name() string
	// Sharding 返回延迟消息的 DST
	// keys 分库分表使用的字段，如在哈希分库分表下的 biz 和 bizID
	Sharding(keys []string) DST
	// EffectiveTables 返回所有的有效目标库和目标表，用于定时任务调度。
	// 定时任务调度器会根据前后两次调用的结果，判断哪些表是有效的，哪些表是无效的，并直接关闭无效表的定时任务。
	EffectiveTables() []DST
}

type DST struct {
	DB    string
	Table string
}
