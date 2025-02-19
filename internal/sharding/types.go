package sharding

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
