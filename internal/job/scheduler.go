package job

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/slice"
	_ "github.com/ecodeclub/ekit/slice"
	"github.com/meoying/kafka-ext/internal/pkg/lock"
	"github.com/meoying/kafka-ext/internal/service"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"log/slog"
	"time"
)

// Scheduler 延迟任务调度器，它需要做的事情有：
// 1. 自动发现新的分片，为新的分片自动创建一个定时任务
// 2. 关闭在已淘汰的分片上运行的定时任务
type Scheduler struct {
	sharding   *sharding2.Sharding
	jobSvc     *service.ProducerService
	lockClient lock.Client
	Logger     *slog.Logger

	cancelFns map[string]context.CancelFunc
	// 当前正在使用的分片
	currentDsts []sharding2.DST
}

func NewScheduler(sharding *sharding2.Sharding, jobSvc *service.ProducerService, lockClient lock.Client) *Scheduler {
	return &Scheduler{
		sharding:   sharding,
		jobSvc:     jobSvc,
		lockClient: lockClient,
		Logger:     slog.Default(),
		cancelFns:  make(map[string]context.CancelFunc),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	go s.schedule(ctx)
}

func (s *Scheduler) schedule(ctx context.Context) {
	// 在初始时，为所有有效分片创建出一个定时任务
	dsts := s.sharding.GetEffectiveTables()
	s.currentDsts = dsts
	for _, dst := range dsts {
		s.createTask(ctx, dst)
	}
	// 接着，定期检查分片是否发生变化，如果发生变化，则为新的分片创建定时任务，并关闭不需要的定时任务。
	// 一般来说，我们这里只有采用了范围分库分表时，才会出现分片变化的情况，
	// 比如今天用表A，明天用表B，那么第二天需要为表 B 创建一个定时任务，并关闭在表 A 上运行的定时任务。
	go s.watch(ctx)
}

func (s *Scheduler) createTask(ctx context.Context, dst sharding2.DST) {
	key := s.key(dst)
	s.Logger.Info("创建分片定时任务", slog.String("key", key))

	task := NewDelayProducerJob(s.jobSvc, s.lockClient, dst)
	taskCtx, cancel := context.WithCancel(ctx)
	task.Run(taskCtx)

	s.cancelFns[key] = cancel
}

func (s *Scheduler) watch(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.adjustTask(ctx)
		case <-ctx.Done():
			s.Logger.Info("退出任务调度")
			s.clean()
			return
		}
	}
}

func (s *Scheduler) adjustTask(ctx context.Context) {
	latest := s.sharding.GetEffectiveTables()
	// 比较新旧分片
	// latest 有的，cur 没有，就是新表，需要创建定时任务
	// cur 有的，latest 没有，就是已经停止使用的表，需要关闭它们的定时任务
	newShadings := slice.DiffSet(latest, s.currentDsts)
	for _, dst := range newShadings {
		s.createTask(ctx, dst)
	}

	eliminations := slice.DiffSet(s.currentDsts, latest)
	for _, dst := range eliminations {
		s.stopTask(dst)
	}

	s.currentDsts = latest
}

func (s *Scheduler) stopTask(dst sharding2.DST) {
	// 这里我就直接调用cancelFunc了
	key := s.key(dst)
	s.Logger.Info("关闭分片定时任务", slog.String("key", key))
	cancel, ok := s.cancelFns[key]
	if ok {
		cancel()
		delete(s.cancelFns, key)
	}
}

// clean 关闭所有定时任务
func (s *Scheduler) clean() {
	for _, cancel := range s.cancelFns {
		cancel()
	}
}

func (s *Scheduler) key(dst sharding2.DST) string {
	return fmt.Sprintf("%s:%s", dst.DB, dst.Table)
}
