package job

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/slice"
	_ "github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/syncx/atomicx"
	"github.com/meoying/dlock-go"
	"github.com/meoying/kafka-ext/internal/service"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"gorm.io/gorm"
	"log/slog"
	"time"
)

// Scheduler 延迟任务调度器，它需要做的事情有：
// 1. 自动发现新的分片，为新的分片自动创建一个定时任务
// 2. 关闭在已淘汰的分片上运行的定时任务
type Scheduler struct {
	dispatcher *sharding2.Dispatcher
	jobSvc     *service.ProducerService
	lockClient dlock.Client
	Logger     *slog.Logger

	cancelFns map[string]context.CancelFunc
	// 当前正在使用的分片
	currentDsts atomicx.Value[[]sharding2.DST]
	// watch interval
	interval time.Duration
	dbs      map[string]*gorm.DB
}

func NewScheduler(dispatcher *sharding2.Dispatcher, jobSvc *service.ProducerService,
	lockClient dlock.Client, dbs map[string]*gorm.DB) *Scheduler {
	return &Scheduler{
		dispatcher: dispatcher,
		jobSvc:     jobSvc,
		lockClient: lockClient,
		Logger:     slog.Default(),
		cancelFns:  make(map[string]context.CancelFunc),
		interval:   time.Minute,
		dbs:        dbs,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	go s.schedule(ctx)
}

func (s *Scheduler) schedule(ctx context.Context) {
	// 在初始时，为所有有效分片创建出一个定时任务
	// 一张表就会有一个对应的 Sender
	dsts := s.dispatcher.GetEffectiveTables()
	s.currentDsts.Store(dsts)
	for _, dst := range dsts {
		s.createTaskIfExist(ctx, dst)
	}
	// 接着，定期检查分片是否发生变化，如果发生变化，则为新的分片创建定时任务，并关闭不需要的定时任务。
	// 一般来说，我们这里只有采用了范围分库分表时，才会出现分片变化的情况，
	// 比如今天用表A，明天用表B，那么第二天需要为表 B 创建一个定时任务，并关闭在表 A 上运行的定时任务。
	go s.watch(ctx)
}

func (s *Scheduler) shardingExist(dst sharding2.DST) bool {
	// 这种实现的缺陷是与 gorm 强耦合了
	db, ok := s.dbs[dst.DB]
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	return db.WithContext(ctx).Migrator().HasTable(dst.Table)
}

func (s *Scheduler) createTaskIfExist(ctx context.Context, dst sharding2.DST) {
	if !s.shardingExist(dst) {
		return
	}

	key := s.key(dst)
	s.Logger.Info("创建分片定时任务", slog.String("key", key))
	// 每个分片都有一个自己的定时任务
	task := NewDelayProducerJob(s.jobSvc, s.lockClient, dst, s)
	taskCtx, cancel := context.WithCancel(ctx)
	task.Run(taskCtx)

	s.cancelFns[key] = cancel
}

func (s *Scheduler) watch(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
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
	latest := s.dispatcher.GetEffectiveTables()
	// 比较新旧分片
	// latest 有的，cur 没有，就是新表，需要创建定时任务
	// cur 有的，latest 没有，就是已经停止使用的表，需要关闭它们的定时任务
	newShadings := slice.DiffSet(latest, s.currentDsts.Load())
	for _, dst := range newShadings {
		s.createTaskIfExist(ctx, dst)
	}

	eliminations := slice.DiffSet(s.currentDsts.Load(), latest)
	for _, dst := range eliminations {
		s.stopTask(dst)
	}

	s.currentDsts.Store(latest)
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
