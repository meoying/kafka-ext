package job

import (
	"context"
	"errors"
	"fmt"
	dlock "github.com/meoying/kafka-ext/internal/pkg/lock"
	"github.com/meoying/kafka-ext/internal/pkg/lock/errs"
	"github.com/meoying/kafka-ext/internal/service"
	"github.com/meoying/kafka-ext/internal/sharding"
	"log/slog"
	"time"
)

// DelayProducerJob 延迟消息生产者
type DelayProducerJob struct {
	svc        *service.ProducerService
	Logger     *slog.Logger
	lockClient dlock.Client
	BatchSize  int
	dst        sharding.DST
}

func NewDelayProducerJob(svc *service.ProducerService, lockClient dlock.Client, dst sharding.DST) *DelayProducerJob {
	return &DelayProducerJob{svc: svc,
		Logger:     slog.Default(),
		lockClient: lockClient,
		BatchSize:  100,
		dst:        dst,
	}
}

func (p *DelayProducerJob) Run(ctx context.Context) {
	go p.do(ctx)
}

func (p *DelayProducerJob) do(ctx context.Context) {
	key := fmt.Sprintf("%s.%s", p.dst.DB, p.dst.Table)
	p.Logger = p.Logger.With(slog.String("key", key))
	interval := time.Minute
	for {
		if !p.tableExist(p.dst) {
			p.Logger.Info("检测到分片不存在，退出消息发送任务")
			return
		}

		lock, err := p.lockClient.NewLock(ctx, key, interval)
		if err != nil {
			p.Logger.Error("初始化分布式锁失败", slog.Any("err", err))
			continue
		}

		lockCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = lock.Lock(lockCtx)
		cancel()

		switch {
		case errors.Is(err, errs.ErrLocked):
			p.Logger.Info("获取分布式锁失败，别人持有中")
			time.Sleep(time.Second * 10)
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			p.Logger.Info("任务取消，退出消息发送任务")
			return
		case err != nil:
			time.Sleep(time.Second * 10)
			continue
		default:
			// err == nil
		}

		err = p.refreshAndSendMsgs(ctx, lock)

		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), time.Second*3)
		if err1 := lock.Unlock(unlockCtx); err1 != nil {
			p.Logger.Error("释放分布式锁失败", slog.Any("err", err1))
		}
		unlockCancel()

		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			p.Logger.Info("任务取消，退出消息发送任务")
			return
		default:
			p.Logger.Error("执行消息发送任务失败，将执行重试", slog.Any("err", err))
			time.Sleep(interval)
		}
	}
}

func (p *DelayProducerJob) refreshAndSendMsgs(ctx context.Context, lock dlock.Lock) error {
	// 1. 续约
	// 2. 执行任务
	// 如果自身出错情况较高，就需要让出分布式锁

	// 连续出现 error 的次数，用于容错、负载均衡
	errCnt := 0
	for {
		// 每次执行业务前，先刷新一下锁
		refreshCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err := lock.Refresh(refreshCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("分布式锁续约失败 %w", err)
		}

		count, err := p.svc.SendDelayMsgs(ctx, p.BatchSize, p.dst)
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			return ctx.Err()
		case err != nil:
			// 通过连续 N 次循环都出错来判定是偶发还是非偶发
			errCnt++
			// 连续 5 次，基本上可以断定不是偶发性错误了
			// 连续次数越多，越容易避开偶发性错误
			const threshold = 5
			if errCnt >= threshold {
				p.Logger.Error("", slog.Int("threshold", threshold))
				return fmt.Errorf("执行任务连续出错超过 threshold = %d，退出循环", threshold)
			}
		default:
			errCnt = 0
			if count == 0 {
				time.Sleep(time.Second)
			}
			// 继续执行
		}
	}
}

func (p *DelayProducerJob) tableExist(dst sharding.DST) bool {
	// TODO：检测分片是否存在
	return true
}
