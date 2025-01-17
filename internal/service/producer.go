package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	dlock "github.com/meoying/kafka-ext/internal/lock"
	"github.com/meoying/kafka-ext/internal/lock/errs"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"time"
)

type ProducerJob struct {
	producer     sarama.SyncProducer
	repo         repository.MessageRepository
	BatchSize    int
	Logger       *slog.Logger
	maxSendCount int
	lockClient   dlock.Client
}

func NewProducerService(producer sarama.SyncProducer, repo repository.MessageRepository, lockClient dlock.Client) *ProducerJob {
	return &ProducerJob{
		producer:     producer,
		repo:         repo,
		Logger:       slog.Default(),
		maxSendCount: 3,
		BatchSize:    10,
		lockClient:   lockClient,
	}
}

func (p *ProducerJob) Start(ctx context.Context) {
	key := "send-delay-msg-job"
	interval := time.Minute
	for {
		lock, err := p.lockClient.NewLock(ctx, key, interval)
		if err != nil {
			p.Logger.Error("初始化分布式锁失败", slog.Any("err", err))
			continue
		}

		lockCtx, cancel := context.WithTimeout(ctx, time.Second*3)
		err = lock.Lock(lockCtx)
		cancel()
		if err != nil {
			if errors.Is(err, errs.ErrLocked) {
				p.Logger.Info("获取分布式锁失败，别人持有中")
			} else {
				p.Logger.Error("获取分布式锁失败，系统出现问题", slog.Any("err", err))
			}
			time.Sleep(time.Second * 10)
			continue
		}

		p.refreshAndExecTask(ctx, lock)

		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), time.Second*3)
		if err = lock.Unlock(unlockCtx); err != nil {
			p.Logger.Error("释放分布式锁失败", slog.Any("err", err))
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

func (p *ProducerJob) refreshAndExecTask(ctx context.Context, lock dlock.Lock) {
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
			p.Logger.Error("分布式锁续约失败", slog.Any("err", err))
			// 退出任务执行
			return
		}

		count, err := p.execTask(ctx)
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			return
		case err != nil:
			// 通过连续 N 次循环都出错来判定是偶发还是非偶发
			errCnt++
			// 连续 5 次，基本上可以断定不是偶发性错误了
			// 连续次数越多，越容易避开偶发性错误
			const threshold = 5
			if errCnt >= threshold {
				p.Logger.Error("执行任务连续出错，退出循环", slog.Int("threshold", threshold))
				return
			}
		default:
			errCnt = 0
			if count == 0 {
				time.Sleep(time.Second)
			}
		}
	}
}

func (p *ProducerJob) execTask(ctx context.Context) (int, error) {
	// 先从数据库中取一批待发送的消息
	// 然后将消息发送出去
	// 最后更新数据库中的消息状态
	msgs, err := p.findMsgs(ctx, 0, p.BatchSize)
	if err != nil {
		// 通过 len == 0 判断有没有消息
		return -1, fmt.Errorf("查找消息失败 %w", err)
	}
	var eg errgroup.Group
	for _, msg := range msgs {
		shadow := msg
		eg.Go(func() error {
			err1 := p.sengMsg(ctx, shadow)
			return err1
		})
	}
	err = eg.Wait()
	if err != nil {
		return len(msgs), fmt.Errorf("发送消息失败 %w", err)
	}
	return len(msgs), nil
}

func (p *ProducerJob) findMsgs(ctx context.Context, offset, limit int) ([]msg2.Message, error) {
	dbCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	msgs, err := p.repo.FindMsgs(dbCtx, offset, limit)
	cancel()
	return msgs, err
}

func (p *ProducerJob) sengMsg(ctx context.Context, msg msg2.Message) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     msg.BizTopic,
		Partition: msg.Partition,
		Key:       sarama.StringEncoder(msg.Key),
		Value:     sarama.StringEncoder(msg.Content),
	})
	count := msg.SendCount + 1
	field := map[string]interface{}{
		"status":     dao.MsgStatusInit,
		"send_count": count,
		"utime":      time.Now().UnixMilli(),
	}
	if err != nil {
		if count >= p.maxSendCount {
			field["status"] = dao.MsgStatusFail
		}
	} else {
		field["status"] = dao.MsgStatusSuccess
	}

	dbCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	err1 := p.repo.UpdateMsg(dbCtx, msg.Key, field)
	cancel()
	if err1 != nil {
		return fmt.Errorf("更新消息失败 %w, topic %s, key %s",
			err1, msg.BizTopic, msg.Key)
	}
	return err
}
