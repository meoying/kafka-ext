package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
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
}

func NewProducerService(producer sarama.SyncProducer, repo repository.MessageRepository) *ProducerJob {
	return &ProducerJob{
		producer:     producer,
		repo:         repo,
		Logger:       slog.Default(),
		maxSendCount: 3,
		BatchSize:    10,
	}
}

func (p *ProducerJob) Start(ctx context.Context) {
	for {
		count, err := p.findAndSendMsgs(ctx)
		if err != nil {
			p.Logger.Error("执行任务失败", slog.Any("err", err))
		}
		ctxErr := ctx.Err()
		switch {
		case errors.Is(ctxErr, context.Canceled), errors.Is(ctxErr, context.DeadlineExceeded):
			p.Logger.Info("任务被取消，退出任务循环")
			return
		default:
			p.Logger.Error("定时任务失败，将执行重试")
		}
		if count == 0 {
			time.Sleep(time.Second)
		}
	}
}

func (p *ProducerJob) findAndSendMsgs(ctx context.Context) (int, error) {
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
