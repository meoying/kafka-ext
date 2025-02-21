package service

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/sharding"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"time"
)

type ProducerService struct {
	producer     sarama.SyncProducer
	repo         repository.MessageRepository
	Logger       *slog.Logger
	MaxSendCount int
}

func NewProducerService(producer sarama.SyncProducer, repo repository.MessageRepository) *ProducerService {
	return &ProducerService{
		producer:     producer,
		repo:         repo,
		Logger:       slog.Default(),
		MaxSendCount: 3,
	}
}

func (p *ProducerService) SendDelayMsgs(ctx context.Context, batchSize int, dst sharding.DST) (int, error) {
	// 先从数据库中取一批待发送的消息
	// 然后将消息发送出去
	// 最后更新数据库中的消息状态
	msgs, err := p.findMsgs(ctx, 0, batchSize, dst)
	if err != nil {
		// 通过 len == 0 判断有没有消息
		return -1, fmt.Errorf("查找消息失败 %w", err)
	}
	var eg errgroup.Group
	for _, msg := range msgs {
		shadow := msg
		eg.Go(func() error {
			err1 := p.sengMsg(ctx, shadow, dst)
			return err1
		})
	}
	err = eg.Wait()
	if err != nil {
		return len(msgs), fmt.Errorf("发送消息失败 %w", err)
	}
	return len(msgs), nil
}

func (p *ProducerService) findMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]msg2.Message, error) {
	dbCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	msgs, err := p.repo.FindMsgs(dbCtx, offset, limit, dst)
	cancel()
	return msgs, err
}

func (p *ProducerService) sengMsg(ctx context.Context, msg msg2.Message, dst sharding.DST) error {
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
		if count >= p.MaxSendCount {
			field["status"] = dao.MsgStatusFail
		}
	} else {
		field["status"] = dao.MsgStatusSuccess
	}

	dbCtx, dbCancel := context.WithTimeout(ctx, time.Second*3)
	err1 := p.repo.UpdateMsg(dbCtx, msg.Key, field, dst)
	dbCancel()
	if err1 != nil {
		return fmt.Errorf("更新消息失败 %w, topic %s, key %s",
			err1, msg.BizTopic, msg.Key)
	}
	return err
}
