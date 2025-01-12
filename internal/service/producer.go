package service

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"time"
)

type ProducerService struct {
	producer     sarama.SyncProducer
	repo         repository.MessageRepository
	batchSize    int
	logger       *slog.Logger
	maxSendCount int
}

func NewProducerService(producer sarama.SyncProducer, repo repository.MessageRepository, logger *slog.Logger) *ProducerService {
	return &ProducerService{
		producer:     producer,
		repo:         repo,
		logger:       logger,
		maxSendCount: 3,
		batchSize:    10,
	}
}

func (p *ProducerService) FindAndSendMsgs(ctx context.Context) error {
	// 先从数据库中取一批待发送的消息
	// 然后将消息发送出去
	// 最后更新数据库中的消息状态
	offset := 0
	msgs, err := p.findMsgs(ctx, offset, p.batchSize)
	if err != nil {
		p.logger.Error("查找消息失败", slog.Any("err", err))
		return err
	}
	// TODO: 使用延时队列提升时间精度

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
		p.logger.Error("发送消息失败", slog.Any("err", err))
	}
	return err
}

func (p *ProducerService) findMsgs(ctx context.Context, offset, limit int) ([]msg2.Message, error) {
	dbCtx, cancel := context.WithTimeout(ctx, time.Second*3)
	msgs, err := p.repo.FindMsgs(dbCtx, offset, limit)
	cancel()
	return msgs, err
}

func (p *ProducerService) sengMsg(ctx context.Context, msg msg2.Message) error {
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
