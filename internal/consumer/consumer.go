package consumer

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/service"
	"log/slog"
	"time"
)

// DelayConsumer 消费消息，然后转储到数据库
type DelayConsumer struct {
	svc    *service.ConsumerService
	logger *slog.Logger
}

func NewDelayConsumer(svc *service.ConsumerService, logger *slog.Logger) *DelayConsumer {
	return &DelayConsumer{svc: svc, logger: logger}
}

func (c *DelayConsumer) Setup(session sarama.ConsumerGroupSession) error {
	c.logger.Info("启动消费者", slog.String("member_id", session.MemberID()))

	return nil
}

func (c *DelayConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *DelayConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgs := claim.Messages()
	for msg := range msgs {
		delayMsg, err := c.newMsg(msg)
		if err != nil {
			c.logger.Error("提取消费内容失败",
				slog.String("key", string(msg.Key)),
				slog.Any("err", err))
			session.MarkMessage(msg, "")
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		err = c.svc.StoreMsg(ctx, delayMsg)
		cancel()

		if err != nil {
			c.logger.Error("转储消息失败",
				slog.String("key", string(msg.Key)),
				slog.Any("err", err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *DelayConsumer) newMsg(msg *sarama.ConsumerMessage) (msg2.DelayMessage, error) {
	var res msg2.DelayMessage
	err := json.Unmarshal(msg.Value, &res)
	if err != nil {
		return msg2.DelayMessage{}, err
	}
	return res, nil
}
