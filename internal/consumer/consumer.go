package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/service"
	"log/slog"
	"time"
)

// DelayConsumer 消费消息，然后转储到数据库
type DelayConsumer struct {
	svc    *service.ConsumerService
	Logger *slog.Logger
}

func NewDelayConsumer(svc *service.ConsumerService) *DelayConsumer {
	return &DelayConsumer{svc: svc, Logger: slog.Default()}
}

func (c *DelayConsumer) Setup(session sarama.ConsumerGroupSession) error {
	c.Logger.Info("启动消费者", slog.String("member_id", session.MemberID()))
	return nil
}

func (c *DelayConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *DelayConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgs := claim.Messages()
	for msg := range msgs {
		err := c.consume(msg)
		if err != nil {
			c.Logger.Error("消费失败", slog.Any("err", err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *DelayConsumer) consume(msg *sarama.ConsumerMessage) error {
	delayMsg, err := c.newMsg(msg)
	if err != nil {
		return fmt.Errorf("提取消息内容失败 %w, key = %s", err, msg.Key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err = c.svc.StoreMsg(ctx, delayMsg)
	cancel()
	if err != nil {
		return fmt.Errorf("转储消息失败 %w, key = %s", err, msg.Key)
	}
	return nil
}

func (c *DelayConsumer) newMsg(msg *sarama.ConsumerMessage) (msg2.DelayMessage, error) {
	var res msg2.DelayMessage
	err := json.Unmarshal(msg.Value, &res)
	return res, err
}
