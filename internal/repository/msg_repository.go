package repository

import (
	"context"
	"encoding/json"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/sharding"
	"log/slog"
)

type MsgRepository struct {
	dao    dao.MessageDAO
	logger *slog.Logger
}

func NewMsgRepository(dao dao.MessageDAO) *MsgRepository {
	return &MsgRepository{dao: dao, logger: slog.Default()}
}

func (m *MsgRepository) CreateMsg(ctx context.Context, message msg2.DelayMessage, dst sharding.DST) error {
	data := message.Encode()
	return m.dao.CreateMsg(ctx, dao.DelayMsg{
		Key:      message.Key,
		Biz:      message.Biz,
		SendTime: message.SendTime,
		Data:     data,
		Status:   dao.MsgStatusInit,
	}, dst)
}

func (m *MsgRepository) FindMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]msg2.Message, error) {
	msgs, err := m.dao.FindMsgs(ctx, offset, limit, dst)
	if err != nil {
		return nil, err
	}
	res := make([]msg2.Message, 0, len(msgs))
	for _, msg := range msgs {
		var delayMsg msg2.Message
		err1 := json.Unmarshal(msg.Data, &delayMsg)
		if err1 != nil {
			m.logger.Error("解析消息失败",
				slog.Any("key", msg.Key),
				slog.Any("err", err1))
			continue
		}
		delayMsg.ID = msg.ID
		delayMsg.SendCount = msg.SendCount
		res = append(res, delayMsg)
	}
	return res, nil
}

func (m *MsgRepository) UpdateMsg(ctx context.Context, key string, fields map[string]any, dst sharding.DST) error {
	return m.dao.UpdateMsg(ctx, key, fields, dst)
}
