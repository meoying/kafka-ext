package repository

import (
	"context"
	"encoding/json"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	dao2 "github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/sharding"
	"log/slog"
)

type MsgRepository struct {
	logger     *slog.Logger
	daoCreator dao2.Creator
	// 缓存 dao
	daos       map[string]dao2.MessageDAO
	dispatcher *sharding.Dispatcher
}

func NewMsgRepository(dispatcher *sharding.Dispatcher, creator dao2.Creator) *MsgRepository {
	return &MsgRepository{
		dispatcher: dispatcher,
		daoCreator: creator,
		daos:       make(map[string]dao2.MessageDAO),
		logger:     slog.Default()}
}

func (m *MsgRepository) CreateMsg(ctx context.Context, message msg2.DelayMessage) error {
	dst, err := m.dispatcher.Dispatch(message)
	if err != nil {
		return err
	}
	dao, err := m.getDAO(dst.DB)
	if err != nil {
		return err
	}

	return dao.CreateMsg(ctx, dst.Table, dao2.DelayMsg{
		Key:      message.Key,
		Biz:      message.Biz,
		SendTime: message.SendTime,
		Data:     message.Encode(),
		Status:   dao2.MsgStatusInit,
	})
}

func (m *MsgRepository) FindMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]msg2.Message, error) {
	dao, err := m.getDAO(dst.DB)
	if err != nil {
		return nil, err
	}
	msgs, err := dao.FindMsgs(ctx, dst.Table, offset, limit)
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
	dao, err := m.getDAO(dst.DB)
	if err != nil {
		return err
	}
	return dao.UpdateMsg(ctx, dst.Table, key, fields)
}

func (m *MsgRepository) getDAO(db string) (dao2.MessageDAO, error) {
	dao, ok := m.daos[db]
	if !ok {
		d, err := m.daoCreator.Create(db)
		if err != nil {
			return nil, err
		}
		dao = d
	}
	return dao, nil
}
