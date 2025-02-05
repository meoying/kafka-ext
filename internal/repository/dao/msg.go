package dao

import (
	"context"
	"fmt"
	"github.com/meoying/kafka-ext/internal/sharding"
	"gorm.io/gorm"
	"time"
)

type MsgDAO struct {
	dbs map[string]*gorm.DB
}

func NewMsgDAO(dbs map[string]*gorm.DB) *MsgDAO {
	return &MsgDAO{dbs: dbs}
}

func (m *MsgDAO) CreateMsg(ctx context.Context, message DelayMsg, dst sharding.DST) error {
	db, ok := m.dbs[dst.DB]
	if !ok {
		return fmt.Errorf("不存在的db %s", dst.DB)
	}
	now := time.Now().UnixMilli()
	message.Ctime = now
	message.Utime = now
	return db.WithContext(ctx).Table(dst.Table).Create(&message).Error
}

func (m *MsgDAO) FindMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]DelayMsg, error) {
	db, ok := m.dbs[dst.DB]
	if !ok {
		return nil, fmt.Errorf("不存在的db %s", dst.DB)
	}
	now := time.Now().UnixMilli()
	var res []DelayMsg
	err := db.WithContext(ctx).Table(dst.Table).
		Where("send_time <= ? AND status = ?", now, MsgStatusInit).
		Offset(offset).Limit(limit).Find(&res).Error
	return res, err
}

func (m *MsgDAO) UpdateMsg(ctx context.Context, key string, fields map[string]any, dst sharding.DST) error {
	db, ok := m.dbs[dst.DB]
	if !ok {
		return fmt.Errorf("不存在的db %s", dst.DB)
	}
	return db.WithContext(ctx).Table(dst.Table).Where(DelayMsg{Key: key}).Updates(fields).Error
}
