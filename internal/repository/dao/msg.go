package dao

import (
	"context"
	"gorm.io/gorm"
	"time"
)

type MsgDAO struct {
	db *gorm.DB
}

func NewMsgDAO(db *gorm.DB) *MsgDAO {
	return &MsgDAO{db: db}
}

func (m *MsgDAO) CreateMsg(ctx context.Context, message DelayMsg) error {
	now := time.Now().UnixMilli()
	message.Ctime = now
	message.Utime = now
	return m.db.WithContext(ctx).Create(&message).Error
}

func (m *MsgDAO) FindMsgs(ctx context.Context, offset, limit int) ([]DelayMsg, error) {
	now := time.Now().UnixMilli()
	var res []DelayMsg
	err := m.db.WithContext(ctx).Where("send_time <= ? AND status = ?", now, MsgStatusInit).
		Offset(offset).Limit(limit).Find(&res).Error
	return res, err
}

func (m *MsgDAO) UpdateMsg(ctx context.Context, key string, fields map[string]any) error {
	return m.db.WithContext(ctx).Model(&DelayMsg{}).Where(DelayMsg{Key: key}).Updates(fields).Error
}

func (m *MsgDAO) InitTable() error {
	return m.db.AutoMigrate(&DelayMsg{})
}
