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

func (m *MsgDAO) CreateMsg(ctx context.Context, table string, message DelayMsg) error {
	now := time.Now().UnixMilli()
	message.Ctime = now
	message.Utime = now
	return m.db.WithContext(ctx).Table(table).Create(&message).Error
}

func (m *MsgDAO) FindMsgs(ctx context.Context, table string, offset, limit int) ([]DelayMsg, error) {
	now := time.Now().UnixMilli()
	var res []DelayMsg
	err := m.db.WithContext(ctx).Table(table).
		Where("send_time <= ? AND status = ?", now, MsgStatusInit).
		Offset(offset).Limit(limit).Find(&res).Error
	return res, err
}

func (m *MsgDAO) UpdateMsg(ctx context.Context, table string, key string, fields map[string]any) error {
	res := m.db.WithContext(ctx).Table(table).Where(DelayMsg{Key: key}).Updates(fields)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}
