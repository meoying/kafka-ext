package dao

import (
	"context"
)

type MessageDAO interface {
	CreateMsg(ctx context.Context, table string, message DelayMsg) error
	FindMsgs(ctx context.Context, table string, offset, limit int) ([]DelayMsg, error)
	UpdateMsg(ctx context.Context, table string, key string, fields map[string]any) error
}

// Creator MessageDAO creator
type Creator interface {
	// Create 返回一个 MessageDAO 的实例
	Create(dbName string) (MessageDAO, error)
}
