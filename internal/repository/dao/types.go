package dao

import "context"

type MessageDAO interface {
	CreateMsg(ctx context.Context, message DelayMsg) error
	FindMsgs(ctx context.Context, offset, limit int) ([]DelayMsg, error)
	UpdateMsg(ctx context.Context, key string, fields map[string]any) error
}
