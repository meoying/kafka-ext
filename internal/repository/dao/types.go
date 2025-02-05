package dao

import (
	"context"
	"github.com/meoying/kafka-ext/internal/sharding"
)

type MessageDAO interface {
	CreateMsg(ctx context.Context, message DelayMsg, dst sharding.DST) error
	FindMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]DelayMsg, error)
	UpdateMsg(ctx context.Context, key string, fields map[string]any, dst sharding.DST) error
}
