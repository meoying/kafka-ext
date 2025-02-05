package repository

import (
	"context"
	msg2 "github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/sharding"
)

type MessageRepository interface {
	CreateMsg(ctx context.Context, message msg2.DelayMessage, dst sharding.DST) error
	FindMsgs(ctx context.Context, offset, limit int, dst sharding.DST) ([]msg2.Message, error)
	UpdateMsg(ctx context.Context, key string, fields map[string]any, dst sharding.DST) error
}
