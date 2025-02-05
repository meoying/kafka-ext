package service

import (
	"context"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/sharding"
)

type ConsumerService struct {
	repo     repository.MessageRepository
	sharding *sharding.Sharding
}

func NewConsumerService(repo repository.MessageRepository, sharding *sharding.Sharding) *ConsumerService {
	return &ConsumerService{repo: repo, sharding: sharding}
}

func (m *ConsumerService) StoreMsg(ctx context.Context, message msg.DelayMessage) error {
	dst := m.sharding.Route(message)
	return m.repo.CreateMsg(ctx, message, dst)
}
