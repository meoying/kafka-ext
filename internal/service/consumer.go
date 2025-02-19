package service

import (
	"context"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
)

type ConsumerService struct {
	repo repository.MessageRepository
}

func NewConsumerService(repo repository.MessageRepository) *ConsumerService {
	return &ConsumerService{repo: repo}
}

func (m *ConsumerService) StoreMsg(ctx context.Context, message msg.DelayMessage) error {
	return m.repo.CreateMsg(ctx, message)
}
