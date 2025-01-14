package job

import (
	"context"
	"github.com/meoying/kafka-ext/internal/service"
	"log/slog"
)

// DelayProducerJob 延迟消息生产者
type DelayProducerJob struct {
	svc    *service.ProducerJob
	Logger *slog.Logger
}

func NewDelayProducerJob(svc *service.ProducerJob) *DelayProducerJob {
	return &DelayProducerJob{svc: svc, Logger: slog.Default()}
}

func (d *DelayProducerJob) Run(ctx context.Context) {
	go d.svc.Start(ctx)
}
