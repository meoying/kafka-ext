package job

import (
	"context"
	"github.com/meoying/kafka-ext/internal/service"
	"log/slog"
)

// DelayProducerJob 延迟消息生产者
type DelayProducerJob struct {
	svc    *service.ProducerService
	logger *slog.Logger
}

func NewDelayProducerJob(svc *service.ProducerService, logger *slog.Logger) *DelayProducerJob {
	return &DelayProducerJob{svc: svc, logger: logger}
}

func (d *DelayProducerJob) Run(ctx context.Context) error {
	return d.svc.FindAndSendMsgs(ctx)
}
