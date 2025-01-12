package job

import (
	"context"
	"github.com/robfig/cron/v3"
	"log/slog"
	"time"
)

type DelayProducerJobAdapter struct {
	job    *DelayProducerJob
	logger *slog.Logger
}

func NewDelayProducerJobAdapter(job *DelayProducerJob, logger *slog.Logger) cron.Job {
	return &DelayProducerJobAdapter{job: job, logger: logger}
}

func (d *DelayProducerJobAdapter) Run() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	err := d.job.Run(ctx)
	cancel()
	if err != nil {
		d.logger.Error("运行发送消息任务失败", slog.Any("err", err))
	}
}
