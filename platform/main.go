package main

import (
	"context"
	"github.com/IBM/sarama"
	consumer2 "github.com/meoying/kafka-ext/internal/consumer"
	job2 "github.com/meoying/kafka-ext/internal/job"
	glock "github.com/meoying/kafka-ext/internal/lock/gorm"
	"github.com/meoying/kafka-ext/internal/repository"
	dao2 "github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	db, err := initDB()
	if err != nil {
		panic(err)
	}

	cfg := sarama.NewConfig()
	consumer, err := initConsumer(cfg)
	if err != nil {
		panic(err)
	}
	producer, err := initProducer(cfg)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dao := dao2.NewMsgDAO(db)
	err = dao.InitTable()
	if err != nil {
		panic(err)
	}

	repo := repository.NewMsgRepository(dao)
	consumerSvc := service.NewConsumerService(repo)
	delayConsumer := consumer2.NewDelayConsumer(consumerSvc)

	go func() {
		err1 := consumer.Consume(ctx, []string{"kafka_ext_delay_msg"}, delayConsumer)
		if err1 != nil {
			cancel()
			panic(err1)
		}
	}()

	producerSvc := service.NewProducerService(producer, repo)

	lockCli := glock.NewClient(db)
	err = lockCli.InitTable()
	job := job2.NewDelayProducerJob(producerSvc, lockCli)
	job.Run(ctx)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigchan // 阻塞等待信号
		cancel()
	}()

	<-ctx.Done()
	// 等待全部goroutine退出
	time.Sleep(time.Second * 5)
}

func initDB() (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/kafka_ext?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	return db, err
}

func initConsumer(cfg *sarama.Config) (sarama.ConsumerGroup, error) {
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"delay_consumer", cfg)
	return consumer, err
}

func initProducer(cfg *sarama.Config) (sarama.SyncProducer, error) {
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, cfg)
	return producer, err
}
