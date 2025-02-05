package main

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/meoying/kafka-ext/config"
	consumer2 "github.com/meoying/kafka-ext/internal/consumer"
	job2 "github.com/meoying/kafka-ext/internal/job"
	"github.com/meoying/kafka-ext/internal/pkg/lock"
	"github.com/meoying/kafka-ext/internal/pkg/lock/gorm"
	"github.com/meoying/kafka-ext/internal/repository"
	dao2 "github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"github.com/meoying/kafka-ext/internal/sharding/strategy"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	err := initViper()
	if err != nil {
		panic(err)
	}

	var c config.Config
	err = initConfig(&c)
	if err != nil {
		panic(err)
	}

	dbs, err := initDB(c)
	if err != nil {
		panic(err)
	}

	cfg := sarama.NewConfig()
	consumer, err := initConsumer(c, cfg)
	if err != nil {
		panic(err)
	}
	producer, err := initProducer(c, cfg)
	if err != nil {
		panic(err)
	}

	sharding, err := initSharding(c)
	if err != nil {
		panic(err)
	}

	lockCli, err := initLockClient(c)
	if err != nil {
		panic(err)
	}

	dao := dao2.NewMsgDAO(dbs)
	repo := repository.NewMsgRepository(dao)

	consumerSvc := service.NewConsumerService(repo, sharding)
	producerSvc := service.NewProducerService(producer, repo)

	delayConsumer := consumer2.NewDelayConsumer(consumerSvc)
	scheduler := job2.NewScheduler(sharding, producerSvc, lockCli)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 启动消费者
	go func() {
		err1 := consumer.Consume(ctx, []string{c.Kafka.Topic}, delayConsumer)
		if err1 != nil {
			cancel()
			panic(err1)
		}
	}()
	// 启动调度器，开启生产者任务
	scheduler.Start(ctx)

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

func initDB(c config.Config) (map[string]*gorm.DB, error) {
	dbs := make(map[string]*gorm.DB)
	for _, dbCfg := range c.DataSource {
		db, err := gorm.Open(mysql.Open(dbCfg.DSN))
		if err != nil {
			return nil, err
		}
		dbs[dbCfg.Name] = db
	}
	return dbs, nil
}

func initConsumer(c config.Config, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{c.Kafka.Addr},
		c.Kafka.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化消费者失败: %w", err)
	}
	return consumer, nil
}

func initProducer(c config.Config, cfg *sarama.Config) (sarama.SyncProducer, error) {
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{c.Kafka.Addr}, cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化生产者失败: %w", err)
	}
	return producer, nil
}

func initViper() error {
	viper.SetConfigFile("config/config.yaml")
	return viper.ReadInConfig()
}

func initConfig(c *config.Config) error {
	err := viper.UnmarshalKey("datasource", &c.DataSource)
	if err != nil {
		return fmt.Errorf("解析 datasource 配置失败: %w", err)
	}

	err = viper.UnmarshalKey("algorithm", &c.Algorithm)
	if err != nil {
		return fmt.Errorf("解析 algorithm 配置失败: %w", err)
	}

	err = viper.UnmarshalKey("lock", &c.Lock)
	if err != nil {
		return fmt.Errorf("解析 lock 配置失败: %w", err)
	}

	err = viper.UnmarshalKey("kafka", &c.Kafka)
	if err != nil {
		return fmt.Errorf("解析 kafka 配置失败: %w", err)
	}
	return nil
}

func initSharding(c config.Config) (*sharding2.Sharding, error) {
	var algorithm sharding2.Strategy
	switch c.Algorithm.Type {
	case "hash":
		dbPattern := strategy.Pattern{
			Base:     c.Algorithm.Hash.DBPattern.Base,
			Name:     c.Algorithm.Hash.DBPattern.Name,
			Sharding: c.Algorithm.Hash.DBPattern.Sharding,
		}
		tablePattern := strategy.Pattern{
			Base:     c.Algorithm.Hash.TablePattern.Base,
			Name:     c.Algorithm.Hash.TablePattern.Name,
			Sharding: c.Algorithm.Hash.TablePattern.Sharding,
		}
		algorithm = strategy.NewHashSharding(dbPattern, tablePattern)
	case "range":
		// TODO: 未实现
		panic("未实现")
	default:
		return nil, fmt.Errorf("未知的分库分表算法类型 %s", c.Algorithm.Type)
	}
	return sharding2.NewSharding(algorithm), nil
}

func initLockClient(c config.Config) (lock.Client, error) {
	switch c.Lock.Type {
	case "gorm":
		db, err := gorm.Open(mysql.Open(c.Lock.GORM.DSN))
		if err != nil {
			return nil, fmt.Errorf("初始化分布式锁的 db 失败 %w", err)
		}
		cli := glock.NewClient(db)
		err = cli.InitTable()
		return cli, err
	default:
		return nil, fmt.Errorf("不支持的分布式锁类型 %s", c.Lock.Type)
	}
}
