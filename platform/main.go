package main

import (
	"context"
	"encoding/json"
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
	"strconv"
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

	consumer, err := initConsumer(c)
	if err != nil {
		panic(err)
	}
	producer, err := initProducer(c)
	if err != nil {
		panic(err)
	}

	dispatcher, err := initSharding(c)
	if err != nil {
		panic(err)
	}

	lockCli, err := initLockClient(c)
	if err != nil {
		panic(err)
	}

	manager := dao2.NewGormManager(dbs)
	repo := repository.NewMsgRepository(dispatcher, manager)

	consumerSvc := service.NewConsumerService(repo)
	producerSvc := service.NewProducerService(producer, repo)

	delayConsumer := consumer2.NewDelayConsumer(consumerSvc)
	scheduler := job2.NewScheduler(dispatcher, producerSvc, lockCli)

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

func initConsumer(c config.Config) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{c.Kafka.Addr},
		c.Kafka.GroupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化消费者失败: %w", err)
	}
	return consumer, nil
}

func initProducer(c config.Config) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
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

	err = viper.UnmarshalKey("sharding", &c.Sharding)
	if err != nil {
		return fmt.Errorf("解析 sharding 配置失败: %w", err)
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

func initSharding(c config.Config) (*sharding2.Dispatcher, error) {
	if len(c.Sharding.BizStrategy) <= 0 {
		return nil, fmt.Errorf("bizStrategy 配置为空")
	}

	const (
		StrategyHash      = "hash"
		StrategyTimeRange = "time_range"
	)
	// 每个 biz 使用的 strategy
	bizStrategy := make(map[string]string)
	strategies := make(map[string]sharding2.Strategy)

	// 初始化所有用到的 strategy
	for _, bs := range c.Sharding.BizStrategy {
		switch bs.Strategy {
		case StrategyHash:
			hashCfg, ok := c.Sharding.Strategy[StrategyHash]
			if !ok || hashCfg == nil {
				return nil, fmt.Errorf("分库分表策略 hash 配置为空")
			}
			s, err := initHashStrategy(hashCfg)
			if err != nil {
				return nil, err
			}

			strategies[s.Name()] = s
			for _, biz := range bs.Biz {
				bizStrategy[biz] = s.Name()
			}
		case StrategyTimeRange:
			cfg, ok := c.Sharding.Strategy[StrategyTimeRange]
			if !ok || cfg == nil {
				return nil, fmt.Errorf("分库分表策略 time_range 配置为空")
			}
			s, err := initTimeRangeStrategy(cfg)
			if err != nil {
				return nil, err
			}

			strategies[s.Name()] = s
			for _, biz := range bs.Biz {
				bizStrategy[biz] = s.Name()
			}
		default:
			return nil, fmt.Errorf("未知的策略类型 %s", bs.Strategy)
		}
	}
	return sharding2.NewDispatcher(bizStrategy, strategies), nil
}

func initTimeRangeStrategy(config any) (strategy.TimeRange, error) {
	cfg, ok := config.(map[string]any)
	if !ok {
		return strategy.TimeRange{}, fmt.Errorf("time_range策略配置解析失败")
	}

	db, ok := cfg["db"].(string)
	if !ok {
		return strategy.TimeRange{}, fmt.Errorf("time_range db 解析失败")
	}

	tablePattern, ok := cfg["tablepattern"].(string)
	if !ok {
		return strategy.TimeRange{}, fmt.Errorf("time_range tablePattern 解析失败")
	}

	interval, ok := cfg["interval"].(string)
	if !ok {
		return strategy.TimeRange{}, fmt.Errorf("time_range interval 解析失败")
	}

	i, err := strconv.Atoi(interval)
	if err != nil {
		return strategy.TimeRange{}, fmt.Errorf("time_range interva 解析失败 %w", err)
	}

	startTime, ok := cfg["starttime"].(string)
	if !ok {
		return strategy.TimeRange{}, fmt.Errorf("time_range startTime 解析失败")
	}

	const shortForm = "2006-01-02"
	t, err := time.Parse(shortForm, startTime)
	if err != nil {
		return strategy.TimeRange{}, fmt.Errorf("time_range startTime 解析失败 %w", err)
	}

	return strategy.NewTimeRange(db, tablePattern, t, i), nil
}

func initHashStrategy(config any) (strategy.Hash, error) {
	const (
		cfgDBdbPattern  = "dbpattern"
		cfgTablePattern = "tablepattern"
	)

	cfg, ok := config.(map[string]any)
	if !ok {
		return strategy.Hash{}, fmt.Errorf("hash 策略配置解析失败")
	}

	dbCfg, ok := cfg[cfgDBdbPattern]
	if !ok {
		return strategy.Hash{}, fmt.Errorf("hash dbPattern 不存在")
	}
	var dbPattern strategy.HashPattern
	data, _ := json.Marshal(&dbCfg)
	err := json.Unmarshal(data, &dbPattern)
	if err != nil {
		return strategy.Hash{}, fmt.Errorf("hash dbPattern 配置解析失败")
	}

	tableCfg, ok := cfg[cfgTablePattern]
	if !ok {
		return strategy.Hash{}, fmt.Errorf("hash tablePattern 不存在")
	}
	var tablePattern strategy.HashPattern
	data, _ = json.Marshal(&tableCfg)
	err = json.Unmarshal(data, &tablePattern)
	if err != nil {
		return strategy.Hash{}, fmt.Errorf("hash tablePattern 配置解析失败")
	}

	return strategy.NewHash(dbPattern, tablePattern), nil
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
