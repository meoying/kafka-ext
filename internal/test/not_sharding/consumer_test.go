package not_sharding

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/meoying/kafka-ext/config"
	consumer2 "github.com/meoying/kafka-ext/internal/consumer"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"github.com/meoying/kafka-ext/internal/sharding/strategy"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

const (
	dbName    = "kafka_ext"
	tableName = "delay_msgs"
)

type ConsumerTestSuite struct {
	suite.Suite
	db         *gorm.DB
	producer   sarama.SyncProducer
	dispatcher *sharding2.Dispatcher
}

func TestConsumer(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}

func initCfg(t *testing.T, c *config.Config) {
	viper.SetConfigFile("config/config.yaml")
	err := viper.ReadInConfig()
	require.NoError(t, err)

	err = viper.UnmarshalKey("datasource", &c.DataSource)
	require.NoError(t, err)
	require.Equal(t, 1, len(c.DataSource))
	require.Equal(t, dbName, c.DataSource[0].Name)

	err = viper.UnmarshalKey("sharding", &c.Sharding)
	require.NoError(t, err)
}

func (s *ConsumerTestSuite) SetupSuite() {
	var c config.Config
	initCfg(s.T(), &c)

	dispatcher, err := initSharding(s.T(), c)
	require.NoError(s.T(), err)
	s.dispatcher = dispatcher

	db, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.db = db
	err = s.db.AutoMigrate(&dao.DelayMsg{})
	require.NoError(s.T(), err)

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, cfg)
	require.NoError(s.T(), err)

	s.producer = producer
}

func (s *ConsumerTestSuite) TearDownTest() {
	err := s.db.Exec("TRUNCATE TABLE delay_msgs").Error
	require.NoError(s.T(), err)
}

func (s *ConsumerTestSuite) TestConsumer() {
	dbs := map[string]*gorm.DB{
		dbName: s.db,
	}

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()

	manager := dao.NewGormCreator(dbs)
	repo := repository.NewMsgRepository(s.dispatcher, manager)
	svc := service.NewConsumerService(repo)
	delayConsumer := consumer2.NewDelayConsumer(svc)

	// 往kafka中发几条消息
	err = s.producer.SendMessages(produceMsg())
	require.NoError(s.T(), err)

	// 5秒内消费完
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	go func() {
		err1 := consumer.Consume(ctx, []string{"test_topic"}, delayConsumer)
		assert.NoError(s.T(), err1)
	}()

	<-ctx.Done()

	// 断言数据库中有数据
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel1()
	var res []dao.DelayMsg
	err = s.db.WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 3, len(res))
}

func produceMsg() []*sarama.ProducerMessage {
	return []*sarama.ProducerMessage{
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(1)),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(2)),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(3)),
		},
	}
}

func genMsg(seq int) string {
	dMsg := msg.DelayMessage{
		Key:      fmt.Sprintf("key-%d", seq),
		Biz:      "bizA",
		Content:  fmt.Sprintf("msg-content-%d", seq),
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}

func initSharding(t *testing.T, c config.Config) (*sharding2.Dispatcher, error) {
	if len(c.Sharding.BizStrategy) <= 0 {
		return nil, fmt.Errorf("bizStrategy 配置为空")
	}

	const (
		StrategyHash = "hash"
	)
	// 每个 biz 使用的 strategy
	bizStrategy := make(map[string]string)
	strategies := make(map[string]sharding2.Strategy)

	// 初始化所有用到的 strategy
	for _, bs := range c.Sharding.BizStrategy {
		switch bs.Strategy {
		case StrategyHash:
			hashCfg, ok := c.Sharding.Strategy[StrategyHash]
			require.True(t, ok)
			hash, err := initHashStrategy(t, hashCfg)
			require.NoError(t, err)
			strategies[hash.Name()] = hash
			for _, biz := range bs.Biz {
				bizStrategy[biz] = hash.Name()
			}
		default:
			return nil, fmt.Errorf("未知的策略类型 %s", bs.Strategy)
		}
	}

	require.True(t, len(strategies) == 1)
	wantBizStrategy := map[string]string{
		"bizA": "hash",
		"bizB": "hash",
	}
	require.Equal(t, wantBizStrategy, bizStrategy)

	return sharding2.NewDispatcher(bizStrategy, strategies), nil
}

func initHashStrategy(t *testing.T, hashCfg any) (strategy.Hash, error) {
	const (
		cfgDBdbPattern  = "dbpattern"
		cfgTablePattern = "tablepattern"
	)

	cfg, ok := hashCfg.(map[string]any)
	require.True(t, ok)

	dbCfg, ok := cfg[cfgDBdbPattern]
	require.True(t, ok)
	var dbPattern strategy.HashPattern
	data, _ := json.Marshal(&dbCfg)
	err := json.Unmarshal(data, &dbPattern)
	require.NoError(t, err)

	tableCfg, ok := cfg[cfgTablePattern]
	require.True(t, ok)
	var tablePattern strategy.HashPattern
	data, _ = json.Marshal(&tableCfg)
	err = json.Unmarshal(data, &tablePattern)
	require.NoError(t, err)
	// 断言配置信息
	require.True(t, !dbPattern.Sharding)
	require.True(t, !tablePattern.Sharding)
	require.Equal(t, dbName, dbPattern.Name)
	require.Equal(t, tableName, tablePattern.Name)

	return strategy.NewHash(dbPattern, tablePattern), nil
}
