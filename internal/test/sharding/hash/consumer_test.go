package hash

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
	"strings"
	"testing"
	"time"
)

const (
	db_0    = "kafka_ext_db_0"
	db_1    = "kafka_ext_db_1"
	table_0 = "delay_msgs_tab_0"
	table_1 = "delay_msgs_tab_1"
	bizA    = "bizA"
	bizB    = "bizB"
)

type ConsumerTestSuite struct {
	suite.Suite
	dbs        map[string]*gorm.DB
	producer   sarama.SyncProducer
	dispatcher *sharding2.Dispatcher
}

func TestConsumer(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}

func (s *ConsumerTestSuite) SetupSuite() {
	s.dbs = make(map[string]*gorm.DB)

	var c config.Config
	initCfg(s.T(), &c)

	dispatcher, err := initSharding(s.T(), c)
	require.NoError(s.T(), err)
	s.dispatcher = dispatcher

	db0, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.dbs[db_0] = db0

	db1, err := gorm.Open(mysql.Open(c.DataSource[1].DSN))
	require.NoError(s.T(), err)
	s.dbs[db_1] = db1

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, cfg)
	require.NoError(s.T(), err)

	s.producer = producer
}

func (s *ConsumerTestSuite) TearDownTest() {
	err := s.dbs[db_0].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db_0].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
	require.NoError(s.T(), err)

	err = s.dbs[db_1].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db_1].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
	require.NoError(s.T(), err)
}

func (s *ConsumerTestSuite) TestConsumer() {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()

	manager := dao.NewGormCreator(s.dbs)
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
	// db_0, table_0
	var res []dao.DelayMsg
	err = s.dbs[db_0].Table(table_0).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), bizA, res[0].Biz)
	assert.Equal(s.T(), table_0, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	// db_0, table_1
	err = s.dbs[db_0].Table(table_1).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), bizA, res[0].Biz)
	assert.Equal(s.T(), table_1, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel2()

	// db_1, table_0
	err = s.dbs[db_1].Table(table_0).WithContext(ctx2).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), bizB, res[0].Biz)
	assert.Equal(s.T(), table_0, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	// db_1, table_1
	err = s.dbs[db_1].Table(table_1).WithContext(ctx2).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), bizB, res[0].Biz)
	assert.Equal(s.T(), table_1, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)
}

func produceMsg() []*sarama.ProducerMessage {
	// 两个库，总共4个表，每张表一条消息
	return []*sarama.ProducerMessage{
		{
			Topic: "test_topic",
			// db0 table 0
			Value: sarama.StringEncoder(genMsg("bizA", table_0)),
		},
		{

			Topic: "test_topic",
			// db0 table 1
			Value: sarama.StringEncoder(genMsg("bizA", table_1)),
		},
		{
			Topic: "test_topic",
			// db1 table 0
			Value: sarama.StringEncoder(genMsg("bizB", table_0)),
		},
		{
			Topic: "test_topic",
			// db1 table 1
			Value: sarama.StringEncoder(genMsg("bizB", table_1)),
		},
	}
}

func genMsg(biz, table string) string {
	dMsg := msg.DelayMessage{
		Key:      table,
		Biz:      biz,
		Content:  fmt.Sprintf("msg-content-%s:%s", biz, table),
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}

func hash(key string, base int) int {
	// 返回 目标库的编号
	if strings.Contains(key, bizA) {
		return 0
	}
	if strings.Contains(key, bizB) {
		return 1
	}
	// 返回 目标表的编号
	if strings.Contains(key, table_0) {
		return 0
	}
	if strings.Contains(key, table_1) {
		return 1
	}

	return -1
}

func initCfg(t *testing.T, c *config.Config) {
	viper.SetConfigFile("config/config.yaml")
	err := viper.ReadInConfig()
	require.NoError(t, err)

	err = viper.UnmarshalKey("datasource", &c.DataSource)
	require.NoError(t, err)
	require.Equal(t, 2, len(c.DataSource))
	require.Equal(t, db_0, c.DataSource[0].Name)
	require.Equal(t, db_1, c.DataSource[1].Name)

	err = viper.UnmarshalKey("sharding", &c.Sharding)
	require.NoError(t, err)
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
			hashStrategy, err := initHashStrategy(t, hashCfg)
			// 修改哈希函数
			hashStrategy.Hash = hash
			require.NoError(t, err)
			strategies[hashStrategy.Name()] = hashStrategy
			for _, biz := range bs.Biz {
				bizStrategy[biz] = hashStrategy.Name()
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
	require.True(t, dbPattern.Sharding)
	require.True(t, tablePattern.Sharding)
	require.Equal(t, "kafka_ext_db_%d", dbPattern.Name)
	require.Equal(t, "delay_msgs_tab_%d", tablePattern.Name)

	return strategy.NewHash(dbPattern, tablePattern), nil
}
