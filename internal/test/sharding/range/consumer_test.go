package not_sharding

import (
	"context"
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
	db = "kafka_ext"
)

var table string

type ConsumerTestSuite struct {
	suite.Suite
	dbs      map[string]*gorm.DB
	producer sarama.SyncProducer
	sharding *sharding2.Sharding
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
	require.Equal(t, db, c.DataSource[0].Name)

	err = viper.UnmarshalKey("algorithm", &c.Algorithm)
	require.NoError(t, err)

	// 断言分库分表的配置信息
	require.Equal(t, "range", c.Algorithm.Type)
	require.Equal(t, db, c.Algorithm.Range.DB)
	require.Equal(t, "delay_msgs_%s", c.Algorithm.Range.Table)
	require.Equal(t, 1, c.Algorithm.Range.Interval)
	require.Equal(t, 7, c.Algorithm.Range.Retention)
}

func (s *ConsumerTestSuite) SetupSuite() {
	var c config.Config
	initCfg(s.T(), &c)

	// 初始化数据库
	s.dbs = make(map[string]*gorm.DB)
	db0, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.dbs[db] = db0

	// 初始化分库分表
	algorithm := strategy.NewTimeRange(c.Algorithm.Range.DB, c.Algorithm.Range.Table,
		c.Algorithm.Range.Retention, c.Algorithm.Range.Interval)
	sharding := sharding2.NewSharding(algorithm)
	s.sharding = sharding

	// 每次测试前需要将分片表创建出来
	dsts := sharding.GetEffectiveTables()
	for _, dst := range dsts {
		s.createTable(dst.DB, dst.Table)
	}
	// 第一个元素是最新的分片，也是当前消息存储的位置
	table = dsts[0].Table

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, cfg)
	require.NoError(s.T(), err)
	s.producer = producer
}

func (s *ConsumerTestSuite) createTable(dbName, tableName string) {
	sql := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s LIKE kafka_ext.delay_msgs
`, dbName, tableName)
	err := s.dbs[db].Exec(sql).Error
	require.NoError(s.T(), err)
}

func (s *ConsumerTestSuite) TearDownTest() {
	err := s.dbs[db].Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
	require.NoError(s.T(), err)
}

func (s *ConsumerTestSuite) TestConsumer() {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()

	dbDAO := dao.NewMsgDAO(s.dbs)
	repo := repository.NewMsgRepository(dbDAO)
	svc := service.NewConsumerService(repo, s.sharding)
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
	err = s.dbs[db].Table(table).WithContext(ctx1).Find(&res).Error
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
		Biz:      "test_biz",
		Content:  fmt.Sprintf("msg-content-%d", seq),
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}
