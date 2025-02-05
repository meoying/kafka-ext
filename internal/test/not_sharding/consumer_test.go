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
	dbName    = "kafka_ext"
	tableName = "delay_msgs"
)

type ConsumerTestSuite struct {
	suite.Suite
	db       *gorm.DB
	producer sarama.SyncProducer

	dbPattern    strategy.Pattern
	tablePattern strategy.Pattern
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

	err = viper.UnmarshalKey("algorithm", &c.Algorithm)
	require.NoError(t, err)

	// 断言是不分库分表的配置信息
	require.Equal(t, "hash", c.Algorithm.Type)
	require.True(t, !c.Algorithm.Hash.DBPattern.Sharding)
	require.True(t, !c.Algorithm.Hash.TablePattern.Sharding)
	require.Equal(t, dbName, c.Algorithm.Hash.DBPattern.Name)
	require.Equal(t, tableName, c.Algorithm.Hash.TablePattern.Name)
}

func (s *ConsumerTestSuite) SetupSuite() {
	var c config.Config
	initCfg(s.T(), &c)

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
	s.dbPattern = dbPattern
	s.tablePattern = tablePattern

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

	notSharding := strategy.NewHashSharding(s.dbPattern, s.tablePattern)
	//notSharding := strategy.NewNotSharding(dbName, tableName)
	sharding := sharding2.NewSharding(notSharding)

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()

	dbDAO := dao.NewMsgDAO(dbs)
	repo := repository.NewMsgRepository(dbDAO)
	svc := service.NewConsumerService(repo, sharding)
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
		Biz:      "test_biz",
		Content:  fmt.Sprintf("msg-content-%d", seq),
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}
