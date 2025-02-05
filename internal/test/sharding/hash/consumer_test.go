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
	"strings"
	"testing"
	"time"
)

const (
	db_0    = "kafka_ext_db_0"
	db_1    = "kafka_ext_db_1"
	table_0 = "delay_msgs_tab_0"
	table_1 = "delay_msgs_tab_1"
)

type ConsumerTestSuite struct {
	suite.Suite
	dbs      map[string]*gorm.DB
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
	require.Equal(t, 2, len(c.DataSource))
	require.Equal(t, db_0, c.DataSource[0].Name)
	require.Equal(t, db_1, c.DataSource[1].Name)

	err = viper.UnmarshalKey("algorithm", &c.Algorithm)
	require.NoError(t, err)

	// 断言分库分表的配置信息
	require.Equal(t, "hash", c.Algorithm.Type)

	require.True(t, c.Algorithm.Hash.DBPattern.Sharding)
	require.Equal(t, 2, c.Algorithm.Hash.DBPattern.Base)
	require.Equal(t, "kafka_ext_db_%d", c.Algorithm.Hash.DBPattern.Name)

	require.Equal(t, 2, c.Algorithm.Hash.TablePattern.Base)
	require.True(t, c.Algorithm.Hash.TablePattern.Sharding)
	require.Equal(t, "delay_msgs_tab_%d", c.Algorithm.Hash.TablePattern.Name)
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

	s.dbs = make(map[string]*gorm.DB)

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
	hashSharding := strategy.NewHashSharding(s.dbPattern, s.tablePattern)
	// 修改哈希函数
	hashSharding.Hash = hash
	//notSharding := strategy.NewNotSharding(dbName, tableName)
	sharding := sharding2.NewSharding(hashSharding)

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()

	dbDAO := dao.NewMsgDAO(s.dbs)
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
	// db_0, table_0
	var res []dao.DelayMsg
	err = s.dbs[db_0].Table(table_0).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), db_0, res[0].Biz)
	assert.Equal(s.T(), table_0, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	// db_0, table_1
	err = s.dbs[db_0].Table(table_1).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), db_0, res[0].Biz)
	assert.Equal(s.T(), table_1, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel2()

	// db_1, table_0
	err = s.dbs[db_1].Table(table_0).WithContext(ctx2).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), db_1, res[0].Biz)
	assert.Equal(s.T(), table_0, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	// db_1, table_1
	err = s.dbs[db_1].Table(table_1).WithContext(ctx2).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), db_1, res[0].Biz)
	assert.Equal(s.T(), table_1, res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)
}

func produceMsg() []*sarama.ProducerMessage {
	// 两个库，总共4个表，每张表一条消息
	return []*sarama.ProducerMessage{
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(db_0, table_0)),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(db_0, table_1)),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(db_1, table_0)),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg(db_1, table_1)),
		},
	}
}

func genMsg(db, table string) string {
	dMsg := msg.DelayMessage{
		Key:      fmt.Sprintf("%s", table),
		Biz:      fmt.Sprintf("%s", db),
		Content:  fmt.Sprintf("msg-content-%s:%s", db, table),
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}

func hash(key string, base int) int {
	if strings.Contains(key, db_0) {
		return 0
	}
	if strings.Contains(key, db_1) {
		return 1
	}
	if strings.Contains(key, table_0) {
		return 0
	}
	if strings.Contains(key, table_1) {
		return 1
	}

	return -1
}
