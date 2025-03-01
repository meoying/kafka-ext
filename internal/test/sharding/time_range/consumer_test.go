package time_range

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/benbjohnson/clock"
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
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	db     = "kafka_ext"
	table0 = "delay_msgs_tab_0"
	table1 = "delay_msgs_tab_1"
	table2 = "delay_msgs_tab_2"
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

func initCfg(t *testing.T, c *config.Config) {
	viper.SetConfigFile("config/config.yaml")
	err := viper.ReadInConfig()
	require.NoError(t, err)

	err = viper.UnmarshalKey("datasource", &c.DataSource)
	require.NoError(t, err)
	require.Equal(t, 1, len(c.DataSource))
	require.Equal(t, db, c.DataSource[0].Name)

	err = viper.UnmarshalKey("sharding", &c.Sharding)
	require.NoError(t, err)

}

func (s *ConsumerTestSuite) SetupSuite() {
	var c config.Config
	initCfg(s.T(), &c)

	dispatcher, err := initSharding(s.T(), c)
	require.NoError(s.T(), err)
	s.dispatcher = dispatcher

	// 初始化数据库
	s.dbs = make(map[string]*gorm.DB)
	db0, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.dbs[db] = db0

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, cfg)
	require.NoError(s.T(), err)
	s.producer = producer
}

func (s *ConsumerTestSuite) TearDownTest() {
	err := s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
	require.NoError(s.T(), err)
	err = s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_2").Error
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

	// 断言分片上有中有数据
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel1()
	var res []dao.DelayMsg
	err = s.dbs[db].Table(table0).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), "bizA", res[0].Biz)
	assert.Equal(s.T(), "123", res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	err = s.dbs[db].Table(table2).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), "bizA", res[0].Biz)
	assert.Equal(s.T(), "123", res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)

	err = s.dbs[db].Table(table2).WithContext(ctx1).Find(&res).Error
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 1, len(res))
	assert.Equal(s.T(), "bizA", res[0].Biz)
	assert.Equal(s.T(), "123", res[0].Key)
	assert.True(s.T(), res[0].Utime > 0)
}

func produceMsg() []*sarama.ProducerMessage {
	return []*sarama.ProducerMessage{
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg()),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg()),
		},
		{
			Topic: "test_topic",
			Value: sarama.StringEncoder(genMsg()),
		},
	}
}

func genMsg() string {
	dMsg := msg.DelayMessage{
		Key:      "123",
		Biz:      "bizA",
		Content:  "msg-content",
		BizTopic: "biz_topic",
		SendTime: time.Now().Add(time.Second * 3).UnixMilli(),
	}
	return string(dMsg.Encode())
}

func initSharding(t *testing.T, c config.Config) (*sharding2.Dispatcher, error) {
	require.True(t, len(c.Sharding.BizStrategy) > 0)

	const (
		StrategyTimeRange = "time_range"
	)
	// 每个 biz 使用的 strategy
	bizStrategy := make(map[string]string)
	strategies := make(map[string]sharding2.Strategy)

	// 初始化所有用到的 strategy
	for _, bs := range c.Sharding.BizStrategy {
		switch bs.Strategy {
		case StrategyTimeRange:
			cfg, ok := c.Sharding.Strategy[StrategyTimeRange]
			require.True(t, ok)

			s, err := initTimeRangeStrategy(t, cfg)
			require.NoError(t, err)

			mockTime := &MockTime{clock: clock.NewMock()}
			s.Now = mockTime.Time

			strategies[s.Name()] = s
			for _, biz := range bs.Biz {
				bizStrategy[biz] = s.Name()
			}
		default:
		}
	}

	require.True(t, len(strategies) == 1)
	wantBizStrategy := map[string]string{
		"bizA": "time_range",
		"bizB": "time_range",
	}
	require.Equal(t, wantBizStrategy, bizStrategy)

	return sharding2.NewDispatcher(bizStrategy, strategies), nil
}

func initTimeRangeStrategy(t *testing.T, config any) (strategy.TimeRange, error) {
	cfg, ok := config.(map[string]any)
	require.True(t, ok)

	db, ok := cfg["db"].(string)
	require.True(t, ok)

	tablePattern, ok := cfg["tablepattern"].(string)
	require.True(t, ok)

	interval, ok := cfg["interval"].(string)
	require.True(t, ok)

	i, err := strconv.Atoi(interval)
	require.NoError(t, err)

	startTime, ok := cfg["starttime"].(string)
	require.True(t, ok)

	const shortForm = "2006-01-02"
	t1, err := time.Parse(shortForm, startTime)
	require.NoError(t, err)

	return strategy.NewTimeRange(db, tablePattern, t1, i), nil
}

type MockTime struct {
	clock *clock.Mock
	count int
	mu    sync.Mutex
}

func (m *MockTime) Time() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.count == 0 {
		// 开始时间当天
		m.clock.Set(time.Date(2025, 2, 20, 0, 0, 0, 0, time.UTC))
		m.count++
		return m.clock.Now()
	}
	if m.count == 1 {
		// 开始时间的第二天
		m.clock.Set(time.Date(2025, 2, 21, 0, 0, 0, 0, time.UTC))
		m.count++
		return m.clock.Now()
	}
	// 开始时间的第三天
	m.clock.Set(time.Date(2025, 2, 22, 0, 0, 0, 0, time.UTC))
	return m.clock.Now()
}
