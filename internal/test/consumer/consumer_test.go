package consumer

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	consumer2 "github.com/meoying/kafka-ext/internal/consumer"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log/slog"
	"testing"
	"time"
)

type ConsumerTestSuite struct {
	suite.Suite
	db       *gorm.DB
	logger   *slog.Logger
	producer sarama.SyncProducer
}

func TestConsumer(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}

func (s *ConsumerTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/kafka_ext?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
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

	s.logger = slog.Default()
}

func (s *ConsumerTestSuite) TearDownTest() {
	err := s.db.Exec("TRUNCATE TABLE delay_msgs").Error
	require.NoError(s.T(), err)
}

func (s *ConsumerTestSuite) TestConsumer() {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_1_0_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	consumer, err := sarama.NewConsumerGroup([]string{"localhost:9094"},
		"test_consumer", cfg)
	require.NoError(s.T(), err)
	defer consumer.Close()
	// 5秒内消费完
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	dbDAO := dao.NewMsgDAO(s.db)
	repo := repository.NewMsgRepository(dbDAO, s.logger)
	svc := service.NewConsumerService(repo)
	delayConsumer := consumer2.NewDelayConsumer(svc, s.logger)

	go func() {
		err1 := consumer.Consume(ctx, []string{"test_topic"}, delayConsumer)
		assert.NoError(s.T(), err1)
	}()

	// 往kafka中发几条消息
	err = s.producer.SendMessages(produceMsg())
	require.NoError(s.T(), err)

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
