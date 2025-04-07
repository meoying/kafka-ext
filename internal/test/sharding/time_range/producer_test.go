package time_range

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/meoying/dlock-go"
	glock "github.com/meoying/dlock-go/gorm"
	"github.com/meoying/kafka-ext/config"
	"github.com/meoying/kafka-ext/internal/job"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
	sharding2 "github.com/meoying/kafka-ext/internal/sharding"
	"github.com/meoying/kafka-ext/internal/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

type ProducerTestSuite struct {
	suite.Suite
	dbs        map[string]*gorm.DB
	lockClient dlock.Client
	dispatcher *sharding2.Dispatcher
}

func TestProducer(t *testing.T) {
	suite.Run(t, new(ProducerTestSuite))
}

func (s *ProducerTestSuite) createTable(dbName, tableName string) {
	sql := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s LIKE kafka_ext.delay_msgs
`, dbName, tableName)
	err := s.dbs[db].Exec(sql).Error
	require.NoError(s.T(), err)
}

func (s *ProducerTestSuite) SetupSuite() {
	var c config.Config
	initCfg(s.T(), &c)

	dispatcher, err := initSharding(s.T(), c)
	require.NoError(s.T(), err)
	s.dispatcher = dispatcher

	// 初始化数据库连接
	s.dbs = make(map[string]*gorm.DB)
	db0, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.dbs[db] = db0

	// 分布式锁
	lockClient := glock.NewCASFirstClient(s.dbs[db])
	err = lockClient.InitTable()
	require.NoError(s.T(), err)
	s.lockClient = lockClient
}

func (s *ProducerTestSuite) TearDownTest() {
	err := s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
	require.NoError(s.T(), err)
	err = s.dbs[db].Exec("TRUNCATE TABLE delay_msgs_tab_2").Error
	require.NoError(s.T(), err)

	err = s.dbs[db].Exec("TRUNCATE TABLE distributed_locks").Error
	require.NoError(s.T(), err)
}

func (s *ProducerTestSuite) TestProducer() {
	testCases := []struct {
		name   string
		before func(repo repository.MessageRepository)
		mock   func(ctrl *gomock.Controller) sarama.SyncProducer
		after  func()
	}{
		{
			name: "发送成功",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(&sarama.ProducerMessage{
					Topic:     "biz-topic",
					Partition: 0,
					Key:       sarama.StringEncoder("123"),
					Value:     sarama.StringEncoder("消息内容"),
				}).Times(3).Return(int32(1), int64(1), nil)
				return producer
			},
			before: func(repo repository.MessageRepository) {
				err := repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "123", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
				err = repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "123", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
				err = repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "123", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
			},
			after: func() {
				ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel1()
				var res []dao.DelayMsg
				err := s.dbs[db].Table(table0).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "123", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 1, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusSuccess, res[0].Status)

				err = s.dbs[db].Table(table1).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "123", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 1, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusSuccess, res[0].Status)

				err = s.dbs[db].Table(table2).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "123", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 1, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusSuccess, res[0].Status)
			},
		},
		{
			name: "发送失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(&sarama.ProducerMessage{
					Topic:     "biz-topic",
					Partition: 0,
					Key:       sarama.StringEncoder("234"),
					Value:     sarama.StringEncoder("消息内容"),
				}).AnyTimes().Return(int32(0), int64(0), errors.New("mock error"))
				return producer
			},
			before: func(repo repository.MessageRepository) {
				err := repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "234", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
				err = repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "234", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
				err = repo.CreateMsg(context.Background(), msg.DelayMessage{Key: "234", Biz: "bizA", BizTopic: "biz-topic", Content: "消息内容"})
				require.NoError(s.T(), err)
			},
			after: func() {
				ctx1, cancel1 := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel1()
				var res []dao.DelayMsg
				err := s.dbs[db].Table(table0).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "234", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 3, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusFail, res[0].Status)

				err = s.dbs[db].Table(table1).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "234", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 3, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusFail, res[0].Status)

				err = s.dbs[db].Table(table2).WithContext(ctx1).Find(&res).Error
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), 1, len(res))
				assert.Equal(s.T(), "bizA", res[0].Biz)
				assert.Equal(s.T(), "234", res[0].Key)
				assert.True(s.T(), res[0].Utime > 0)
				assert.Equal(s.T(), 3, res[0].SendCount)
				assert.Equal(s.T(), dao.MsgStatusFail, res[0].Status)
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(s.T())
			defer ctrl.Finish()

			producer := tc.mock(ctrl)
			manager := dao.NewGormCreator(s.dbs)
			repo := repository.NewMsgRepository(s.dispatcher, manager)
			svc := service.NewProducerService(producer, repo)
			// 这里必须通过 repo 存储消息到分片上，这样调度时才能获得所有分片
			tc.before(repo)

			scheduler := job.NewScheduler(s.dispatcher, svc, s.lockClient, s.dbs)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			scheduler.Start(ctx)
			<-ctx.Done()

			tc.after()
		})
	}
}
