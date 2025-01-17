package producer

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/meoying/kafka-ext/internal/job"
	dlock "github.com/meoying/kafka-ext/internal/lock"
	glock "github.com/meoying/kafka-ext/internal/lock/gorm"
	"github.com/meoying/kafka-ext/internal/msg"
	"github.com/meoying/kafka-ext/internal/repository"
	"github.com/meoying/kafka-ext/internal/repository/dao"
	"github.com/meoying/kafka-ext/internal/service"
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
	db         *gorm.DB
	lockClient dlock.Client
}

func TestProducer(t *testing.T) {
	suite.Run(t, new(ProducerTestSuite))
}

func (s *ProducerTestSuite) SetupSuite() {
	db, err := gorm.Open(mysql.Open("root:root@tcp(localhost:13316)/kafka_ext?charset=utf8mb4&collation=utf8mb4_general_ci&parseTime=True&loc=Local&timeout=1s&readTimeout=3s&writeTimeout=3s"))
	require.NoError(s.T(), err)
	s.db = db

	err = db.AutoMigrate(&dao.DelayMsg{})
	require.NoError(s.T(), err)

	lockClient := glock.NewClient(s.db)
	err = lockClient.InitTable()
	require.NoError(s.T(), err)
	s.lockClient = lockClient
}

func (s *ProducerTestSuite) TearDownTest() {
	err := s.db.Exec("TRUNCATE TABLE delay_msgs").Error
	require.NoError(s.T(), err)

	err = s.db.Exec("TRUNCATE TABLE distributed_locks").Error
	require.NoError(s.T(), err)
}

func (s *ProducerTestSuite) TestProducer() {
	testCases := []struct {
		name   string
		before func()
		mock   func(ctrl *gomock.Controller) sarama.SyncProducer
		after  func()
	}{
		{
			name: "发送成功",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(gomock.Any()).
					Return(1, 1, nil)
				return producer
			},
			before: func() {
				now := time.Now()
				err := s.db.Create(&dao.DelayMsg{
					Key: "key1",
					Biz: "test-biz",
					Data: msg.DelayMessage{
						Key:      "key1",
						Biz:      "test-biz",
						Content:  "第一条消息",
						BizTopic: "biz-topic",
						SendTime: now.Add(time.Second).UnixMilli(),
					}.Encode(),
					SendTime:  now.Add(time.Second).UnixMilli(),
					Status:    dao.MsgStatusInit,
					SendCount: 0,
					Ctime:     now.UnixMilli(),
					Utime:     now.UnixMilli(),
				}).Error
				require.NoError(s.T(), err)
			},
			after: func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				var res dao.DelayMsg
				err := s.db.WithContext(ctx).Where(dao.DelayMsg{Key: "key1"}).First(&res).Error
				cancel()
				require.NoError(s.T(), err)
				assert.Equal(s.T(), dao.MsgStatusSuccess, res.Status)
			},
		},
		{
			name: "发送失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(&sarama.ProducerMessage{
					Topic:     "biz-topic",
					Partition: 0,
					Key:       sarama.StringEncoder("key2"),
					Value:     sarama.StringEncoder("第二条消息"),
				}).
					Return(0, 0, errors.New("mock error"))
				return producer
			},
			before: func() {
				now := time.Now()
				err := s.db.Create(&dao.DelayMsg{
					Key: "key2",
					Biz: "test-biz",
					Data: msg.DelayMessage{
						Key:      "key2",
						Biz:      "test-biz",
						Content:  "第二条消息",
						BizTopic: "biz-topic",
						SendTime: now.Add(time.Second).UnixMilli(),
					}.Encode(),
					SendTime: now.Add(time.Second).UnixMilli(),
					Status:   dao.MsgStatusInit,
					// 再发一次就失败了
					SendCount: 2,
					Ctime:     now.UnixMilli(),
					Utime:     now.UnixMilli(),
				}).Error
				require.NoError(s.T(), err)
			},
			after: func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				var res dao.DelayMsg
				err := s.db.WithContext(ctx).Where(dao.DelayMsg{Key: "key2"}).First(&res).Error
				cancel()
				require.NoError(s.T(), err)
				assert.Equal(s.T(), dao.MsgStatusFail, res.Status)
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(s.T())
			defer ctrl.Finish()

			tc.before()

			producer := tc.mock(ctrl)
			dbDAO := dao.NewMsgDAO(s.db)
			repo := repository.NewMsgRepository(dbDAO)
			svc := service.NewProducerService(producer, repo, s.lockClient)
			task := job.NewDelayProducerJob(svc)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			task.Run(ctx)
			<-ctx.Done()

			tc.after()
		})
	}
}
