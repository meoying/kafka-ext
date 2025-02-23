package hash

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/meoying/kafka-ext/config"
	"github.com/meoying/kafka-ext/internal/job"
	"github.com/meoying/kafka-ext/internal/msg"
	dlock "github.com/meoying/kafka-ext/internal/pkg/lock"
	"github.com/meoying/kafka-ext/internal/pkg/lock/gorm"
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

func (s *ProducerTestSuite) SetupSuite() {
	s.dbs = make(map[string]*gorm.DB)

	var c config.Config
	initCfg(s.T(), &c)

	dispatcher, err := initSharding(s.T(), c)
	require.NoError(s.T(), err)
	s.dispatcher = dispatcher

	dsts := dispatcher.GetEffectiveTables()
	// 断言有4个分片
	want := []sharding2.DST{
		{
			DB:    db_0,
			Table: table_0,
		},
		{
			DB:    db_0,
			Table: table_1,
		},
		{
			DB:    db_1,
			Table: table_0,
		},
		{
			DB:    db_1,
			Table: table_1,
		},
	}
	require.Equal(s.T(), want, dsts)

	db0, err := gorm.Open(mysql.Open(c.DataSource[0].DSN))
	require.NoError(s.T(), err)
	s.dbs[db_0] = db0

	db1, err := gorm.Open(mysql.Open(c.DataSource[1].DSN))
	require.NoError(s.T(), err)
	s.dbs[db_1] = db1

	lockClient := glock.NewClient(s.dbs[db_0])
	err = lockClient.InitTable()
	require.NoError(s.T(), err)
	s.lockClient = lockClient

}

func (s *ProducerTestSuite) TearDownTest() {
	err := s.dbs[db_0].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db_0].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
	require.NoError(s.T(), err)

	err = s.dbs[db_1].Exec("TRUNCATE TABLE delay_msgs_tab_0").Error
	require.NoError(s.T(), err)
	err = s.dbs[db_1].Exec("TRUNCATE TABLE delay_msgs_tab_1").Error
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
				producer.EXPECT().SendMessage(&sarama.ProducerMessage{
					Topic:     "biz-topic",
					Partition: 0,
					Key:       sarama.StringEncoder("success_key"),
					Value:     sarama.StringEncoder("第一条消息"),
				}).Times(4).Return(int32(1), int64(1), nil)
				return producer
			},
			before: func() {
				// db0 table0
				s.insertMsg(s.dbs[db_0], "success_key", bizA, table_0)
				// db0 table1
				s.insertMsg(s.dbs[db_0], "success_key", bizA, table_1)
				// db1 table0
				s.insertMsg(s.dbs[db_1], "success_key", bizB, table_0)
				// db1 table1
				s.insertMsg(s.dbs[db_1], "success_key", bizB, table_1)
			},
			after: func() {
				// db0 table0
				s.assertMsg(s.dbs[db_0], "success_key", table_0, dao.MsgStatusSuccess)

				// db0 table1
				s.assertMsg(s.dbs[db_0], "success_key", table_1, dao.MsgStatusSuccess)

				// db1 table0
				s.assertMsg(s.dbs[db_1], "success_key", table_0, dao.MsgStatusSuccess)

				// db1 table1
				s.assertMsg(s.dbs[db_1], "success_key", table_1, dao.MsgStatusSuccess)
			},
		},
		{
			name: "发送失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(gomock.Any()).AnyTimes().Return(int32(1), int64(1), errors.New("mock error"))
				return producer
			},
			before: func() {
				// db0 table0
				s.insertMsg(s.dbs[db_0], "failed_key", bizA, table_0)
				// db0 table1
				s.insertMsg(s.dbs[db_0], "failed_key", bizA, table_1)
				// db1 table0
				s.insertMsg(s.dbs[db_1], "failed_key", bizB, table_0)
				// db1 table1
				s.insertMsg(s.dbs[db_1], "failed_key", bizB, table_1)
			},
			after: func() {
				// db0 table0
				s.assertMsg(s.dbs[db_0], "failed_key", table_0, dao.MsgStatusFail)
				// db0 table1
				s.assertMsg(s.dbs[db_0], "failed_key", table_1, dao.MsgStatusFail)
				// db1 table0
				s.assertMsg(s.dbs[db_1], "failed_key", table_0, dao.MsgStatusFail)
				// db1 table1
				s.assertMsg(s.dbs[db_1], "failed_key", table_1, dao.MsgStatusFail)
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(s.T())
			defer ctrl.Finish()

			tc.before()

			producer := tc.mock(ctrl)
			manager := dao.NewGormCreator(s.dbs)
			repo := repository.NewMsgRepository(s.dispatcher, manager)
			svc := service.NewProducerService(producer, repo)
			scheduler := job.NewScheduler(s.dispatcher, svc, s.lockClient)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			scheduler.Start(ctx)
			<-ctx.Done()

			tc.after()
		})
	}
}

func (s *ProducerTestSuite) insertMsg(db *gorm.DB, key, biz string, table string) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := db.WithContext(ctx).Table(table).Create(&dao.DelayMsg{
		Key: key,
		Biz: biz,
		Data: msg.DelayMessage{
			Key:      key,
			Biz:      biz,
			Content:  "第一条消息",
			BizTopic: "biz-topic",
			SendTime: now.Add(time.Second).UnixMilli(),
		}.Encode(),
		SendTime:  now.Add(time.Second).UnixMilli(),
		Status:    dao.MsgStatusInit,
		SendCount: 2,
		Ctime:     now.UnixMilli(),
		Utime:     now.UnixMilli(),
	}).Error
	require.NoError(s.T(), err)
}

func (s *ProducerTestSuite) assertMsg(db *gorm.DB, key, table string, wantStatus uint8) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	var res dao.DelayMsg
	err := db.WithContext(ctx).Table(table).Where(dao.DelayMsg{Key: key}).First(&res).Error
	cancel()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), wantStatus, res.Status)
}
