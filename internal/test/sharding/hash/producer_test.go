package not_sharding

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
	"github.com/meoying/kafka-ext/internal/sharding/strategy"
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
	sharding   *sharding2.Sharding
}

func TestProducer(t *testing.T) {
	suite.Run(t, new(ProducerTestSuite))
}

func (s *ProducerTestSuite) SetupSuite() {
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

	hashSharding := strategy.NewHashSharding(dbPattern, tablePattern)
	sharding := sharding2.NewSharding(hashSharding)

	dsts := sharding.GetEffectiveTables()
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

	s.sharding = sharding
	s.dbs = make(map[string]*gorm.DB)

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
					Key:       sarama.StringEncoder("key1"),
					Value:     sarama.StringEncoder("第一条消息"),
				}).AnyTimes().Return(int32(1), int64(1), nil)
				return producer
			},
			before: func() {
				// 两个db，共4张表，每张表上都有一条待发的消息
				s.insertMsg(s.dbs[db_0], table_0)
				s.insertMsg(s.dbs[db_0], table_1)
				s.insertMsg(s.dbs[db_1], table_0)
				s.insertMsg(s.dbs[db_1], table_1)
			},
			after: func() {
				// 4张表上都有一个定时任务
				s.assertMsg(s.dbs[db_0], table_0, dao.MsgStatusSuccess)
				s.assertMsg(s.dbs[db_0], table_1, dao.MsgStatusSuccess)
				s.assertMsg(s.dbs[db_1], table_0, dao.MsgStatusSuccess)
				s.assertMsg(s.dbs[db_1], table_1, dao.MsgStatusSuccess)
			},
		},
		{
			name: "发送失败",
			mock: func(ctrl *gomock.Controller) sarama.SyncProducer {
				producer := mocks.NewMockSyncProducer(ctrl)
				producer.EXPECT().SendMessage(&sarama.ProducerMessage{
					Topic:     "biz-topic",
					Partition: 0,
					Key:       sarama.StringEncoder("key1"),
					Value:     sarama.StringEncoder("第一条消息"),
				}).AnyTimes().Return(int32(1), int64(1), errors.New("mock error"))
				return producer
			},
			before: func() {
				// 两个db，共4张表，每张表上都有一条待发的消息
				s.insertMsg(s.dbs[db_0], table_0)
				s.insertMsg(s.dbs[db_0], table_1)
				s.insertMsg(s.dbs[db_1], table_0)
				s.insertMsg(s.dbs[db_1], table_1)
			},
			after: func() {
				// 4张表上都有一个定时任务
				s.assertMsg(s.dbs[db_0], table_0, dao.MsgStatusFail)
				s.assertMsg(s.dbs[db_0], table_1, dao.MsgStatusFail)
				s.assertMsg(s.dbs[db_1], table_0, dao.MsgStatusFail)
				s.assertMsg(s.dbs[db_1], table_1, dao.MsgStatusFail)
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(s.T())
			defer ctrl.Finish()

			tc.before()

			producer := tc.mock(ctrl)
			dbDAO := dao.NewMsgDAO(s.dbs)
			repo := repository.NewMsgRepository(dbDAO)
			svc := service.NewProducerService(producer, repo)

			scheduler := job.NewScheduler(s.sharding, svc, s.lockClient)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			scheduler.Start(ctx)
			<-ctx.Done()

			tc.after()
		})
	}
}

func (s *ProducerTestSuite) insertMsg(db *gorm.DB, table string) {
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	err := db.WithContext(ctx).Table(table).Create(&dao.DelayMsg{
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
		SendCount: 2,
		Ctime:     now.UnixMilli(),
		Utime:     now.UnixMilli(),
	}).Error
	require.NoError(s.T(), err)
}

func (s *ProducerTestSuite) assertMsg(db *gorm.DB, table string, wantStatus uint8) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	var res dao.DelayMsg
	err := db.WithContext(ctx).Table(table).Where(dao.DelayMsg{Key: "key1"}).First(&res).Error
	cancel()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), wantStatus, res.Status)
}
