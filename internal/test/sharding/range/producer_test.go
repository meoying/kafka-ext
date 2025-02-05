package not_sharding

import (
	"context"
	"errors"
	"fmt"
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

	// 初始化数据库连接
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
	s.sharding = sharding

	timestamp := time.Now()
	baseTime := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(),
		0, 0, 0, 0, timestamp.Location())
	t0 := baseTime.AddDate(0, 0, 0).Format("20060102")
	t1 := baseTime.AddDate(0, 0, -1).Format("20060102")
	t2 := baseTime.AddDate(0, 0, -2).Format("20060102")
	t3 := baseTime.AddDate(0, 0, -3).Format("20060102")
	t4 := baseTime.AddDate(0, 0, -4).Format("20060102")
	t5 := baseTime.AddDate(0, 0, -5).Format("20060102")
	t6 := baseTime.AddDate(0, 0, -6).Format("20060102")

	// 断言有7个分片，并且和预期的分片一致
	want := []sharding2.DST{
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t0),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t1),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t2),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t3),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t4),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t5),
		},
		{
			DB:    db,
			Table: fmt.Sprintf(c.Algorithm.Range.Table, t6),
		},
	}
	require.Equal(s.T(), want, dsts)

	// 分布式锁
	lockClient := glock.NewClient(s.dbs[db])
	err = lockClient.InitTable()
	require.NoError(s.T(), err)
	s.lockClient = lockClient
}

func (s *ProducerTestSuite) TearDownTest() {
	dsts := s.sharding.GetEffectiveTables()
	for _, dst := range dsts {
		err := s.dbs[dst.DB].Exec(fmt.Sprintf("TRUNCATE TABLE %s", dst.Table)).Error
		require.NoError(s.T(), err)
	}
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
				dsts := s.sharding.GetEffectiveTables()
				for _, dst := range dsts {
					s.insertMsg(s.dbs[dst.DB], dst.Table)
				}
			},
			after: func() {
				dsts := s.sharding.GetEffectiveTables()
				for _, dst := range dsts {
					s.assertMsg(s.dbs[dst.DB], dst.Table, dao.MsgStatusSuccess)
				}
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
				dsts := s.sharding.GetEffectiveTables()
				for _, dst := range dsts {
					s.insertMsg(s.dbs[dst.DB], dst.Table)
				}
			},
			after: func() {
				dsts := s.sharding.GetEffectiveTables()
				for _, dst := range dsts {
					s.assertMsg(s.dbs[dst.DB], dst.Table, dao.MsgStatusFail)
				}
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
