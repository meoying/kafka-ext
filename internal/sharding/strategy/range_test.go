package strategy

import (
	"errors"
	"github.com/meoying/kafka-ext/internal/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"strings"
	"testing"
)

func TestRange_Sharding(t *testing.T) {
	testCases := []struct {
		name     string
		interval int
		keys     []string
		wantDST  sharding.DST
		wantErr  error
	}{
		{
			name:    "keys 长度不对",
			keys:    []string{},
			wantErr: errors.New("range: 分片 key 错误"),
		},
		{
			name:    "keys 类型不对",
			keys:    []string{"a"},
			wantErr: errors.New("rang: 分片 key 类型转换错误"),
		},
		{
			name:     "interval 小于 0",
			keys:     []string{"1"},
			interval: -1,
			wantErr:  errors.New("range: 分片间隔必须大于 0"),
		},
		{
			name:     "OK，interval 为 1",
			keys:     []string{"1"},
			interval: 1,
			wantDST: sharding.DST{
				DB:    "test_db",
				Table: "test_tab_1",
			},
		},
		{
			name:     "OK，interval 为 10",
			keys:     []string{"1"},
			interval: 10,
			wantDST: sharding.DST{
				DB:    "test_db",
				Table: "test_tab_0",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewRange("test_db", "test_tab_%d", tc.interval)
			dst, err := s.Sharding(tc.keys)
			if err != nil {
				assert.True(t, strings.Contains(err.Error(), tc.wantErr.Error()))
				return
			}
			assert.Equal(t, tc.wantDST, dst)
		})
	}
}

func TestRange_EffectiveTables(t *testing.T) {
	wantDSTs := []sharding.DST{
		{
			DB:    "test_db",
			Table: "test_tab_0",
		},
		{
			DB:    "test_db",
			Table: "test_tab_1",
		},
		{
			DB:    "test_db",
			Table: "test_tab_2",
		},
		{
			DB:    "test_db",
			Table: "test_tab_3",
		},
	}
	r := NewRange("test_db", "test_tab_%d", 1)
	var eg errgroup.Group
	eg.Go(func() error {
		_, err := r.Sharding([]string{"0"})
		return err
	})
	eg.Go(func() error {
		_, err := r.Sharding([]string{"1"})
		return err
	})
	eg.Go(func() error {
		_, err := r.Sharding([]string{"2"})
		return err
	})
	eg.Go(func() error {
		_, err := r.Sharding([]string{"3"})
		return err
	})
	err := eg.Wait()
	require.NoError(t, err)
	assert.Equal(t, 3, r.maxSeq.Load())

	dsts := r.EffectiveTables()
	assert.Equal(t, wantDSTs, dsts)
}
