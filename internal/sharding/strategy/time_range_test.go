package strategy

import (
	"errors"
	"github.com/meoying/kafka-ext/internal/sharding"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTimeRange_Sharding(t *testing.T) {
	r := NewRange("test_db", "test_tab_%d", 7)
	testCases := []struct {
		name     string
		strategy TimeRange
		wantDST  sharding.DST
		wantErr  error
	}{
		{
			name: "开始时间不对",
			strategy: TimeRange{
				Range: r,
				// 开始时间设置为一千年后
				startTime: time.Date(3021, 1, 1, 0, 0, 0, 0, time.Local),
				Now: func() time.Time {
					return time.Now()
				},
			},
			wantErr: errors.New("time_range: 时间错误，当前时间小于开始时间"),
		},
		{
			name: "正常情况",
			strategy: TimeRange{
				Range:     r,
				startTime: time.Now(),
				Now: func() time.Time {
					return time.Now()
				},
			},
			wantDST: sharding.DST{
				DB:    "test_db",
				Table: "test_tab_0",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dst, err := tc.strategy.Sharding([]string{})
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantDST, dst)
		})
	}
}
