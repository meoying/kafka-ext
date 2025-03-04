package strategy

import (
	"errors"
	"fmt"
	"github.com/meoying/kafka-ext/internal/sharding"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHash_Sharding(t *testing.T) {
	testCases := []struct {
		name    string
		db      HashPattern
		table   HashPattern
		keys    []string
		wantDST sharding.DST
		wantErr error
	}{
		{
			name: "key 长度不够",
			keys: []string{"test_Biz"},
			db: HashPattern{
				Base:     3,
				Name:     "test_db_%d",
				Sharding: true,
			},
			table: HashPattern{
				Base:     3,
				Name:     "test_table_%d",
				Sharding: true,
			},
			wantErr: errors.New("hash: 分片 key 错误"),
		},
		{
			name: "不分库分表",
			keys: []string{"test_Biz", "id_123"},
			db: HashPattern{
				Base:     3,
				Name:     "test_db",
				Sharding: false,
			},
			table: HashPattern{
				Base:     3,
				Name:     "test_table",
				Sharding: false,
			},
			wantDST: sharding.DST{
				DB:    "test_db",
				Table: "test_table",
			},
		},
		{
			name: "分库分表",
			keys: []string{"test_Biz", "id_123"},
			db: HashPattern{
				Base:     3,
				Name:     "test_db_%d",
				Sharding: true,
			},
			table: HashPattern{
				Base:     3,
				Name:     "test_table_%d",
				Sharding: true,
			},
			wantDST: sharding.DST{
				DB:    fmt.Sprintf("test_db_%d", hash("test_Biz", 3)),
				Table: fmt.Sprintf("test_table_%d", hash("id_123", 3)),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewHash(tc.db, tc.table)
			dst, err := s.Sharding(tc.keys)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantDST, dst)
		})
	}
}

func TestHash_EffectiveTables(t *testing.T) {
	testCases := []struct {
		name     string
		db       HashPattern
		table    HashPattern
		wantDSTs []sharding.DST
	}{
		{
			name: "分库分表",
			db: HashPattern{
				Base:     3,
				Name:     "test_db_%d",
				Sharding: true,
			},
			table: HashPattern{
				Base:     3,
				Name:     "test_table_%d",
				Sharding: true,
			},
			wantDSTs: []sharding.DST{
				{
					DB:    "test_db_0",
					Table: "test_table_0",
				},
				{
					DB:    "test_db_0",
					Table: "test_table_1",
				},
				{
					DB:    "test_db_0",
					Table: "test_table_2",
				},
				{
					DB:    "test_db_1",
					Table: "test_table_0",
				},
				{
					DB:    "test_db_1",
					Table: "test_table_1",
				},
				{
					DB:    "test_db_1",
					Table: "test_table_2",
				},
				{
					DB:    "test_db_2",
					Table: "test_table_0",
				},
				{
					DB:    "test_db_2",
					Table: "test_table_1",
				},
				{
					DB:    "test_db_2",
					Table: "test_table_2",
				},
			},
		},
		{
			name: "不分库分表",
			db: HashPattern{
				Base:     3,
				Name:     "test_db",
				Sharding: false,
			},
			table: HashPattern{
				Base:     3,
				Name:     "test_table",
				Sharding: false,
			},
			wantDSTs: []sharding.DST{
				{
					DB:    "test_db",
					Table: "test_table",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewHash(tc.db, tc.table)
			dsts := s.EffectiveTables()
			assert.Equal(t, tc.wantDSTs, dsts)
		})
	}
}
