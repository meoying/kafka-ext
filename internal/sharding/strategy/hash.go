package strategy

import (
	"fmt"
	"github.com/meoying/kafka-ext/internal/sharding"
	"hash/fnv"
)

// Hash 哈希分库分表算法
type Hash struct {
	dbPattern    Pattern
	tablePattern Pattern
	// 哈希函数。方便写测试
	Hash func(key string, base int) int
}

func NewHashSharding(dbPattern Pattern, tablePattern Pattern) Hash {
	return Hash{dbPattern: dbPattern, tablePattern: tablePattern, Hash: hash}
}

func (h Hash) Name() string {
	return "hash"
}

func (h Hash) Sharding(keys []string) sharding.DST {
	dbKey := keys[0]
	tableKey := keys[1]

	var dbName string
	if !h.dbPattern.Sharding {
		dbName = h.dbPattern.Name
	} else {
		num := h.Hash(dbKey, h.dbPattern.Base)
		dbName = fmt.Sprintf(h.dbPattern.Name, num)
	}

	var tableName string
	if !h.tablePattern.Sharding {
		tableName = h.tablePattern.Name
	} else {
		num := h.Hash(tableKey, h.tablePattern.Base)
		tableName = fmt.Sprintf(h.tablePattern.Name, num)
	}

	return sharding.DST{
		DB:    dbName,
		Table: tableName,
	}
}

func (h Hash) EffectiveTables() []sharding.DST {
	dbs := make([]string, 0, 3)
	tables := make([]string, 0, 3)

	if h.dbPattern.Sharding {
		for i := 0; i < h.dbPattern.Base; i++ {
			dbs = append(dbs, fmt.Sprintf(h.dbPattern.Name, i))
		}
	} else {
		dbs = append(dbs, h.dbPattern.Name)
	}

	if h.tablePattern.Sharding {
		for i := 0; i < h.tablePattern.Base; i++ {
			tables = append(tables, fmt.Sprintf(h.tablePattern.Name, i))
		}
	} else {
		tables = append(tables, h.tablePattern.Name)
	}

	// 求一个笛卡尔积
	dsts := make([]sharding.DST, 0, len(dbs)*len(tables))
	for _, db := range dbs {
		for _, table := range tables {
			dsts = append(dsts, sharding.DST{
				DB:    db,
				Table: table,
			})
		}
	}
	return dsts
}

func hash(key string, base int) int {
	// 使用FNV-1a哈希算法计算哈希值
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	hash := int(hasher.Sum32())
	// 取模运算得到编号，确保结果在 0 到 base-1 之间
	return hash % base
}

type Pattern struct {
	Base     int
	Name     string
	Sharding bool
}
