package strategy

// NotSharding 不分库分表
type NotSharding struct {
	Hash
}

func NewNotSharding(db string, table string) NotSharding {
	dbPattern := HashPattern{
		Base:     1,
		Name:     db,
		Sharding: false,
	}
	tablePattern := HashPattern{
		Base:     1,
		Name:     table,
		Sharding: false,
	}
	return NotSharding{
		Hash: Hash{
			dbPattern:    dbPattern,
			tablePattern: tablePattern,
		},
	}
}
