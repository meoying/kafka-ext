package config

type Config struct {
	DataSource []DB      `json:"dataSource" yaml:"dataSource"`
	Algorithm  Algorithm `json:"algorithm" yaml:"algorithm"`
	Lock       Lock      `json:"lock" yaml:"lock"`
	Kafka      Kafka     `json:"kafka" yaml:"kafka"`
}

type DB struct {
	Name string `json:"name" yaml:"name"`
	DSN  string `json:"dsn" yaml:"dsn"`
}

type Algorithm struct {
	Type  string `json:"type" yaml:"type"`
	Hash  Hash   `json:"hash" yaml:"hash"`
	Range Range  `json:"range" yaml:"range"`
}

type Hash struct {
	DBPattern    Pattern `json:"dbPattern" yaml:"dbPattern"`
	TablePattern Pattern `json:"tablePattern" yaml:"tablePattern"`
}

type Pattern struct {
	Base     int    `json:"base" yaml:"base"`
	Name     string `json:"name" yaml:"name"`
	Sharding bool   `json:"sharding" yaml:"sharding"`
}

type Range struct {
	DB        string `json:"db" yaml:"db"`
	Table     string `json:"table" yaml:"table"`
	Interval  int    `json:"interval"`
	Retention int    `json:"retention"`
}

type Lock struct {
	Type string `json:"type" yaml:"type"`
	GORM struct {
		DSN string `json:"dsn" yaml:"dsn"`
	} `json:"gorm" yaml:"gorm"`
}

type Kafka struct {
	Addr    string `json:"addr" yaml:"addr"`
	GroupID string `json:"groupID" yaml:"groupID"`
	Topic   string `json:"topic" yaml:"topic"`
}
