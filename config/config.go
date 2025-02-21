package config

type Config struct {
	DataSource []DB     `json:"dataSource" yaml:"dataSource"`
	Sharding   Sharding `json:"algorithm" yaml:"algorithm"`
	Lock       Lock     `json:"lock" yaml:"lock"`
	Kafka      Kafka    `json:"kafka" yaml:"kafka"`
}

type DB struct {
	Name string `json:"name" yaml:"name"`
	DSN  string `json:"dsn" yaml:"dsn"`
}

type Sharding struct {
	BizStrategy []BizStrategy  `json:"bizStrategy" yaml:"bizStrategy"`
	Strategy    map[string]any `json:"strategy" yaml:"strategy"`
}

type BizStrategy struct {
	Strategy string   `json:"strategy" yaml:"strategy"`
	Biz      []string `json:"biz" yaml:"biz"`
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
