package dao

// DelayMsg 代表一条延迟消息
type DelayMsg struct {
	ID   int64  `gorm:"column:id;primaryKey;autoIncrement"`
	Key  string `gorm:"column:key;unique"`
	Biz  string `gorm:"column:biz"`
	Data []byte `gorm:"column:data;type:TEXT"`
	// 发送时间
	SendTime  int64 `gorm:"column:send_time;index:idx_send_time_status"`
	Status    uint8 `gorm:"column:status;default:0;index:idx_send_time_status"`
	SendCount int   `gorm:"column:send_count"`
	Ctime     int64 `gorm:"column:ctime"`
	Utime     int64 `gorm:"column:utime"`
}

func (DelayMsg) TableName() string {
	return "delay_msgs"
}

const (
	MsgStatusInit    uint8 = 0 // 未发送
	MsgStatusSuccess uint8 = 1 // 发送成功
	MsgStatusFail    uint8 = 2 // 发送失败
)
