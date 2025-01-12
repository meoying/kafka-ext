package msg

import (
	"encoding/json"
)

type Message struct {
	ID        int64 `json:"id,omitempty"`
	SendCount int   `json:"send_count,omitempty"`
	DelayMessage
}

type DelayMessage struct {
	Key       string `json:"key"`
	Biz       string `json:"biz"`
	Content   string `json:"content"`
	BizTopic  string `json:"biz_topic"`
	Partition int32  `json:"partition"`
	SendTime  int64  `json:"send_time"`
}

func (d DelayMessage) Encode() []byte {
	data, _ := json.Marshal(d)
	return data
}
