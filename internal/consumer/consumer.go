// Copyright 2023 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package consumer

// 1. 将 DelayMessage 转储到数据库
// 2. 启动一个异步任务，将快要到发送时间的消息取出来，到点转发
// 2.1 每次取已经到时间点的 >= SendTime，而后转发。分批取
// 2.2 每次取出 >= SendTime - 1s，在内存里面维护一个延迟队列，到点发送。
//
//		要小心分批取，要小心延迟队列的长度
//	 周日晚上提交合并请求
type DelayMessage struct {
	Key      string
	Biz      string
	Content  string
	BizTopic string
	SendTime int64
}
