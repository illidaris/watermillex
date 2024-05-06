package kafkaex

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/spf13/cast"
)

// Handler 是一个处理BoxMessage的函数类型，它接收一个context.Context和一个*BoxMessage作为参数，返回一个error。
type Handler func(ctx context.Context, box *BoxMessage) error

// NewBoxMessage 创建并返回一个新的BoxMessage实例，其中Options的HandleTimeout字段被初始化为getExecTimeout()的返回值。
func NewBoxMessage() *BoxMessage {
	return &BoxMessage{
		Options: Options{
			HandleTimeout: getExecTimeout(),
		},
	}
}

// BoxMessage 定义了一个消息结构体，包括基础的消息信息和执行相关的信息。
type BoxMessage struct {
	Options
	MsgId   string `json:"msgid" form:"msgid"`     // 消息ID
	Execer  string `json:"execer" form:"execer"`   // 执行者
	ExecAt  int64  `json:"execat" form:"execat"`   // 执行时间戳
	ExecErr string `json:"execerr" form:"execerr"` // 执行错误信息
	Value   []byte `json:"val" form:"val"`         // 消息值
}

// Dead 判断消息是否进入死信状态。当RetryMax为0或RetryIndex大于等于RetryMax时，返回true。
func (m *BoxMessage) Dead() bool {
	return m.RetryMax == 0 || m.RetryIndex >= m.RetryMax
}

// ExecResult 更新消息的执行结果，并根据是否重试或消息是否进入死信状态，发布消息到相应的主题。
func (m *BoxMessage) ExecResult(execer string, err error) {
	m.Execer = execer
	m.ExecAt = time.Now().Unix()
	if err != nil {
		msgErr := err.Error()
		if len(msgErr) > 255 {
			msgErr = msgErr[:255]
		}
		m.ExecErr = msgErr
	}
}

// WithOption 为BoxMessage应用一个或多个选项，并返回修改后的BoxMessage实例。
func (m *BoxMessage) WithOption(opts ...Option) *BoxMessage {
	for _, opt := range opts {
		opt(&m.Options)
	}
	return m
}

// WithRawMessage 根据message.Message更新BoxMessage实例，并返回修改后的实例。它会设置MsgId和Value，并应用消息的元数据作为选项。
func (m *BoxMessage) WithRawMessage(msg *message.Message) *BoxMessage {
	m.MsgId = msg.UUID
	m.Value = msg.Payload
	return m.WithHeadersOption(msg.Metadata)
}

// NewRawMessage 根据BoxMessage的内容创建一个新的message.Message实例，并返回该实例。它会使用BoxMessage中的字段设置消息的元数据。
func (m *BoxMessage) NewRawMessage() *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), m.Value)
	msg.Metadata.Set(APHMQH_MSG_GROUP, m.Group)
	msg.Metadata.Set(APHMQH_MSG_TOPIC, m.Topic)
	msg.Metadata.Set(APHMQH_PARTITION_KEY, m.Key)
	msg.Metadata.Set(APHMQH_TRACE_ID, m.TraceId)
	msg.Metadata.Set(APHMQH_RETRIES, cast.ToString(m.RetryIndex))
	msg.Metadata.Set(APHMQH_RETRIES_MAX, cast.ToString(m.RetryMax))
	msg.Metadata.Set(APHMQH_EXEC_TIMEOUT, cast.ToString(m.HandleTimeout.Milliseconds()))
	msg.Metadata.Set(APHMQH_MSG_ID, m.MsgId)
	msg.Metadata.Set(APHMQH_EXECER, m.Execer)
	msg.Metadata.Set(APHMQH_EXECAT, cast.ToString(m.ExecAt))
	msg.Metadata.Set(APHMQH_EXECERR, m.ExecErr)
	return msg
}

// WithHeadersOption 使用headers中的信息更新BoxMessage实例，并返回修改后的实例。它从headers中读取配置项并应用到BoxMessage上。
func (m *BoxMessage) WithHeadersOption(headers map[string]string) *BoxMessage {
	if v := headers[APHMQH_MSG_GROUP]; v != "" {
		m.Group = v
	}
	if v := headers[APHMQH_MSG_TOPIC]; v != "" {
		m.Topic = v
	}
	if v := headers[APHMQH_PARTITION_KEY]; v != "" {
		m.Key = v
	}
	if v := headers[APHMQH_TRACE_ID]; v != "" {
		m.TraceId = v
	}
	if v := headers[APHMQH_RETRIES]; v != "" {
		m.RetryIndex = cast.ToInt64(v)
	}
	if v := headers[APHMQH_RETRIES_MAX]; v != "" {
		m.RetryMax = cast.ToInt64(v)
	}
	if v := headers[APHMQH_EXEC_TIMEOUT]; v != "" {
		timeout := time.Duration(cast.ToInt64(v)) * time.Millisecond
		m.HandleTimeout = timeout
	}
	if v := headers[APHMQH_EXECER]; v != "" {
		m.Execer = v
	}
	if v := headers[APHMQH_EXECAT]; v != "" {
		m.ExecAt = cast.ToInt64(v)
	}
	if v := headers[APHMQH_EXECERR]; v != "" {
		m.ExecErr = v
	}
	return m
}
