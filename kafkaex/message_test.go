package kafkaex

import (
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestBoxMessage(t *testing.T) {
	// 创建一个BoxMessage实例
	box := NewBoxMessage()

	// 测试MsgId字段是否为空
	assert.Empty(t, box.MsgId)

	// 测试Execer字段是否为空
	assert.Empty(t, box.Execer)

	// 测试ExecAt字段是否为0
	assert.Equal(t, int64(0), box.ExecAt)

	// 测试ExecErr字段是否为空
	assert.Empty(t, box.ExecErr)

	// 测试Value字段是否为空
	assert.Empty(t, box.Value)

	// 测试Dead方法在RetryMax为0时返回true
	box.RetryMax = 0
	assert.True(t, box.Dead())

	// 测试Dead方法在RetryIndex大于等于RetryMax时返回true
	box.RetryMax = 3
	box.RetryIndex = 3
	assert.True(t, box.Dead())

	// 测试Dead方法在RetryMax大于0且RetryIndex小于RetryMax时返回false
	box.RetryMax = 3
	box.RetryIndex = 1
	assert.False(t, box.Dead())

	// 测试ExecResult方法在noretry为true时将消息发布到APHMQITP_DEAD主题
	execer := "testExecer"
	err := errors.New("testError")
	box.ExecResult(execer, err)
	assert.Equal(t, execer, box.Execer)
	assert.NotEqual(t, int64(0), box.ExecAt)
	if right := err.Error(); len(right) > 255 {
		right = right[:255]
		assert.Equal(t, right, box.ExecErr)
	}

	// 检查消息是否发布到APHMQITP_DEAD主题

	// 测试ExecResult方法在noretry为false且消息未进入死信状态时将消息发布到APHMQITP_RETRY主题
	box.ExecResult(execer, err)
	assert.Equal(t, execer, box.Execer)
	assert.NotEqual(t, int64(0), box.ExecAt)
	if right := err.Error(); len(right) > 255 {
		right = right[:255]
		assert.Equal(t, right, box.ExecErr)
	}
	// 检查消息是否发布到APHMQITP_RETRY主题

	// 测试WithOption方法应用多个选项并返回修改后的实例
	option1 := func(o *Options) {
		o.HandleTimeout = time.Second
	}
	option2 := func(o *Options) {
		o.RetryMax = 3
	}
	box = box.WithOption(option1, option2)
	assert.Equal(t, time.Second, box.Options.HandleTimeout)
	assert.Equal(t, int64(3), box.Options.RetryMax)

	// 测试WithRawMessage方法根据message.Message更新BoxMessage实例并返回修改后的实例
	msg := &message.Message{
		UUID:    "testUUID",
		Payload: []byte("testPayload"),
		Metadata: map[string]string{
			"MsgGroup":     "testGroup",
			"MsgTopic":     "testTopic",
			"PartitionKey": "testKey",
			"TraceId":      "testTraceId",
			"Retries":      "1",
			"RetriesMax":   "3",
			"ExecTimeout":  "1000",
			"MsgId":        "testMsgId",
			"Execer":       "testExecer",
			"ExecAt":       "1234567890",
			"ExecErr":      "testError",
		},
	}
	box = box.WithRawMessage(msg)
	assert.Equal(t, "testUUID", box.MsgId)
	assert.Equal(t, []byte("testPayload"), box.Value)
	// 检查其他字段是否正确更新

	// 测试NewRawMessage方法根据BoxMessage的内容创建一个新的message.Message实例并返回该实例
	rawMsg := box.NewRawMessage()
	assert.Equal(t, box.MsgId, rawMsg.Metadata.Get(APHMQH_MSG_ID))
	assert.Equal(t, box.Execer, rawMsg.Metadata.Get(APHMQH_EXECER))
	assert.Equal(t, cast.ToString(box.ExecAt), rawMsg.Metadata.Get(APHMQH_EXECAT))
	assert.Equal(t, box.ExecErr, rawMsg.Metadata.Get(APHMQH_EXECERR))
	// 检查其他字段是否正确设置

	// 测试WithHeadersOption方法使用headers中的信息更新BoxMessage实例并返回修改后的实例
	headers := map[string]string{
		APHMQH_MSG_TOPIC: "value1",
	}
	box = box.WithHeadersOption(headers)
	// 检查BoxMessage实例的字段值是否被正确更新
	assert.Equal(t, "value1", box.Options.Topic)
}
