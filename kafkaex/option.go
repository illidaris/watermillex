package kafkaex

import (
	"time"
)

// Options 定义了消息队列的选项配置
type Options struct {
	Group         string        `json:"group" form:"group"`           // 消息组别
	Topic         string        `json:"topic" form:"topic"`           // 消息主题
	Key           string        `json:"key" form:"key"`               // 消息键
	TraceId       string        `json:"traceid" form:"traceid"`       // 跟踪ID
	RetryMax      int64         `json:"retrymax" form:"retrymax"`     // 最大重试次数
	RetryIndex    int64         `json:"retryindex" form:"retryindex"` // 当前重试索引
	Handle        Handler       `json:"-" form:"-"`                   // 消息处理函数
	HandleTimeout time.Duration `json:"timeout" form:"timeout"`       // 处理超时时间
}

// Fmt 检查并设置Options的默认值
func (o *Options) Fmt() *Options {
	if o.Group == "" {
		o.Group = o.Topic
	}
	return o
}

// Verify 验证Options配置的有效性
func (o *Options) Verify() error {
	if o.Topic == "" {
		return ErrNoFoundTopic
	}
	if o.Group == "" {
		return ErrNoFoundTopic
	}
	return nil
}

// NewOptions 创建并初始化一个新的Options实例
func NewOptions(opts ...Option) *Options {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Option 类型为函数，用于修改Options实例
type Option func(*Options)

// WithSharedGroup 设置Group为共享组
func WithSharedGroup() Option {
	return func(o *Options) {
		o.Group = APHMQIGP_DEF
	}
}

// WithGroup 设置自定义的消息组
func WithGroup(group string) Option {
	return func(o *Options) {
		o.Group = group
	}
}

// WithTopic 设置消息主题
func WithTopic(topic string) Option {
	return func(o *Options) {
		o.Topic = topic
	}
}

// WithKey 设置消息键
func WithKey(key string) Option {
	return func(o *Options) {
		o.Key = key
	}
}

// WithTraceID 设置跟踪ID
func WithTraceID(traceId string) Option {
	return func(o *Options) {
		o.TraceId = traceId
	}
}

// WithRetryIndex 设置当前重试索引
func WithRetryIndex(retryIndex int64) Option {
	return func(o *Options) {
		o.RetryIndex = retryIndex
	}
}

// WithRetryMax 设置最大重试次数
func WithRetryMax(retryMax int64) Option {
	return func(o *Options) {
		o.RetryMax = retryMax
	}
}

// WithHandle 设置消息处理函数
func WithHandle(handle Handler) Option {
	return func(o *Options) {
		o.Handle = handle
	}
}

// WithHandleTimeout 设置消息处理超时时间
func WithHandleTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.HandleTimeout = timeout
	}
}
