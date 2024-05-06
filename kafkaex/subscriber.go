package kafkaex

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/illidaris/core"
)

// RegisterRetry 注册一个重试消息的订阅者。如果提供的处理程序为nil，则使用默认的重试发布处理程序。
// ctx: 上下文，用于控制函数的生命周期。
// h: 自定义的消息处理程序，如果为nil，则使用默认处理程序。
// 返回值: 执行过程中遇到的任何错误。
func (m *WaterMillManager) RegisterRetry(ctx context.Context, h Handler) error {
	if h == nil {
		h = defaultRetryPublishHandle
	}
	return m.RegisterSubscriber(ctx, APHMQITP_RETRY,
		WithGroup(APHMQIGP_INNER),
		WithTopic(APHMQITP_RETRY),
		WithHandle(h))
}

// RegisterDead 注册一个死信消息的订阅者。如果提供的处理程序为nil，则使用默认的死信处理程序。
// ctx: 上下文，用于控制函数的生命周期。
// h: 自定义的消息处理程序，如果为nil，则使用默认处理程序。
// 返回值: 执行过程中遇到的任何错误。
func (m *WaterMillManager) RegisterDead(ctx context.Context, h Handler) error {
	if h == nil {
		h = defaultDeadHandle
	}
	return m.RegisterSubscriber(ctx, APHMQITP_DEAD,
		WithGroup(APHMQIGP_INNER),
		WithTopic(APHMQITP_DEAD),
		WithHandle(h))
}

// RegisterSubscriber 注册一个自定义主题的订阅者。
// ctx: 上下文，用于控制函数的生命周期。
// topic: 要订阅的主题。
// opts: 一系列选项，用于配置订阅者。
// 返回值: 执行过程中遇到的任何错误。
func (m *WaterMillManager) RegisterSubscriber(ctx context.Context, topic string, opts ...Option) error {
	opt := NewOptions(opts...)
	if err := opt.Fmt().Verify(); err != nil {
		return err
	}
	if opt.Handle == nil {
		return ErrNoFoundHandle
	}
	sub := m.Subs.GetOrSet(opt.Group, NewSubscriber)
	if sub == nil {
		return ErrNoFoundSubscriber
	}
	messageCh, err := sub.Subscribe(ctx, topic)
	if err != nil {
		return err
	}
	process, err := processHanlder(topic, opt.Group, opt.Handle)
	if err != nil {
		return err
	}
	go process(ctx, messageCh)
	return nil
}

// NewSubscriber 创建并返回一个新的Kafka订阅者实例。
// group: 订阅者所属的消费组。
// 返回值: 创建的Kafka订阅者实例和可能遇到的错误。
func NewSubscriber(group string) (*kafka.Subscriber, error) {
	res, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers: getKafkaBrokers(),
			Unmarshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
				if msg.Metadata == nil {
					return msg.UUID, nil
				}
				v := msg.Metadata.Get(APHMQH_PARTITION_KEY)
				return v, nil
			}),
			OverwriteSaramaConfig: getConfig(),
			ConsumerGroup:         group,
		},
		NewWaterMillLogger(),
	)
	if err != nil {
		deflog.ErrorCtx(context.TODO(), err.Error())
	}
	return res, err
}

// processHandler 创建并返回一个处理消息的函数。
// topic: 订阅的主题。
// executer: 执行者的标识。
// handle: 消息处理程序。
// 返回值: 一个函数，该函数可被Go协程调用以处理消息。
func processHanlder(topic, executer string, handle Handler) (func(ctx context.Context, messages <-chan *message.Message), error) {
	noretry := topic == APHMQITP_DEAD || topic == APHMQITP_RETRY
	m := GetManager()
	if m == nil {
		return nil, ErrNoFoundManager
	}
	return func(ctx context.Context, messages <-chan *message.Message) {
		for msg := range messages {
			box := NewBoxMessage()
			box.WithRawMessage(msg)
			err := invoke(ctx, box, handle) // 执行订阅
			box.ExecResult(executer, err)   // 处理结果，封入消息
			if err != nil {
				if noretry || box.Dead() {
					_ = m.Publish(APHMQITP_DEAD, box)
				} else {
					_ = m.Publish(APHMQITP_RETRY, box)
				}
			}
			msg.Ack()
		}
	}, nil
}

// invoke 调用提供的处理程序来处理消息，并处理任何可能发生的错误。
// ctx: 上下文，用于传递请求的元数据和控制超时等。
// box: 包含消息数据和其他元信息的对象。
// handle: 消息处理程序。
// 返回值: 处理过程中可能发生的错误。
func invoke(ctx context.Context, box *BoxMessage, handle Handler) (err error) {
	var cancel func()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	// trace
	if box.TraceId != "" {
		ctx = core.TraceID.SetString(ctx, box.TraceId)
	}
	// timeout
	if box.HandleTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, box.HandleTimeout)
		if cancel != nil {
			defer cancel()
		}
	}
	err = handle(ctx, box)
	return
}
