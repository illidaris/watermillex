package kafkaex

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Publish 将消息发布到指定的主题。
func (m *WaterMillManager) Publish(topic string, boxM *BoxMessage) error {
	return m.RawPublish(topic, boxM, nil)
}

// RawPublish 将消息发布到指定的主题。
func (m *WaterMillManager) RawPublish(topic string, boxM *BoxMessage, ow func(*sarama.Config) *sarama.Config) error {
	// 尝试从缓存中获取或创建一个新的发布者
	pub := m.Pubs.GetOrSet(boxM.Group, func(key string) (*kafka.Publisher, error) {
		return NewPublisher(topic, ow)
	})
	if pub == nil {
		return ErrNoFoundPublisher // 如果无法获取发布者，则返回错误
	}
	return pub.Publish(topic, boxM.NewRawMessage()) // 调用发布者发布消息
}

// NewPublisher 创建并返回一个新的Kafka发布者实例。
// 参数:
// - topic: 发布者将要发布消息的主题名称。
// 返回值:
// - *kafka.Publisher: 创建的Kafka发布者实例。
// - error: 如果在创建发布者过程中遇到错误，则返回错误信息；否则返回nil。
func NewPublisher(topic string, ow func(*sarama.Config) *sarama.Config) (*kafka.Publisher, error) {
	cfg := getConfig()
	if ow != nil {
		cfg = ow(cfg)
	}
	res, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               getKafkaBrokers(), // 指定Kafka代理服务器列表
			OverwriteSaramaConfig: cfg,               // 应用额外的Sarama配置
			Marshaler: kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
				if msg.Metadata == nil {
					return msg.UUID, nil
				}
				v := msg.Metadata.Get(APHMQH_PARTITION_KEY)
				return v, nil // 返回消息的分区键，如果不存在则不设置分区
			}),
		},
		NewWaterMillLogger(), // 应用WaterMill日志配置
	)
	if err != nil {
		deflog.ErrorCtx(context.TODO(), err.Error()) // 记录创建发布者失败的错误日志
	}
	return res, err
}
