package kafkaex

import (
	"context"

	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/illidaris/aphrodite/pkg/structure"
)

// GetManager 是用于获取默认消息管理器的函数。
// 该函数确保仅初始化一次默认的消息管理器和日志记录器。
// 返回值是初始化后的默认消息管理器实例，实现了IManager接口。
func GetManager() IManager {
	once.Do(func() {
		defManager = NewWaterMillManager()
	})
	return defManager
}

// IManager 接口定义了消息管理器的基本功能，
// 包括发布消息、注册订阅者、注册重试处理和注册死信处理。
type IManager interface {
	Publish(topic string, boxmsg *BoxMessage) error
	RegisterSubscriber(ctx context.Context, topic string, opts ...Option) error
	RegisterRetry(ctx context.Context, h Handler) error
	RegisterDead(ctx context.Context, h Handler) error
}

// NewWaterMillManager 是用于创建一个新的WaterMillManager实例的函数。
// 返回值是一个初始化的WaterMillManager实例，包含了空的订阅者和发布者映射。
func NewWaterMillManager() IManager {
	m := &WaterMillManager{
		Subs: structure.NewItemMap[kafka.Subscriber](),
		Pubs: structure.NewItemMap[kafka.Publisher](),
	}
	return m
}

// WaterMillManager 是具体的消息管理器实现，负责管理订阅者和发布者。
type WaterMillManager struct {
	Subs structure.ItemMap[kafka.Subscriber] // 存储订阅者
	Pubs structure.ItemMap[kafka.Publisher]  // 存储发布者
}
