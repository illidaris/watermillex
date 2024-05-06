package kafkaex

import "errors"

// 定义了一系列的错误类型，用于表示消息发布与订阅过程中可能出现的配置错误。
// 这些错误类型主要用于标识配置信息缺失的情况，包括：
// - 没有配置发布者
// - 没有配置订阅者
// - 没有配置主题
// - 没有配置组名
// - 没有配置执行函数
var (
	ErrNoFoundManager    = errors.New("没有配置管理器")  // 表示没有找到配置的理器
	ErrNoFoundPublisher  = errors.New("没有配置发布者")  // 表示没有找到配置的发布者
	ErrNoFoundSubscriber = errors.New("没有配置订阅者")  // 表示没有找到配置的订阅者
	ErrNoFoundTopic      = errors.New("没有配置主题")   // 表示没有找到配置的主题
	ErrNoFoundGroup      = errors.New("没有配置组名")   // 表示没有找到配置的组名
	ErrNoFoundHandle     = errors.New("没有配置执行函数") // 表示没有找到配置的执行函数
)
