package kafkaex

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
)

// 定义全局变量，用于配置和管理Kafka连接信息及重试、超时策略。
var (
	once            sync.Once                              // 确保全局变量只被初始化一次。
	defManager      IManager                               // 默认的管理接口。
	deflog          ILogger         = NewWaterMillLogger() // 默认的日志记录器。
	getKafkaUser    func() string                          // 获取Kafka的用户名的函数。
	getKafkaPwd     func() string                          // 获取Kafka的密码的函数。
	getKafkaBrokers func() []string                        // 获取Kafka的Broker列表的函数。
	retryDelay      time.Duration                          // 重试延迟时间。
	execTimeout     time.Duration                          // 执行超时时间。
)

// SetGetKafkaUserFunc 设置获取Kafka用户名的函数。
func SetGetKafkaUserFunc(f func() string) {
	getKafkaUser = f
}

// SetGetKafkaPwdFunc 设置获取Kafka密码的函数。
func SetGetKafkaPwdFunc(f func() string) {
	getKafkaPwd = f
}

// SetGetKafkaBrokersFunc 设置获取Kafka Broker列表的函数。
func SetGetKafkaBrokersFunc(f func() []string) {
	getKafkaBrokers = f
}

// SetRetryDelay 设置重试延迟时间。
func SetRetryDelay(delay time.Duration) {
	retryDelay = delay
}

// SetTimeout 设置执行超时时间。
func SetTimeout(timeout time.Duration) {
	execTimeout = timeout
}

// getRetryDelay 返回配置的重试延迟时间，若未配置则返回默认值3秒。
func getRetryDelay() time.Duration {
	if retryDelay == 0 {
		return time.Second * 3
	}
	return retryDelay
}

// getExecTimeout 返回配置的执行超时时间，若未配置则返回默认值25秒。
func getExecTimeout() time.Duration {
	if execTimeout == 0 {
		return time.Second * 25
	}
	return execTimeout
}

// getConfig 配置Sarama客户端参数
func getConfig() *sarama.Config {
	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()      // 初始化Sarama配置
	saramaSubscriberConfig.Net.SASL.Enable = true                        // 启用SASL认证
	saramaSubscriberConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext // 设置SASL认证机制为明文传输
	saramaSubscriberConfig.Net.SASL.User = getKafkaUser()                // 设置SASL用户名
	saramaSubscriberConfig.Net.SASL.Password = getKafkaPwd()             // 设置SASL密码
	saramaSubscriberConfig.
		Consumer.
		Group.
		Rebalance.
		GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(), // 设置消费组的重平衡策略为Sticky，使得分配更加均匀
	}
	saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest   // 设置消费者初始偏移量为最旧的消息
	saramaSubscriberConfig.Producer.RequiredAcks = sarama.WaitForAll        // 设置生产者发送消息时的确认策略为所有副本都确认，0-无需应答 1-本地确认 -1-全部确认
	saramaSubscriberConfig.Producer.Timeout = time.Second * 5               // 等待 WaitForAck的时间
	saramaSubscriberConfig.Producer.Partitioner = sarama.NewHashPartitioner // 分区策略
	saramaSubscriberConfig.Producer.Retry.Max = 3                           // 重新发送的次数
	saramaSubscriberConfig.Producer.Return.Successes = true
	return saramaSubscriberConfig
}

// defaultRetryPublishHanle 消息重入真实消息队列 默认重试
func defaultRetryPublishHandle(ctx context.Context, box *BoxMessage) error {
	time.Sleep(getRetryDelay())
	box.RetryIndex++ // 计数累加
	deflog.InfoCtx(ctx, "消息%s重入%s,%s", box.MsgId, box.Topic, string(box.Value))
	m := GetManager()
	if m == nil {
		return ErrNoFoundManager
	}
	return m.Publish(box.Topic, box)
}

// defaultDeadHandle 消息私信队列 默认死信
func defaultDeadHandle(ctx context.Context, box *BoxMessage) error {
	bs, err := json.Marshal(box)
	if err != nil {
		deflog.ErrorCtx(ctx, "死信队列无法消费，解析失败%v", err)
		return err
	}
	deflog.InfoCtx(ctx, "死信队列>>>输出：", string(bs))
	return nil
}
