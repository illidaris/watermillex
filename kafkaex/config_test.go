package kafkaex

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestGetRetryDelay(t *testing.T) {
	// Set retry delay to 5 seconds
	SetRetryDelay(5 * time.Second)

	// Get retry delay
	retryDelay := getRetryDelay()

	// Assert that the retry delay is 5 seconds
	assert.Equal(t, 5*time.Second, retryDelay)
}

func TestGetExecTimeout(t *testing.T) {
	// Set exec timeout to 10 seconds
	SetTimeout(10 * time.Second)

	// Get exec timeout
	execTimeout := getExecTimeout()

	// Assert that the exec timeout is 10 seconds
	assert.Equal(t, 10*time.Second, execTimeout)
}

func TestConfig(t *testing.T) {
	// Set Kafka user, password and brokers
	SetGetKafkaUserFunc(func() string {
		return "kafkaUser"
	})
	SetGetKafkaPwdFunc(func() string {
		return "kafkaPwd"
	})
	SetGetKafkaBrokersFunc(func() []string {
		return []string{"kafkaBroker1", "kafkaBroker2"}
	})

	// Get Sarama config
	config := getConfig()

	// Assert that the SASL authentication is enabled
	assert.True(t, config.Net.SASL.Enable)

	// Assert that the SASL mechanism is set to plaintext
	assert.Equal(t, string(sarama.SASLTypePlaintext), string(config.Net.SASL.Mechanism))

	// Assert that the SASL username is "kafkaUser"
	assert.Equal(t, "kafkaUser", config.Net.SASL.User)

	// Assert that the SASL password is "kafkaPwd"
	assert.Equal(t, "kafkaPwd", config.Net.SASL.Password)

	// Assert that the consumer group rebalance strategy is set to Sticky
	assert.Equal(t, []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}, config.Consumer.Group.Rebalance.GroupStrategies)

	// Assert that the consumer initial offset is set to oldest
	assert.Equal(t, sarama.OffsetOldest, config.Consumer.Offsets.Initial)

	// Assert that the producer required acks is set to all
	assert.Equal(t, sarama.WaitForAll, config.Producer.RequiredAcks)

	// Assert that the producer timeout is set to 5 seconds
	assert.Equal(t, 5*time.Second, config.Producer.Timeout)

	// Assert that the producer retry max is set to 3
	assert.Equal(t, 3, config.Producer.Retry.Max)

	// Assert that the producer returns successes is true
	assert.True(t, config.Producer.Return.Successes)
}
