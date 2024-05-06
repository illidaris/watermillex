package kafkaex

// 内置Group
const (
	APHMQIGP_DEF = "aphmqigp_def" // APHMQIGP_DEF 是一个默认标识符
)

// 内置队列
const (
	APHMQITP_DEAD  = "aphmqitp_dead"  // APHMQITP_DEAD 表示消息处理失败，处于死信状态
	APHMQITP_RETRY = "aphmqitp_retry" // APHMQITP_RETRY 表示消息需要重试
)

// 消息头metadata
const (
	APHMQH_PARTITION_KEY = "_aphmqh_partition"  // APHMQH_PARTITION_KEY 用于标识消息所在的分区
	APHMQH_TRACE_ID      = "_aphmqh_traceid"    // APHMQH_TRACE_ID 用于标识消息的跟踪ID
	APHMQH_MSG_ID        = "_aphmqh_msgid"      // APHMQH_MSG_ID 用于唯一标识消息的ID
	APHMQH_MSG_GROUP     = "_aphmqh_msggp"      // APHMQH_MSG_GROUP 用于标识消息所属的组
	APHMQH_MSG_TOPIC     = "_aphmqh_msgtopic"   // APHMQH_MSG_TOPIC 用于标识消息的主题
	APHMQH_RETRIES       = "_aphmqh_retries"    // APHMQH_RETRIES 表示消息的重试次数
	APHMQH_RETRIES_MAX   = "_aphmqh_retriesmax" // APHMQH_RETRIES_MAX 表示消息的最大重试次数
	APHMQH_EXECER        = "_aphmqh_execer"     // APHMQH_EXECER 用于标识执行消息的实体
	APHMQH_EXECAT        = "_aphmqh_execat"     // APHMQH_EXECAT 用于标识消息执行的时间
	APHMQH_EXECERR       = "_aphmqh_execerr"    // APHMQH_EXECERR 用于记录消息执行失败的原因
	APHMQH_EXEC_TIMEOUT  = "_aphmqh_timeout"    // APHMQH_EXEC_TIMEOUT 用于标识消息执行的超时时间
)
