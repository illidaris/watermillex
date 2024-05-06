package kafkaex

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/illidaris/core"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 定义 ILogger 接口，提供日志的 Info 和 Error 方法。
type ILogger interface {
	InfoCtx(ctx context.Context, msg string, args ...any)
	ErrorCtx(ctx context.Context, msg string, args ...any)
}

// WaterMillLogger 实现了 ILogger 接口和 watermill.LoggerAdapter 接口，用于记录日志。
type WaterMillLogger struct {
	log *zap.Logger
}

// NewWaterMillLogger 创建并返回一个新的 WaterMillLogger 实例，该实例的日志级别为开发级别，并跳过调用者。
func NewWaterMillLogger() *WaterMillLogger {
	return &WaterMillLogger{
		log: zap.L().WithOptions(zap.AddCallerSkip(1)),
	}
}

// Error 记录一个错误日志消息，包括错误信息和额外的日志字段。
func (l *WaterMillLogger) Error(msg string, err error, fields watermill.LogFields) {
	l.log.Error(fmt.Sprintf("%s,err %v", msg, err), Field2ZapField(fields)...)
}

// ErrorCtx 使用上下文信息记录一个错误日志消息，包括格式化的消息和额外的参数。
func (l *WaterMillLogger) ErrorCtx(ctx context.Context, msg string, args ...any) {
	l.log.Error(fmt.Sprintf(msg, args...), WithTrace(ctx)...)
}

// InfoCtx 使用上下文信息记录一个信息日志消息，包括格式化的消息和额外的参数。
func (l *WaterMillLogger) InfoCtx(ctx context.Context, msg string, args ...any) {
	l.log.Info(fmt.Sprintf(msg, args...), WithTrace(ctx)...)
}

// Info 记录一个信息日志消息，包括额外的日志字段。
func (l *WaterMillLogger) Info(msg string, fields watermill.LogFields) {
	l.log.Info(msg, Field2ZapField(fields)...)
}

// Debug 记录一个调试日志消息，包括额外的日志字段。
func (l *WaterMillLogger) Debug(msg string, fields watermill.LogFields) {
	l.log.Debug(msg, Field2ZapField(fields)...)
}

// Trace 记录一个跟踪日志消息，包括额外的日志字段。实则为信息级别日志。
func (l *WaterMillLogger) Trace(msg string, fields watermill.LogFields) {
	l.log.Info(msg, Field2ZapField(fields)...)
}

// With 返回一个包含额外日志字段的日志适配器实例。
func (l *WaterMillLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	ll := &WaterMillLogger{}
	ll.log = l.log.With(Field2ZapField(fields)...)
	return ll
}

// WithTrace 从上下文中提取跟踪ID、会话ID和业务信息，并作为日志字段返回。
func WithTrace(ctx context.Context) []zapcore.Field {
	traceID := core.TraceID.GetString(ctx)
	sessionID := core.SessionID.GetString(ctx)
	return []zapcore.Field{
		zap.String(core.TraceID.String(), traceID),
		zap.String(core.SessionID.String(), sessionID),
	}
}

// Field2ZapField 将 watermill.LogFields 转换为 zapcore.Field 切片，以供日志记录使用。
func Field2ZapField(fs watermill.LogFields) []zapcore.Field {
	fields := []zapcore.Field{}
	for k, v := range fs {
		fields = append(fields, zap.Any(k, v))
	}
	return fields
}
