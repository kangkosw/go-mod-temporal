package utils

import (
	"fmt"
	"os"
	"time"

	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogLevel represents log levels
type LogLevel int

const (
	// DebugLevel logs debug messages
	DebugLevel LogLevel = iota
	// InfoLevel logs info messages
	InfoLevel
	// WarnLevel logs warning messages
	WarnLevel
	// ErrorLevel logs error messages
	ErrorLevel
	// FatalLevel logs fatal messages
	FatalLevel
)

// LogConfig holds logging configuration
type LogConfig struct {
	Level       LogLevel
	OutputPath  string
	Format      string // "json" or "console"
	EnableColor bool
	TimeFormat  string

	// File rotation
	MaxSize    int  // megabytes
	MaxBackups int  // number of backups
	MaxAge     int  // days
	Compress   bool // compress rotated files
}

// Logger wraps zap logger with Temporal integration
type Logger struct {
	zap    *zap.Logger
	sugar  *zap.SugaredLogger
	config *LogConfig
}

// NewLogger creates a new logger instance
func NewLogger(config *LogConfig) (*Logger, error) {
	if config == nil {
		config = DefaultLogConfig()
	}

	zapConfig := zap.NewProductionConfig()

	// Set log level
	switch config.Level {
	case DebugLevel:
		zapConfig.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case InfoLevel:
		zapConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case WarnLevel:
		zapConfig.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case ErrorLevel:
		zapConfig.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case FatalLevel:
		zapConfig.Level = zap.NewAtomicLevelAt(zap.FatalLevel)
	}

	// Set output path
	if config.OutputPath != "" {
		zapConfig.OutputPaths = []string{config.OutputPath}
		zapConfig.ErrorOutputPaths = []string{config.OutputPath}
	}

	// Set encoding
	if config.Format == "console" {
		zapConfig.Encoding = "console"
		if config.EnableColor {
			zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		}
	} else {
		zapConfig.Encoding = "json"
	}

	// Set time format
	if config.TimeFormat != "" {
		zapConfig.EncoderConfig.TimeKey = "timestamp"
		zapConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(config.TimeFormat)
	}

	zapLogger, err := zapConfig.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &Logger{
		zap:    zapLogger,
		sugar:  zapLogger.Sugar(),
		config: config,
	}, nil
}

// DefaultLogConfig returns default logging configuration
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Level:       InfoLevel,
		OutputPath:  "stdout",
		Format:      "console",
		EnableColor: true,
		TimeFormat:  "2006-01-02 15:04:05.000",
		MaxSize:     100,
		MaxBackups:  3,
		MaxAge:      28,
		Compress:    true,
	}
}

// Debug logs debug message
func (l *Logger) Debug(msg string, keyvals ...interface{}) {
	l.sugar.Debugw(msg, keyvals...)
}

// Info logs info message
func (l *Logger) Info(msg string, keyvals ...interface{}) {
	l.sugar.Infow(msg, keyvals...)
}

// Warn logs warning message
func (l *Logger) Warn(msg string, keyvals ...interface{}) {
	l.sugar.Warnw(msg, keyvals...)
}

// Error logs error message
func (l *Logger) Error(msg string, keyvals ...interface{}) {
	l.sugar.Errorw(msg, keyvals...)
}

// Fatal logs fatal message and exits
func (l *Logger) Fatal(msg string, keyvals ...interface{}) {
	l.sugar.Fatalw(msg, keyvals...)
}

// With creates a child logger with additional fields
func (l *Logger) With(keyvals ...interface{}) log.Logger {
	return &Logger{
		zap:    l.zap,
		sugar:  l.sugar.With(keyvals...),
		config: l.config,
	}
}

// Sync flushes buffered log entries
func (l *Logger) Sync() error {
	return l.zap.Sync()
}

// Close closes the logger
func (l *Logger) Close() error {
	return l.Sync()
}

// GetZapLogger returns the underlying zap logger
func (l *Logger) GetZapLogger() *zap.Logger {
	return l.zap
}

// GetSugaredLogger returns the underlying sugared logger
func (l *Logger) GetSugaredLogger() *zap.SugaredLogger {
	return l.sugar
}

// Temporal-specific logging helpers

// LogWorkflowStart logs workflow start
func (l *Logger) LogWorkflowStart(workflowID, workflowType, taskQueue string) {
	l.Info("Workflow started",
		"workflowID", workflowID,
		"workflowType", workflowType,
		"taskQueue", taskQueue,
		"timestamp", time.Now(),
	)
}

// LogWorkflowComplete logs workflow completion
func (l *Logger) LogWorkflowComplete(workflowID, workflowType string, duration time.Duration, err error) {
	if err != nil {
		l.Error("Workflow failed",
			"workflowID", workflowID,
			"workflowType", workflowType,
			"duration", duration,
			"error", err,
		)
	} else {
		l.Info("Workflow completed",
			"workflowID", workflowID,
			"workflowType", workflowType,
			"duration", duration,
		)
	}
}

// LogActivityStart logs activity start
func (l *Logger) LogActivityStart(activityID, activityType string, attempt int32) {
	l.Info("Activity started",
		"activityID", activityID,
		"activityType", activityType,
		"attempt", attempt,
		"timestamp", time.Now(),
	)
}

// LogActivityComplete logs activity completion
func (l *Logger) LogActivityComplete(activityID, activityType string, attempt int32, duration time.Duration, err error) {
	if err != nil {
		l.Error("Activity failed",
			"activityID", activityID,
			"activityType", activityType,
			"attempt", attempt,
			"duration", duration,
			"error", err,
		)
	} else {
		l.Info("Activity completed",
			"activityID", activityID,
			"activityType", activityType,
			"attempt", attempt,
			"duration", duration,
		)
	}
}

// LogScheduleExecution logs schedule execution
func (l *Logger) LogScheduleExecution(scheduleID, workflowID string, success bool, err error) {
	if success {
		l.Info("Schedule executed successfully",
			"scheduleID", scheduleID,
			"workflowID", workflowID,
			"timestamp", time.Now(),
		)
	} else {
		l.Error("Schedule execution failed",
			"scheduleID", scheduleID,
			"workflowID", workflowID,
			"error", err,
			"timestamp", time.Now(),
		)
	}
}

// LogRetryAttempt logs retry attempt
func (l *Logger) LogRetryAttempt(workflowID string, attempt int32, nextRetry time.Duration, err error) {
	l.Warn("Retry attempt",
		"workflowID", workflowID,
		"attempt", attempt,
		"nextRetry", nextRetry,
		"error", err,
		"timestamp", time.Now(),
	)
}

// Utility functions for common logging patterns

// CreateFileLogger creates a file-based logger
func CreateFileLogger(filepath string, level LogLevel) (*Logger, error) {
	config := &LogConfig{
		Level:      level,
		OutputPath: filepath,
		Format:     "json",
		TimeFormat: time.RFC3339,
		MaxSize:    100,
		MaxBackups: 5,
		MaxAge:     30,
		Compress:   true,
	}
	return NewLogger(config)
}

// CreateConsoleLogger creates a console logger
func CreateConsoleLogger(level LogLevel, enableColor bool) (*Logger, error) {
	config := &LogConfig{
		Level:       level,
		OutputPath:  "stdout",
		Format:      "console",
		EnableColor: enableColor,
		TimeFormat:  "2006-01-02 15:04:05.000",
	}
	return NewLogger(config)
}

// CreateDevelopmentLogger creates a development logger with debug level
func CreateDevelopmentLogger() (*Logger, error) {
	config := &LogConfig{
		Level:       DebugLevel,
		OutputPath:  "stdout",
		Format:      "console",
		EnableColor: true,
		TimeFormat:  "15:04:05.000",
	}
	return NewLogger(config)
}

// CreateProductionLogger creates a production logger with structured JSON output
func CreateProductionLogger(logFile string) (*Logger, error) {
	config := &LogConfig{
		Level:      InfoLevel,
		OutputPath: logFile,
		Format:     "json",
		TimeFormat: time.RFC3339,
		MaxSize:    100,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
	}
	return NewLogger(config)
}

// LoggerMiddleware provides logging middleware for workflows
type LoggerMiddleware struct {
	logger *Logger
}

// NewLoggerMiddleware creates a new logger middleware
func NewLoggerMiddleware(logger *Logger) *LoggerMiddleware {
	return &LoggerMiddleware{logger: logger}
}

// LogExecution logs execution details
func (m *LoggerMiddleware) LogExecution(name string, start time.Time, end time.Time, err error) {
	duration := end.Sub(start)

	if err != nil {
		m.logger.Error("Execution failed",
			"name", name,
			"duration", duration,
			"error", err,
			"startTime", start,
			"endTime", end,
		)
	} else {
		m.logger.Info("Execution completed",
			"name", name,
			"duration", duration,
			"startTime", start,
			"endTime", end,
		)
	}
}

// Global logger instance
var globalLogger *Logger

// InitGlobalLogger initializes the global logger
func InitGlobalLogger(config *LogConfig) error {
	logger, err := NewLogger(config)
	if err != nil {
		return err
	}
	globalLogger = logger
	return nil
}

// GetGlobalLogger returns the global logger
func GetGlobalLogger() *Logger {
	if globalLogger == nil {
		// Create default logger if not initialized
		logger, err := CreateConsoleLogger(InfoLevel, true)
		if err != nil {
			// Fallback to basic logger
			zap, _ := zap.NewDevelopment()
			globalLogger = &Logger{
				zap:   zap,
				sugar: zap.Sugar(),
			}
		} else {
			globalLogger = logger
		}
	}
	return globalLogger
}

// Environment-based logger creation

// CreateLoggerFromEnv creates logger based on environment variables
func CreateLoggerFromEnv() (*Logger, error) {
	config := DefaultLogConfig()

	// Check environment variables
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		switch level {
		case "debug":
			config.Level = DebugLevel
		case "info":
			config.Level = InfoLevel
		case "warn":
			config.Level = WarnLevel
		case "error":
			config.Level = ErrorLevel
		case "fatal":
			config.Level = FatalLevel
		}
	}

	if format := os.Getenv("LOG_FORMAT"); format != "" {
		config.Format = format
	}

	if output := os.Getenv("LOG_OUTPUT"); output != "" {
		config.OutputPath = output
	}

	return NewLogger(config)
}
