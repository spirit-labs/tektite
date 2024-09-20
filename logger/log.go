/*
log.go provides a centralized logging system for the application.

It offers a global logger as well as the ability to create named loggers
for different parts of Tektite. The package wraps the zap logging
library, providing a simplified interface while leveraging zap's performance.

Usage:

	// Using the global logger
	import log "github.com/spirit-labs/tektite/logger"
	log.Info("Application started")
	log.Errorf("Failed to connect: %v", err)
	=> "Failed  to connect: connection refused"

	// Creating and using a named logger
	log, _ := logger.GetLogger("clustered_data.go")
	log.Debug("Something happened!")
	=> "[clustered_data.go] Something happened!"
*/
package logger

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
	"time"
)

var globalLogger *zap.Logger
var globalLog *zap.SugaredLogger
var initLock sync.Mutex
var initialised bool
var globalEncoding string
var globalLevel zapcore.Level
var DebugEnabled = false
var loggersByName = make(map[string]*TektiteLogger)

type Config struct {
	Format string `help:"Format to write log lines in" enum:"console,json" default:"console"`
	Level  string `help:"Lowest log level that will be emitted" enum:"debug,info,warn,error" default:"info"`
}

func (cfg *Config) Configure() error {
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(cfg.Level))); err != nil {
		return err
	}
	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	if format != "console" && format != "json" {
		return errwrap.New("log-format must be one of 'console' or 'json'")
	}
	initialise(level, format, true)
	return nil
}

func initialise(level zapcore.Level, encoding string, override bool) {
	initLock.Lock()
	defer initLock.Unlock()
	if initialised && !override {
		return
	}
	globalLogger = CreateLogger(level, encoding)
	globalLog = globalLogger.Sugar()
	// Cache as simple bool to avoid atomics - we never change it after initialisation so this is ok
	DebugEnabled = globalLog.Desugar().Core().Enabled(zap.DebugLevel)
	globalEncoding = encoding
	globalLevel = level

	initialised = true
}

func CreateLogger(level zapcore.Level, encoding string) *zap.Logger {
	encoderConf := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	conf := zap.Config{
		Level:       zap.NewAtomicLevelAt(level),
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    150,
			Thereafter: 150,
		},
		Encoding:          encoding,
		EncoderConfig:     encoderConf,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stdout"},
		DisableCaller:     true,
		DisableStacktrace: true,
	}
	l, _ := conf.Build()
	return l
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.999999"))
}

func Info(args ...interface{}) {
	globalLog.Info(args)
}

func Infof(format string, args ...interface{}) {
	globalLog.Infof(format, args...)
}

func Debug(args ...interface{}) {
	if !DebugEnabled {
		return
	}
	globalLog.Debug(args)
}

func Debugf(format string, args ...interface{}) {
	globalLog.Debugf(format, args...)
}

func Warn(args ...interface{}) {
	globalLog.Warn(args)
}

func Warnf(format string, args ...interface{}) {
	globalLog.Warnf(format, args...)
}

func Error(args ...interface{}) {
	globalLog.Error(args)
}

func Errorf(format string, args ...interface{}) {
	globalLog.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	globalLog.Fatal(args)
}

func Fatalf(format string, args ...interface{}) {
	globalLog.Fatalf(format, args...)
}

// GetLogger gets a logger with the given name. The name is logged as a prefix for any message coming from this logger.
func GetLogger(loggerName string) (*TektiteLogger, error) {
	return GetLoggerWithLevel(loggerName, globalLevel)
}

func GetLoggerWithLevel(loggerName string, level zapcore.Level) (*TektiteLogger, error) {
	initLock.Lock()
	defer initLock.Unlock()
	if !initialised {
		return nil, errors.Errorf("cannot create a logger with name %s before the global logger is initialised", loggerName)
	}

	if value, exists := loggersByName[loggerName]; exists {
		Warnf("tried to create a duplicate logger with name %s", loggerName)
		return value, nil
	}

	logger := CreateLogger(level, globalEncoding)
	tektiteLogger := NewTektiteLogger(logger, loggerName)
	loggersByName[loggerName] = tektiteLogger
	return tektiteLogger, nil
}

type TektiteLogger struct {
	logger       *zap.Logger
	log          *zap.SugaredLogger
	debugEnabled bool
	prefix       string
}

func NewTektiteLogger(logger *zap.Logger, loggerName string) *TektiteLogger {
	log := logger.Sugar()

	return &TektiteLogger{
		logger:       logger,
		log:          log,
		debugEnabled: log.Desugar().Core().Enabled(zap.DebugLevel),
		prefix:       fmt.Sprintf("[%s] ", loggerName),
	}
}

func (t *TektiteLogger) Info(args ...interface{}) {
	t.log.Info(append([]interface{}{t.prefix}, args...)...)
}

func (t *TektiteLogger) Infof(format string, args ...interface{}) {
	t.log.Infof(t.prefix+format, args...)
}

func (t *TektiteLogger) Debug(args ...interface{}) {
	if !t.debugEnabled {
		return
	}
	t.log.Debug(append([]interface{}{t.prefix}, args...)...)
}

func (t *TektiteLogger) Debugf(format string, args ...interface{}) {
	t.log.Debugf(t.prefix+format, args...)
}

func (t *TektiteLogger) Warn(args ...interface{}) {
	t.log.Warn(append([]interface{}{t.prefix}, args...)...)
}

func (t *TektiteLogger) Warnf(format string, args ...interface{}) {
	t.log.Warnf(t.prefix+format, args...)
}

func (t *TektiteLogger) Error(args ...interface{}) {
	t.log.Error(append([]interface{}{t.prefix}, args...)...)
}

func (t *TektiteLogger) Errorf(format string, args ...interface{}) {
	t.log.Errorf(t.prefix+format, args...)
}

func (t *TektiteLogger) Fatal(args ...interface{}) {
	t.log.Fatal(append([]interface{}{t.prefix}, args...)...)
}

func (t *TektiteLogger) Fatalf(format string, args ...interface{}) {
	t.log.Fatalf(t.prefix+format, args...)
}
