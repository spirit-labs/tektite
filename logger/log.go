package logger

import (
	"github.com/spirit-labs/tektite/asl/errwrap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
	"sync"
	"time"
)

var logger *zap.Logger
var log *zap.SugaredLogger
var initLock sync.Mutex
var initialised bool

func init() {
	initialise(zapcore.InfoLevel, "console", false)
}

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
	Initialise(level, format)
	return nil
}

var DebugEnabled = false

func Initialise(level zapcore.Level, encoding string) {
	initialise(level, encoding, true)
}

func initialise(level zapcore.Level, encoding string, override bool) {
	initLock.Lock()
	defer initLock.Unlock()
	if initialised && !override {
		return
	}
	logger = CreateLogger(level, encoding)
	log = logger.Sugar()

	// Cache as simple bool to avoid atomics - we never change it after initialisation so this is ok
	DebugEnabled = log.Desugar().Core().Enabled(zap.DebugLevel)

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
		Level:            zap.NewAtomicLevelAt(level),
		Development:      false,
		Sampling:         nil,
		Encoding:         encoding,
		EncoderConfig:    encoderConf,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stdout"},
	}
	conf.DisableCaller = true
	conf.DisableStacktrace = true
	l, _ := conf.Build()
	return l
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.999999"))
}

func Info(args ...interface{}) {
	log.Info(args)
}

func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func Debug(args ...interface{}) {
	if !DebugEnabled {
		return
	}
	log.Debug(args)
}

func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func Warn(args ...interface{}) {
	log.Warn(args)
}

func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

func Error(args ...interface{}) {
	log.Error(args)
}

func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	log.Fatal(args)
}

func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
