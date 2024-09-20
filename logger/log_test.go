package logger

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func TestTektiteLoggerEnabledLevels(t *testing.T) {
	config := Config{
		Level:  "warn",
		Format: "console",
	}
	loggerName := "Test Tektite Logger"
	err := config.Configure()
	require.NoError(t, err)

	testTektiteLogger, err := GetLogger(loggerName)
	require.NoError(t, err)
	testTektiteLogger.Infof("testing logging")
	testTektiteLogger.Warnf("WARN testing logging %s", "args")
	testTektiteLogger.Warn("msg 1", "msg 2")

	// assert it uses the globally-configured log level by default
	require.False(t, testTektiteLogger.logger.Core().Enabled(zap.DebugLevel), "Expected the logger to be disabled for Debug level because the default is Warn")
	require.False(t, testTektiteLogger.logger.Core().Enabled(zap.InfoLevel), "Expected the logger to be disabled for Info level because the default is Warn")
	require.True(t, testTektiteLogger.logger.Core().Enabled(zap.WarnLevel), "Expected the logger to be enabled for Warn level because the default is Warn")

	// assert you can't override it with the same name
	testSameTektiteLogger, err := GetLoggerWithLevel(loggerName, zap.DebugLevel)
	// assert the log level hasn't changed
	require.False(t, testSameTektiteLogger.logger.Core().Enabled(zap.DebugLevel), "Expected the logger to be disabled for Debug level because the default is Warn")
	require.False(t, testSameTektiteLogger.logger.Core().Enabled(zap.InfoLevel), "Expected the logger to be disabled for Info level because the default is Warn")
	require.True(t, testSameTektiteLogger.logger.Core().Enabled(zap.WarnLevel), "Expected the logger to be enabled for Warn level because the default is Warn")
}

func TestTektiteLogger(t *testing.T) {
	config := Config{
		Level:  "debug",
		Format: "console",
	}
	loggerName := "Test-Tektite-Logger"
	err := config.Configure()
	require.NoError(t, err)

	testTektiteLogger, err := GetLogger(loggerName)
	require.NoError(t, err)

	testTektiteLogger.Debug("debug 1", " debug 2")
	testTektiteLogger.Debugf("debug %d debug %d", 1, 2)
	testTektiteLogger.Info("info 1", " info 2")
	testTektiteLogger.Infof("info %d info %d", 1, 2)
	testTektiteLogger.Warn("warn 1", " warn 2")
	testTektiteLogger.Warnf("warn %d warn %d", 1, 2)
	testTektiteLogger.Error("error 1", " error 2")
	testTektiteLogger.Errorf("error %d error %d", 1, 2)
}

func TestTektiteLoggerReturnsErrorIfGlobalLoggerUninitialized(t *testing.T) {
	_, err := GetLogger("test logger")
	require.Error(t, err, "should not be able to get a logger before the global one is initialized")
}
