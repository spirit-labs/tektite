package errors

import (
	"fmt"
)

type ErrorCode int

const (
	ParseError ErrorCode = iota + 1000
	StatementError
	PrepareQueryError
	ExecuteQueryError
	WasmError
	Unavailable ErrorCode = iota + 2000
	ConnectionError
	ShutdownError
	CompactionPollTimeout
	CompactionJobNotFound
	LevelManagerNotLeaderNode
	VersionManagerShutdown
	FailureCancelled
	InvalidConfiguration ErrorCode = iota + 3000
	InternalError        ErrorCode = iota + 5000
)

func NewInternalError(errReference string) TektiteError {
	return NewTektiteErrorf(InternalError, "internal error - reference: %s please consult server logs for details", errReference)
}

func NewInvalidConfigurationError(msg string) TektiteError {
	return NewTektiteErrorf(InvalidConfiguration, "invalid configuration: %s", msg)
}

func NewTektiteErrorf(errorCode ErrorCode, msgFormat string, args ...interface{}) TektiteError {
	msg := fmt.Sprintf(msgFormat, args...)
	return TektiteError{Code: errorCode, Msg: msg}
}

func NewTektiteError(errorCode ErrorCode, msg string) TektiteError {
	return TektiteError{Code: errorCode, Msg: msg}
}

func NewParseError(msg string) error {
	return NewTektiteError(ParseError, msg)
}

func NewStatementError(msg string) error {
	return NewTektiteError(StatementError, msg)
}

func NewQueryErrorf(msg string, args ...interface{}) error {
	return NewTektiteErrorf(ExecuteQueryError, msg, args...)
}

func Error(msg string) error {
	return New(msg)
}

type TektiteError struct {
	Code      ErrorCode
	Msg       string
	ExtraData []byte
}

func (u TektiteError) Error() string {
	return u.Msg
}
