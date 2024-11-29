package common

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/errwrap"
	log "github.com/spirit-labs/tektite/logger"
)

func NewTektiteErrorf(errorCode ErrCode, msgFormat string, args ...interface{}) TektiteError {
	msg := fmt.Sprintf(msgFormat, args...)
	return NewTektiteError(errorCode, msg)
}

func NewTektiteError(errorCode ErrCode, msg string) TektiteError {
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

func NewInternalError(err error) TektiteError {
	// With an internal error we log the original error with a reference and we only pass the reference back to the
	// client, as we don't want to expose server internals to clients
	ref := fmt.Sprintf("tektite-internal-err-reference-%s", uuid.New().String())
	log.Errorf("internal error with reference %s: %v", ref, err)
	return NewTektiteErrorf(InternalError, "an internal error has occurred - please search server logs for reference: %s", ref)
}

func IsTektiteErrorWithCode(err error, code ErrCode) bool {
	var perr TektiteError
	if errwrap.As(err, &perr) {
		if perr.Code == code {
			return true
		}
	}
	return false
}

func IsUnavailableError(err error) bool {
	return IsTektiteErrorWithCode(err, Unavailable)
}

func Error(msg string) error {
	return errwrap.New(msg)
}

type TektiteError struct {
	Code      ErrCode
	Msg       string
	ExtraData []byte
}

func (u TektiteError) Error() string {
	return u.Msg
}

type ErrCode int

const (
	ParseError ErrCode = iota + 1000
	StatementError
	PrepareQueryError
	ExecuteQueryError
	WasmError
	Unavailable ErrCode = iota + 2000
	ConnectionError
	ShutdownError
	CompactionPollTimeout
	CompactionJobNotFound
	LevelManagerNotLeaderNode
	VersionManagerShutdown
	FailureCancelled
	RegisterDeadVersionWrongClusterVersion
	TopicAlreadyExists
	TopicDoesNotExist
	NoSuchUser
	InvalidConfiguration ErrCode = iota + 3000
	InternalError        ErrCode = iota + 5000
)
