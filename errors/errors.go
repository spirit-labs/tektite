// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package errors

import (
	"fmt"
)

type ErrorCode int

const (
	ParseError = iota + 1000
	StatementError
	PrepareQueryError
	ExecuteQueryError
	WasmError
	Unavailable = iota + 2000
	ConnectionError
	ShutdownError
	CompactionPollTimeout
	CompactionJobNotFound
	LevelManagerNotLeaderNode
	VersionManagerShutdown
	FailureCancelled
	InvalidConfiguration = iota + 3000
	InternalError        = iota + 5000
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
