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

package common

import (
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
)

func LogInternalError(err error) errors.TektiteError {
	id, err2 := uuid.NewRandom()
	var errRef string
	if err2 != nil {
		log.Errorf("failed to generate uuid %v", err)
		errRef = ""
	} else {
		errRef = id.String()
	}
	// For internal errors we don't return internal error messages to the CLI as this would leak
	// server implementation details. Instead, we generate a random UUID and add that to the message
	// and log the internal error in the server logs with the UUID so it can be looked up
	perr := errors.NewInternalError(errRef)
	log.Errorf("internal error (reference %s) occurred %+v", errRef, err)
	return perr
}
