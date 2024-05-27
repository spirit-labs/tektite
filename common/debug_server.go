package common

import (
	"fmt"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/testutils"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func RequireDebugServer(t *testing.T) {
	debugPort := testutils.PortProvider.GetPort(t)
	address := fmt.Sprintf("localhost:%d", debugPort)

	go func() {
		// Start a profiling server
		log.Debug(http.ListenAndServe(address, nil))
	}()
	log.Debugf("debug server running on %s", address)
}
