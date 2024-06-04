package common

import (
	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"net/http"
	_ "net/http/pprof"
	"testing"
)

func RequireDebugServer(t *testing.T) {
	EnableTestPorts()
	address, err := AddressWithPort("localhost")
	require.NoError(t, err)
	go func() {
		// Start a profiling server
		log.Debug(http.ListenAndServe(address, nil))
	}()
	log.Debugf("debug server running on %s", address)
}
