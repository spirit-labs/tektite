package scripttest

import (
	"github.com/spirit-labs/tektite/testutils"
	"testing"
)

// This test file doesn't contain any tests, just the test main and tlsKeysInfo
// We put it in another file because it needs to be in the package but not in the other test files which
// are controlled by build flags
var tlsKeysInfo *TLSKeysInfo

var etcdAddress string

func TestMain(m *testing.M) {
	etcd, err := testutils.CreateEtcdContainer()
	if err != nil {
		panic(err)
	}
	etcdAddress = etcd.Address()
	defer etcd.Stop()
	m.Run()
}
