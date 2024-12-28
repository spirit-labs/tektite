//go:build !largecluster

package scripttest

import (
	"github.com/spirit-labs/tektite/asl/scripttest"
	"testing"
)

func TestScriptStandalone(t *testing.T) {
	t.Skip()
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	scripttest.TestScript(t, 1, 1, false, nil, etcdAddress)
}

func TestScriptThreeNodes(t *testing.T) {
	t.Skip()
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	scripttest.TestScript(t, 3, 3, false, nil, etcdAddress)
}
