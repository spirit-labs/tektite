//go:build !largecluster

package scripttest

import (
	"testing"
)

func TestScriptStandalone(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	testScript(t, 1, 1, false, tlsKeysInfo)
}

func TestScriptThreeNodes(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("-short: skipped")
	}
	testScript(t, 3, 3, false, tlsKeysInfo)
}

//	func TestScriptFiveNodes(t *testing.T) {
//		if testing.Short() {
//			t.Skip("-short: skipped")
//		}
//		testScript(t, 5, 3, false, tlsKeysInfo)
//	}
