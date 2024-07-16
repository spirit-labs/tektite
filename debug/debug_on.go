//go:build !main
// +build !main

package debug

// When built for testing, debug will be on

//goland:noinspection GoUnusedGlobalVariable
var Debug = true
var SanityChecks = true

var AggregateChecks = false
