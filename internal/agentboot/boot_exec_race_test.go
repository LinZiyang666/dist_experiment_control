//go:build linux && race

package agentboot

// The real tether under test must share the harness's race instrumentation.
func init() { bootExecRace = true }
