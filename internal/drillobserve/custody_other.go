//go:build !linux

package drillobserve

import "os"

// Legacy platforms keep the release observer inert and remove its private keys
// before workloads can inherit them. They never create a fake sealed receipt.
func init()                                 { _ = os.Unsetenv(EnableEnv); _ = os.Unsetenv(CustodyEnv) }
func Flush()                                {}
func ExecEnvironment(env []string) []string { return StripEnvironment(env) }

// Unsupported platforms cannot manufacture known kernel identity or zero G/FD.
// origin: hot-update S10 plan §5.3 / P06
func runtimeSampleIdentity(RuntimeBinding) (RuntimeCounters, error) {
	return RuntimeCounters{Unknown: 0xffff}, ErrRuntimeSample
}
func fillRuntimeSample(*RuntimeCounters) error   { return ErrRuntimeSample }
func takeRuntimeRequest() (runtimeRequest, bool) { return runtimeRequest{}, false }
func poisonRuntimeSamples()                      {}

func finishNativeExit(int) {}

// The S9 physical attachment contract has no supported producer here. A build
// on another platform cannot authorize a sample with invented zero identities.
// origin: hot-update S9 plan lead LQ1
func infrastructurePlatformSupported() bool { return false }
