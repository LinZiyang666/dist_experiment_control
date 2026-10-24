//go:build !linux

package main

// RunLauncher keeps its existing unsupported-platform refusal. No process is
// started here, and no Linux inherited-wait proof is claimed on other kernels.
func watchInheritedChildren() (func() error, error) {
	return func() error { return nil }, nil
}
