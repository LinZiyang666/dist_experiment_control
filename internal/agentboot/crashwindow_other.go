//go:build !linux

package agentboot

// closeForeignDescriptors is Linux-only: no image is ever dispatched on
// another platform (handoff.Prepare returns ErrUnsupported there).
type foreignDescriptor struct{}

func foreignDescriptors() ([]foreignDescriptor, error) { return nil, nil }
func closeForeignDescriptors([]foreignDescriptor) int  { return 0 }
