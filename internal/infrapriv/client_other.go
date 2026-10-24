//go:build !linux

package infrapriv

import (
	"net"
	"os"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Unsupported platforms retain ordinary CLI/build/install behavior; no kernel
// authorization or privileged hot-cut promise is inferred from compilation.
// origin: hot-update S9 plan lead LQ1 / §6.1
func supportedPlatform() error                                 { return c.ErrUnsupported }
func PeerCredentials(*net.UnixConn) (c.Peer, error)            { return c.Peer{}, c.ErrUnsupported }
func sendFiles(*net.UnixConn, []byte, []*os.File) (int, error) { return 0, c.ErrUnsupported }
func validateReceivedFile(*os.File, bool) error                { return c.ErrUnsupported }
func LoadCredential(string, CredentialKind, uint32) (*Credential, error) {
	return nil, c.ErrUnsupported
}
func LoadPolicy(string) (Policy, error)              { return Policy{}, c.ErrUnsupported }
func BootstrapCredentials(string, uint32) error      { return c.ErrUnsupported }
func NewWorkerLimit(string) (*WorkerLimit, error)    { return nil, c.ErrUnsupported }
func acquireWorkerSlot(*WorkerLimit) (func(), error) { return nil, c.ErrUnsupported }
func NewClient(uint32) (*Client, error)              { return nil, c.ErrUnsupported }
func (*Client) Do(c.HostRequest, c.Deadline, c.Stop) (c.HostReceipt, error) {
	return c.HostReceipt{}, c.ErrUnsupported
}
func (*Client) Exchange(c.HostRequest, []*os.File, c.Deadline, c.Stop) (c.HostReceipt, []*os.File, error) {
	return c.HostReceipt{}, nil, c.ErrUnsupported
}

func receiveFilesLimit(*net.UnixConn, []byte, int) (int, []*os.File, error) {
	return 0, nil, c.ErrUnsupported
}
func validateLaunchFiles([]*os.File) error { return c.ErrUnsupported }
