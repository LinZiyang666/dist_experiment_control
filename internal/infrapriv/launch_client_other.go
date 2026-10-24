//go:build !linux

package infrapriv

import c "github.com/LinZiyang666/tether/internal/infracontract"

func LoadLaunchIdentity(string, uint32) (LaunchIdentity, error) {
	return LaunchIdentity{}, c.ErrUnsupported
}
func (*Client) QueryLaunch(LaunchQuery, c.Deadline, c.Stop) (c.HostRequest, error) {
	return c.HostRequest{}, c.ErrUnsupported
}
func (*Client) SubmitLaunchAck(c.HostRequest, c.Deadline, c.Stop) (func(), error) {
	return nil, c.ErrUnsupported
}
