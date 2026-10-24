// Package ssstate owns the descriptor-validated private SS image codec and
// proxy control cuts. It performs no IO; callers seal returned bytes immediately.
// origin: hot-update S5 plan §5.1 / §6.6 / §12.1
package ssstate

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/proxyimage"
)

var ErrStateUnrepresentable = errors.New("ssproxy: state unrepresentable")

type ProxyState = proxyimage.ProxyState
type ProxyCut = proxyimage.ProxyCut

func validProxyState(p ProxyState) bool { return proxyimage.ValidProxyState(p) }
