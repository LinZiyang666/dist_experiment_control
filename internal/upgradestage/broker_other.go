//go:build !linux

package upgradestage

import (
	"context"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// PublishBroker requires Linux's independent /proc running-image witness.
func PublishBroker(context.Context, upgradetx.Key, string, string, *upgradestore.Store, upgradetx.Grant, upgradeartifact.PublicationPort) (upgradeartifact.Receipt, error) {
	return upgradeartifact.Receipt{}, &upgradetx.Error{Code: upgradetx.CodeUnsupportedComponent, Detail: "broker stage requires Linux"}
}
