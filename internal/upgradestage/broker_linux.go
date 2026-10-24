//go:build linux

package upgradestage

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// A daemon with publication permissions uses the same explicit stage port as
// the root helper. Readonly deployments retain their original grants and ask
// the operator to stage them; no chmod, installer execution or new grant occurs.
// origin: hot-update S8 plan §5.2 / LQ1; S8 internal review cluster-roll F2
func PublishBroker(ctx context.Context, key upgradetx.Key, canonical, control string, store *upgradestore.Store, g upgradetx.Grant, port upgradeartifact.PublicationPort) (receipt upgradeartifact.Receipt, err error) {
	if g.Handle.Key != key {
		return upgradeartifact.Receipt{}, reject("publication belongs to another component")
	}
	if receipt, err := upgradeartifact.ReadPublication(control, g); err == nil {
		return receipt, upgradeartifact.Verify(receipt, g)
	}
	v, err := store.Prepared(key, g.Handle.TxID)
	if err != nil || v.Historical.Grant != g {
		return upgradeartifact.Receipt{}, reject("publication has no original prepared grant")
	}
	source, err := os.Open(fmt.Sprintf("/proc/%d/exe", g.Issuer.PID))
	if err != nil {
		return upgradeartifact.Receipt{}, err
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	target, err := os.Open(canonical)
	if err != nil {
		return upgradeartifact.Receipt{}, err
	}
	defer func() { err = errors.Join(err, target.Close()) }()
	return upgradeartifact.Publish(upgradeartifact.WithPublicationPort(ctx, port), g, source, target)
}
