package broker

// These are outer wiring tests. Factories/subscription descriptors are injected;
// they do not substitute for P16/P22/P23's real transfer routing or P32's exec.
import (
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats.go"
)

type imageTransferMarker struct{ brokerimage.TransferOwner }

// origin: hot-update S7 plan §6.3 / T39 / P24
func TestTransferHotSubscriptionsUseCurrentBindings(t *testing.T) {
	b := &Broker{}
	owner := &imageTransferMarker{}
	oldNC, currentNC := &nats.Conn{}, &nats.Conn{}
	var connections []*nats.Conn
	factory := func(nc *nats.Conn) []TransferImageSubscription {
		connections = append(connections, nc)
		return []TransferImageSubscription{{Subject: proto.SubjectPrefix + ".s.*.cmd.by.*.node.*.transfer-status.req", Handler: func(*nats.Msg) {}}}
	}
	if err := ConfigureBrokerTransferImage(b, owner, factory); err != nil {
		t.Fatal(err)
	}
	defer b.imageControl.cancel()
	for _, nc := range []*nats.Conn{oldNC, currentNC} {
		subscriptions := brokerBusinessSubscriptions(b, nc)
		count := 0
		for _, spec := range subscriptions {
			if spec.subj == proto.SubjectPrefix+".s.*.cmd.by.*.node.*.transfer-status.req" {
				count++
			}
		}
		if count != 1 {
			t.Fatal("cold/hot table did not consume the one transfer descriptor")
		}
	}
	if !reflect.DeepEqual(connections, []*nats.Conn{oldNC, currentNC}) {
		t.Fatal("hot subscriptions captured a retired NC")
	}
	if err := ConfigureBrokerTransferImage(b, &imageTransferMarker{}, factory); err == nil {
		t.Fatal("active transfer wiring replaced without a cut")
	}
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferImageConfigurationRejectsIncompleteOwner(t *testing.T) {
	if err := ConfigureBrokerTransferImage(nil, &imageTransferMarker{}, func(*nats.Conn) []TransferImageSubscription { return nil }); err == nil {
		t.Fatal("nil broker accepted")
	}
	b := &Broker{}
	if err := ConfigureBrokerTransferImage(b, nil, func(*nats.Conn) []TransferImageSubscription { return nil }); err == nil {
		t.Fatal("nil tracker accepted")
	}
	if err := ConfigureBrokerTransferImage(b, &imageTransferMarker{}, nil); err == nil {
		t.Fatal("missing registration descriptor accepted")
	}
	var typedNil *imageTransferMarker
	if err := ConfigureBrokerTransferImage(b, typedNil, func(*nats.Conn) []TransferImageSubscription { return nil }); err == nil {
		t.Fatal("typed nil tracker accepted")
	}
	if b.imageControl != nil {
		t.Fatal("invalid wiring allocated a control lifetime")
	}
}

// origin: hot-update S7 plan §8.2 / T37 / P24
func TestTransferImageControlReplacementKeepsCurrentOwner(t *testing.T) {
	if err := brokerimage.InstallColdSingleStoreSupport(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	store, db, err := brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{DBPath: filepath.Join(root, "broker.db"), DataDir: root, Registry: storage.DefaultDriverRegistry()})
	if store != nil {
		t.Cleanup(func() { _ = store.(io.Closer).Close() })
	}
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{cfg: Config{DB: db}}
	marker := &imageTransferMarker{}
	called := false
	factory := func(*nats.Conn) []TransferImageSubscription { called = true; return nil }
	if err := ConfigureBrokerTransferImage(b, marker, factory); err != nil {
		t.Fatal(err)
	}
	old := b.imageControl
	defer old.cancel()
	current, err := replaceBrokerImageControl(b, store, old)
	if current != nil {
		defer current.cancel()
	}
	if err != nil {
		t.Fatal(err)
	}
	if current == old || current.transfers != marker || b.imageControl != current || b.cfg.DB != db {
		t.Fatal("replacement dropped the retained tracker or installed a captured control generation")
	}
	if current.transferSubscriptions == nil {
		t.Fatal("replacement lost the common transfer registration descriptor")
	}
	current.transferSubscriptions(&nats.Conn{})
	if !called {
		t.Fatal("replacement did not preserve transfer subscription factory")
	}
}
