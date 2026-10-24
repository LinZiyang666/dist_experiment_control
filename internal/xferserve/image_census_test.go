package xferserve

import (
	"errors"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// foreignTracker is a tracker manager the tracker package did not build.
type foreignTracker struct{ x.TrackerManager }

// The S8 broker census reads the tracker the current graph resolves. A graph
// with no resolver or no current tracker (a sleeping import before Rebind),
// or a tracker that cannot enumerate itself, refuses instead of reporting an
// idle transfer owner.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 BROKER-EXPORTER R-11
func TestImageOwnerCensusRefusesAnUnresolvedTracker(t *testing.T) {
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for name, owner := range map[string]*ImageOwner{
		"no resolver":     NewImageOwner(ImageConfig{}),
		"sleeping import": NewImageOwner(ImageConfig{Current: func() (x.TrackerManager, x.BucketPort) { return nil, nil }}),
		"foreign tracker": NewImageOwner(ImageConfig{Current: func() (x.TrackerManager, x.BucketPort) { return foreignTracker{}, nil }}),
	} {
		entries, err := owner.Census(d)
		if err == nil {
			t.Errorf("%s: census reported %+v", name, entries)
		}
		if name != "foreign tracker" && !errors.Is(err, brokerimage.ErrImage) {
			t.Errorf("%s: refusal lost the image class: %v", name, err)
		}
	}
}
