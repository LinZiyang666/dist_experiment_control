package agentimage

import (
	"fmt"
	"strconv"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/sealedstate"
)

const calibrationResourceID = "image-freeze-calibration"
const calibrationResourceKind = "image-freeze-calibration-v1"

// Runtime.FrozenAtBootNS keeps its S2 accounting meaning. The earlier outer
// admission time is optional telemetry in handoff's existing opaque Data slot;
// an S3 reader can safely ignore it without losing any business responsibility.
// Keep an immutable descriptor even though the value is public metadata: the
// S3 Borrow/Repack implementation preserves only resources with untaken FDs.
// This lets pristine redispatch retain calibration without adding a JSON field
// that the previous strict Bundle/Runtime decoder would reject.
// origin: hot-update S5 plan §5.2 / §7.1 / DRILL-RUN S3 compatibility
func freezeCalibration(b Bundle) (*sealedstate.Snapshot, error) {
	if b.admissionAt <= 0 || b.admissionAt > b.Runtime.FrozenAtBootNS {
		return nil, fmt.Errorf("%w: invalid calibration interval", ErrBundle)
	}
	snapshot, err := sealedstate.New(calibrationResourceID, nil)
	if err != nil {
		return nil, err
	}
	snapshot.Resources[0].Kind = calibrationResourceKind
	snapshot.Resources[0].Data = strconv.AppendInt(nil, b.admissionAt, 10)
	return snapshot, nil
}

func admissionInstant(b Bundle) int64 {
	if b.admissionAt != 0 {
		return b.admissionAt
	}
	return b.Runtime.FrozenAtBootNS
}

// Inspect immutable metadata before any business file is taken. Absence means
// the old S2/S3 representation; malformed present metadata cannot shorten the
// observed window by falling back to the later Runtime cut.
// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility
func restoreCalibration(b *Bundle, src *handoff.Restored) error {
	seen := false
	for _, resource := range src.Resources {
		if resource.ID != calibrationResourceID {
			continue
		}
		at, err := strconv.ParseInt(string(resource.Data), 10, 64)
		if seen || err != nil || resource.Kind != calibrationResourceKind || at <= 0 || at > b.Runtime.FrozenAtBootNS ||
			string(resource.Data) != strconv.FormatInt(at, 10) {
			return fmt.Errorf("%w: invalid freeze calibration", ErrBundle)
		}
		seen = true
		retained := false
		for _, borrowed := range src.Borrow() {
			if borrowed.ID == calibrationResourceID && len(borrowed.Files) == 1 && borrowed.Files["state"] != nil {
				retained = true
			}
		}
		if !retained {
			return fmt.Errorf("%w: calibration custody is missing", ErrBundle)
		}
		b.admissionAt = at
	}
	return nil
}
