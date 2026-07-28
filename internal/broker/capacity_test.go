package broker

import "testing"

// g6_capacity_test.go (G6 #21) — the disk-aware OBJ_xfer bucket sizing HARD invariant: the computed
// MaxBytes is ALWAYS in (floor, cap] and NEVER <=0 (nats treats MaxBytes<=0 as UNLIMITED → a worse silent
// re-brick), and when the store cannot even fit the floor it REFUSES rather than emit a bad number.
func TestXferMaxBytesForCeiling(t *testing.T) {
	const G = int64(1024 * 1024 * 1024)
	cases := []struct {
		name    string
		ceiling int64
		wantErr bool
		check   func(int64) bool
	}{
		{"unknown ceiling → legacy cap", 0, false, func(v int64) bool { return v == xferBucketCap }},
		{"negative ceiling → legacy cap", -1, false, func(v int64) bool { return v == xferBucketCap }},
		{"tiny 4 GiB store fits below cap", 4 * G, false, func(v int64) bool { return v > xferBucketFloor && v < 4*G }},
		{"racknerd ~10.5 GiB", 10*G + G/2, false, func(v int64) bool { return v > xferBucketFloor && v <= xferBucketCap }},
		{"huge store clamps to 8 GiB cap", 500 * G, false, func(v int64) bool { return v == xferBucketCap }},
		{"too small even for the floor → refuse", 2*G + 100*1024*1024, true, nil}, // avail = ceiling-2G < floor
		{"exactly reserve → refuse (avail 0)", 2 * G, true, nil},
	}
	for _, tc := range cases {
		got, err := xferMaxBytesForCeiling(tc.ceiling)
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: want refuse, got MaxBytes=%d", tc.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected err: %v", tc.name, err)
			continue
		}
		// HARD invariant: never <=0 (UNLIMITED footgun), never above the legacy cap.
		if got <= 0 || got > xferBucketCap {
			t.Errorf("%s: MaxBytes=%d violates the (0, cap] invariant", tc.name, got)
		}
		if !tc.check(got) {
			t.Errorf("%s: MaxBytes=%d failed the case-specific check", tc.name, got)
		}
	}
}
