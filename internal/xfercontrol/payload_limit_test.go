package xfercontrol

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Copy into internal/xfercontrol. MaxMsgSize=-1 is nats-server's ordinary
// unbounded stream value; MaxPayload still gives a known finite ceiling.
// origin: S7 internal review rv-jsnats F4
func TestPrepareAcceptsUnlimitedStreamMessageSize(t *testing.T) {
	f := newControlFixture(t)
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS did not start")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	a, err := xferbucket.NewAuthority(x.ConfigWriterProof{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Nonce: "nonce", Generation: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := xferbucket.New(xferbucket.Config{Base: x.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return "boot", now, err }}, JS: js, Authority: a})
	if err != nil {
		t.Fatal(err)
	}
	f.seal, err = owner.Apply(x.ConfigPatch{Bucket: "xfer-session", MaxBytes: 256 << 20}, testDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("actual SDK-created stream seal: MaxMsgSize=%d MaxPayload=%d", f.seal.MaxMsgSize, f.seal.MaxPayload)
	r, err := f.ctl.Handle(f.request("prepare"), f.principal, testDeadline(t))
	if err != nil || r.Code != "" || f.forwards != 1 {
		t.Fatalf("valid unlimited stream rejected despite finite MaxPayload=%d: response=%+v err=%v forwards=%d", f.seal.MaxPayload, r, err, f.forwards)
	}
}

// origin: S7 internal review rv-jsnats F4
func TestTransferPayloadLimitsRequireKnownConnection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connection int64
		streams    []int64
		want       int64
	}{
		{"unlimited_negative", 1 << 20, []int64{-1}, 1 << 20},
		{"unlimited_zero", 1 << 20, []int64{0}, 1 << 20},
		{"connection_only", 1 << 20, nil, 1 << 20},
		{"smallest_positive", 1 << 20, []int64{-1, 512 << 10, 0, 256 << 10, 2 << 20}, 256 << 10},
		{"connection_is_smaller", 64 << 10, []int64{1 << 20, -1}, 64 << 10},
		{"unknown_zero", 0, []int64{1 << 20}, 0},
		{"unknown_negative", -1, []int64{1 << 20}, 0},
		{"all_unknown", 0, []int64{-1, 0}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := jsstream.TransferPayloadLimit(tc.connection, tc.streams...)
			if tc.want == 0 {
				if err == nil || got != 0 || !strings.Contains(err.Error(), x.CodePayloadUnsupported) {
					t.Fatalf("unknown connection acquired capability: limit=%d err=%v", got, err)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("effective payload=%d err=%v; want %d", got, err, tc.want)
			}
			limits := append([]int64{tc.connection}, tc.streams...)
			geometry, geometryErr := jsstream.TransferChunkSize(x.ObjectSpec{}, limits...)
			if tc.want == 0 && (geometryErr == nil || geometry != 0) {
				t.Fatalf("stream limit concealed unknown connection: geometry=%d err=%v", geometry, geometryErr)
			}
			if tc.want != 0 && (geometryErr != nil || geometry < 32<<10 || int64(geometry) >= tc.want) {
				t.Fatalf("unusable geometry=%d err=%v for payload=%d", geometry, geometryErr, tc.want)
			}
		})
	}
	if size, err := jsstream.TransferChunkSize(x.ObjectSpec{}); err == nil || size != 0 {
		t.Fatalf("missing payload evidence yielded geometry=%d err=%v", size, err)
	}
}

// origin: hot-update S7 plan §2.1 / T09; S7 internal review rv-jsnats F4
func TestPrepareChecksEncodedPayloadGeometry(t *testing.T) {
	for _, chunkSize := range []uint32{32 << 10, 128 << 10} {
		for _, version := range []string{"original-NUID", strings.Repeat("v", 256)} {
			for _, source := range []string{"probe", "seal"} {
				for _, limit := range []string{"unlimited_negative", "unlimited_zero", "unknown_zero", "unknown_negative", "small_positive", "no_header_room", "short_header_room", "exact_stream", "exact_connection", "connection_cap"} {
					t.Run(fmt.Sprintf("chunk_%d/version_%d/%s/%s", chunkSize, len(version), source, limit), func(t *testing.T) {
						f := newControlFixture(t)
						f.id.ObjectVersion = version
						f.intent.ChunkSize = chunkSize
						f.id.IntentDigest = IntentDigest(f.intent)
						f.permit.Identity, f.status.Identity = f.id, f.id
						// Encode the frozen wire headers independently of the geometry helper.
						// Numeric maxima cover later chunks and CAS receipts; the version is
						// varied so a fixed overhead estimate cannot pass the boundary pairs.
						headers := nats.Header{
							"Tether-Xfer-Version": {"1"}, "Tether-Xfer-Index": {"4294967295"},
							"Tether-Xfer-Offset": {"9223372036854775807"}, "Tether-Xfer-Length": {"4294967295"},
							"Tether-Xfer-SHA256": {strings.Repeat("0", 64)}, "Tether-Xfer-Intent": {f.id.IntentDigest},
							"Nats-Msg-Id":                         {"xfer:" + version + ":4294967295"},
							"Nats-Expected-Last-Subject-Sequence": {"18446744073709551615"},
							"Nats-Expected-Last-Sequence":         {"18446744073709551615"},
						}
						msg := &nats.Msg{Header: headers}
						encodedLimit := int64(f.intent.ChunkSize) + int64(msg.Size())
						payload, stream, allowed := int64(1<<20), int64(-1), false
						switch limit {
						case "unlimited_negative":
							allowed = true
						case "unlimited_zero":
							stream, allowed = 0, true
						case "unknown_zero":
							payload, stream = 0, 1<<20
						case "unknown_negative":
							payload, stream = -1, 1<<20
						case "small_positive":
							stream = int64(f.intent.ChunkSize) - 1
						case "no_header_room":
							stream = int64(f.intent.ChunkSize)
						case "short_header_room":
							stream = encodedLimit - 1
						case "exact_stream":
							stream, allowed = encodedLimit, true
						case "exact_connection":
							payload, allowed = encodedLimit, true
						case "connection_cap":
							payload, stream = encodedLimit-1, 1<<20
						}
						if source == "probe" {
							f.probe.MaxPayload, f.probe.MaxMsgSize = payload, stream
						} else {
							f.seal.MaxPayload, f.seal.MaxMsgSize = payload, stream
						}
						r, err := f.ctl.Handle(f.request(x.OpPrepare), f.principal, testDeadline(t))
						if !allowed {
							requireCode(t, r, err, x.CodePayloadUnsupported)
							if f.forwards != 0 || (source == "probe" && f.admits != 0) {
								t.Fatalf("unsupported geometry escaped admission: admits=%d forwards=%d", f.admits, f.forwards)
							}
						} else if err != nil || r.Code != "" || r.Status == nil || f.forwards != 1 {
							t.Fatalf("valid geometry rejected: payload=%d stream=%d encoded=%d response=%+v err=%v forwards=%d", payload, stream, encodedLimit, r, err, f.forwards)
						}
					})
				}
			}
		}
	}
}
