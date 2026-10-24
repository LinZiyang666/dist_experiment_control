package xferclient

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go/jetstream"
)

// The fixed version alone cannot detect duplicate physical rows under the same
// subject. Read raw stream sequences and complete chunk bytes across the lost
// commit reply, including rows whose object metadata was replaced by SDK Put.
// origin: S7 internal review rv-wiring F10 / T49 / M01
func clientPhysicalChunks(t *testing.T, f *clientFixture) map[uint64][32]byte {
	t.Helper()
	stream, err := f.js.Stream(t.Context(), "OBJ_"+proto.XferBucketName("session"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[uint64][32]byte)
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && seq != 0; seq++ {
		msg, err := stream.GetMsg(t.Context(), seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(msg.Subject, "$O."+proto.XferBucketName("session")+".C.") {
			continue
		}
		result[seq] = sha256.Sum256(append(append([]byte(msg.Subject), 0), msg.Data...))
	}
	if len(result) == 0 {
		t.Fatal("empty physical chunk oracle")
	}
	return result
}

func assertClientPhysicalChunks(t *testing.T, f *clientFixture, before map[uint64][32]byte) {
	t.Helper()
	after := clientPhysicalChunks(t, f)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("capable resume changed original physical chunk sequences: before=%v after=%v", before, after)
	}
}
