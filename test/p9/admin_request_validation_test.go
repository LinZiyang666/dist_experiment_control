package p9_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// TestAdminRejectsMalformedRequest covers the strict JSON rejection path.
// S8 returns a stable bad_request envelope without parser internals. Duplicate
// routing keys and trailing JSON must not select a different decoder.
// origin: hot-update S8 internal review LEGACY-ROUTE-TESTS; plan §2.2 / §7.1
func TestAdminRejectsMalformedRequest(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseAdminMalformedRequest(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "malformed admin request", g)
	testharness.AssertNoFDLeak(t, "malformed admin request", fd, 0)
}

func exerciseAdminMalformedRequest(t *testing.T) {
	t.Helper()
	url := startNATS(t)
	db := openDB(t)
	socketPath := adminSocketPath(t)
	defer startBrokerWithAdmin(t, url, db, socketPath)()
	for _, body := range []string{"not-json", `{"op":"runtime","op":"creators"}`, `{"op":"runtime"} {"op":"creators"}`} {
		conn, err := osDialUnix(socketPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte(body + "\n")); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		var response adminsock.Response
		if err := json.Unmarshal(line, &response); err != nil {
			t.Fatal(err)
		}
		if response.OK || response.Code != adminsock.CodeBadRequest || response.Error != "invalid admin request" || response.Runtime != nil || response.Upgrade != nil || response.Infra != nil {
			t.Fatalf("malformed JSON did not return a clean bad_request: %s", line)
		}
		// A malformed client cannot panic the handler or disable ordinary admin.
		after, err := (&adminsock.Client{Path: socketPath}).Call(adminsock.Request{Op: adminsock.OpRuntime})
		if err != nil || !after.OK || after.Runtime == nil {
			t.Fatalf("admin did not survive malformed input: %+v %v", after, err)
		}
	}
}
