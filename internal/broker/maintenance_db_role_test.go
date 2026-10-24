package broker

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
)

// A missing cluster runtime cannot turn a read pool into a single writer.
// Refusal must happen before either the planner or local record SQL is entered.
// origin: S7 internal review GATE-CONVERGE-contracts F6
func TestBrokerMaintenanceRejectsMissingClusterWriter(t *testing.T) {
	b := &Broker{clusterMode: true, cfg: Config{DB: &sql.DB{}}}
	called := false
	err := maintenanceWrite(b, func(*sql.DB) (*cluster.Command, error) {
		called = true
		return nil, nil
	})
	if !errors.Is(err, errClusterModeSingleWrite) || called {
		t.Fatalf("missing cluster writer entered planner: called=%t err=%v", called, err)
	}
	if db, err := brokerMaintenanceDB(b, nil); db != nil || !errors.Is(err, errClusterModeSingleWrite) {
		t.Fatalf("read pool was handed out for single maintenance: db=%p err=%v", db, err)
	}
	if err := finishBrokerLocalMaintenance(b, nil, brokermaint.LocalLease{}); !errors.Is(err, errClusterModeSingleWrite) {
		t.Fatalf("local maintenance entered SQL without a writer: %v", err)
	}
	b.clusterMode = false
	if db, err := brokerMaintenanceDB(b, nil); err != nil || db != b.cfg.DB {
		t.Fatalf("single writable role was refused: db=%p err=%v", db, err)
	}
}
