package clusteroffline_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
)

func TestD9ExternalReviewInitRejectsEmptyClusterIdentity(t *testing.T) {
	required := map[string]func(*clusteroffline.InitFromExistingOptions){
		"Name":         func(o *clusteroffline.InitFromExistingOptions) { o.Name = "" },
		"NodeIdentPub": func(o *clusteroffline.InitFromExistingOptions) { o.NodeIdentPub = "" },
		"NatsRoute":    func(o *clusteroffline.InitFromExistingOptions) { o.NatsRoute = "" },
		"TunnelAddr":   func(o *clusteroffline.InitFromExistingOptions) { o.TunnelAddr = "" },
		"PublicHost":   func(o *clusteroffline.InitFromExistingOptions) { o.PublicHost = "" },
	}
	for field, clear := range required {
		t.Run(field, func(t *testing.T) {
			tmp := t.TempDir()
			dataDir := filepath.Join(tmp, "data")
			dbPath := filepath.Join(dataDir, "tether.db")
			if err := os.MkdirAll(dataDir, 0o700); err != nil {
				t.Fatal(err)
			}
			db, err := storage.Open(dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			secretsDir := filepath.Join(tmp, "secrets")
			writeSecrets(t, secretsDir)

			opts := initOpts(dataDir, dbPath, secretsDir)
			clear(&opts)
			if err := clusteroffline.InitFromExisting(opts); err == nil {
				t.Fatalf("InitFromExisting accepted empty %s", field)
			}
		})
	}
}
