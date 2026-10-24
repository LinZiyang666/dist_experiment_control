package agentupgrade

import (
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func ReadLegacyMarker(path string) (*binaryupgrade.Marker, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var envelope struct {
		binaryupgrade.Marker
		Version uint32          `json:"version"`
		Schema  json.RawMessage `json:"schema_version"`
		Entries json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}

	// origin: hot-update S8 plan §3.3 / M11. The legacy projection of a v2
	// journal cannot authorize old boot, report, rollback or unlink writers.
	if envelope.Version > 1 || len(envelope.Schema) != 0 || len(envelope.Entries) != 0 {
		return nil, &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "transaction journal requires its sole v2 writer"}
	}
	return &envelope.Marker, nil
}

func WriteLegacyMarker(store binaryupgrade.Store, path string, m *binaryupgrade.Marker) error {
	if _, err := ReadLegacyMarker(path); err != nil {
		return err
	}
	return store.WriteMarker(path, m)
}

func ExecuteLegacyRollback(store binaryupgrade.Store, exePath string, m *binaryupgrade.Marker, reason string, logger *slog.Logger, reexec bool, execFn func(string) error) {
	current, err := ReadLegacyMarker(binaryupgrade.MarkerPath(exePath))
	if err != nil || current == nil || !SameLegacyMarker(m, current) {
		return
	}
	store.ExecuteRollback(exePath, m, reason, logger, reexec, execFn)
}

func CheckLegacyBoot(store binaryupgrade.Store, exePath string, logger *slog.Logger, now time.Time, img *agentboot.Image, redispatch func(*handoff.Prepared, *agentboot.Image, func(string) error) func(string) error, execFn func(string) error) string {
	if _, err := ReadLegacyMarker(binaryupgrade.MarkerPath(exePath)); err != nil {
		return ""
	}
	return store.CheckBoot(exePath, logger, now, img, redispatch, execFn)
}
