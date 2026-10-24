package upgradecli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradelock"
)

// origin: S8 internal review remaining-orchestration F5 (R5)
func TestRetrofitDeclarationRegistersCanonicalJournal(t *testing.T) {
	dir := t.TempDir()
	canonical := filepath.Join(dir, "tether")
	if err := os.WriteFile(canonical, []byte("canonical entry"), 0755); err != nil {
		t.Fatal(err)
	}
	role := AgentBootRole(dir, "lab", "node", uint32(os.Geteuid()))
	if err := os.MkdirAll(role.ControlDir, 0700); err != nil {
		t.Fatal(err)
	}
	reg := upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "installation", Aliases: []string{}, Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "agent-lab-node"}}}
	body, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(role.ControlDir, "installation.json"), body, 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		plan, err := RegisteredJournal(role)
		if err != nil || !plan.Registered {
			t.Fatalf("retrofit declaration never becomes a registered installation: %+v %v", plan, err)
		}
		if plan.Key.DomainID == "" || plan.Journal != role.Journal(plan.Key, reg) {
			t.Fatalf("registration changed canonical journal: %+v", plan)
		}
		if info, err := os.Stat(filepath.Join(role.ControlDir, "artifacts")); err != nil || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
			t.Fatalf("publication directory is absent or uncontrolled: %v", err)
		}
		if _, err := os.Stat(filepath.Dir(plan.Journal)); err != nil {
			t.Fatalf("daemon journal directory missing: %v", err)
		}
	}
}

// origin: S8 internal review remaining-orchestration F5 (R5) / hot-update S8 plan §5.1
func TestRetrofitRejectsAmbiguousDeclarations(t *testing.T) {
	for _, name := range []string{"valid", "wrong_key", "wrong_control", "duplicate", "unknown", "trailing", "writable", "symlink", "hardlink", "artifact_alias"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			canonical := filepath.Join(dir, "tether")
			if err := os.WriteFile(canonical, []byte("entry"), 0755); err != nil {
				t.Fatal(err)
			}
			role := BrokerBootRole(dir, filepath.Join(dir, "broker.yaml"), "", uint32(os.Geteuid()))
			if err := os.MkdirAll(role.ControlDir, 0700); err != nil {
				t.Fatal(err)
			}
			reg := upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "installation", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "broker"}}}
			if name == "wrong_key" {
				reg.Participants[0].Key.UID++
			}
			if name == "wrong_control" {
				reg.ControlDir = dir
			}
			body, _ := json.Marshal(reg)
			switch name {
			case "duplicate":
				body = []byte(strings.Replace(string(body), `"deployment_id":"installation"`, `"deployment_id":"installation","deployment_id":"installation"`, 1))
			case "unknown":
				body = append([]byte(`{"extra":true,`), body[1:]...)
			case "trailing":
				body = append(body, []byte(` {}`)...)
			}
			path := filepath.Join(role.ControlDir, "installation.json")
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "writable":
				if err := os.Chmod(path, 0666); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, path+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".real", path); err != nil {
					t.Fatal(err)
				}
			case "artifact_alias":
				if err := os.Symlink(t.TempDir(), filepath.Join(role.ControlDir, "artifacts")); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := RegisteredJournal(role)
			if name == "valid" {
				if err != nil || !plan.Registered {
					t.Fatalf("valid broker declaration refused: %+v %v", plan, err)
				}
				return
			}
			if err == nil || plan.Registered {
				t.Fatalf("ambiguous declaration registered: %+v %v", plan, err)
			}
		})
	}
}
