//go:build linux && amd64

// Real dirfd/openat metadata and nkey seeds in private temporary directories.
// The private owner argument substitutes the test UID for root, without sudo.
// Production root ownership and multiple real UIDs are independently P31 scope.
package infrapriv

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/nats-io/nkeys"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §3.3 / §6.3 / P03 credential-loader inventory
func TestInfraCredentialLoaderPolicy(t *testing.T) {
	directory := t.TempDir()
	dir := openTestDirectory(t, directory)
	defer func() { _ = dir.Close() }() // read-only fixture dirfd; nothing to flush
	uid, gid := uint32(os.Geteuid()), uint32(os.Getegid())
	rows := CredentialDescriptors()
	want := []CredentialDescriptor{{SYSRead, "infra-sys-read.nk", 0, 0640, "nkey-user", "sys-read-adapter"},
		{SYSStep, "infra-sys-step.nk", 0, 0640, "nkey-user", "sys-step-adapter"},
		{LocalInfra, "infra-local.nk", 0, 0640, "nkey-user", "local-infra-authority"},
		{LocalLaunch, "infra-launch.nk", 0, 0640, "nkey-user", "fixed-launcher"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatal("credential reader/metadata inventory changed")
	}
	t.Run("read-metadata-must-stay-stable", func(t *testing.T) {
		before := unix.Stat_t{Dev: 1, Ino: 2, Mode: unix.S_IFREG | 0640, Uid: uid, Gid: gid, Nlink: 1, Size: 58}
		before.Mtim.Sec, before.Ctim.Sec = 3, 4
		after := before
		after.Atim.Sec++
		if !unchangedMetadata(before, after) {
			t.Fatal("read's atime update invalidated its own credential")
		}
		for _, field := range []string{"Dev", "Ino", "Mode", "Uid", "Gid", "Nlink", "Size", "Mtim", "Ctim"} {
			after := before
			value := reflect.ValueOf(&after).Elem().FieldByName(field)
			if value.Kind() == reflect.Struct {
				value = value.FieldByName("Sec")
			}
			if value.Kind() == reflect.Int64 {
				value.SetInt(value.Int() + 1)
			} else {
				value.SetUint(value.Uint() + 1)
			}
			if unchangedMetadata(before, after) {
				t.Fatalf("credential changed during read: %s", field)
			}
		}
	})
	if _, err := credentialRow(255); !errors.Is(err, ErrCredential) {
		t.Fatal("unknown credential reader admitted")
	}
	if err := bootstrapCredentialsAt(dir, uid, gid); err != nil {
		t.Fatal(err)
	}
	before := make([][]byte, len(rows))
	for i, row := range rows {
		data, err := readOwnedFile(dir, row.Name, uid, gid, row.Mode, 58)
		if err != nil {
			t.Fatal(err)
		}
		before[i] = bytes.Clone(data)
		key, err := decodeCredential(data)
		if err != nil {
			t.Fatal(err)
		}
		exerciseCredentialCanary(t, key, before[i])
		_ = key.Close()
		for j := 0; j < i; j++ {
			if bytes.Equal(before[i], before[j]) {
				t.Fatal("credential scopes share a signing seed")
			}
		}
	}
	if err := bootstrapCredentialsAt(dir, uid, gid); err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		data, err := os.ReadFile(filepath.Join(directory, row.Name))
		if err != nil || !bytes.Equal(data, before[i]) {
			t.Fatal("bootstrap regenerated an existing seed")
		}
	}
	if err := os.Remove(filepath.Join(directory, rows[0].Name)); err != nil {
		t.Fatal(err)
	}
	if err := bootstrapCredentialsAt(dir, uid, gid); !errors.Is(err, ErrCredential) {
		t.Fatalf("partial set regenerated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, rows[0].Name)); !os.IsNotExist(err) {
		t.Fatal("partial bootstrap wrote missing seed")
	}
	for _, row := range rows[1:] {
		if err := os.Remove(filepath.Join(directory, row.Name)); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("metadata-before-secret-read", func(t *testing.T) {
		name := rows[0].Name
		path := filepath.Join(directory, name)
		for _, mode := range []os.FileMode{0600, 0660, 0644, 04640} {
			if err := os.WriteFile(path, before[0], 0640); err != nil {
				t.Fatal(err)
			}
			if err := unix.Chmod(path, uint32(mode)); err != nil {
				t.Fatal(err)
			}
			if _, err := readOwnedFile(dir, name, uid, gid, 0640, 58); err == nil {
				t.Fatalf("bad mode accepted: %o", mode)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(path, before[0], 0640); err != nil {
			t.Fatal(err)
		}
		if _, err := readOwnedFile(dir, name, uid+1, gid, 0640, 58); err == nil {
			t.Fatal("wrong owner accepted")
		}
		if _, err := readOwnedFile(dir, name, uid, gid+1, 0640, 58); err == nil {
			t.Fatal("extra credential reader group accepted")
		}
		if err := os.Link(path, filepath.Join(directory, "alias")); err != nil {
			t.Fatal(err)
		}
		if _, err := readOwnedFile(dir, name, uid, gid, 0640, 58); err == nil {
			t.Fatal("hard-linked secret accepted")
		}
		if err := os.Remove(filepath.Join(directory, "alias")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path, filepath.Join(directory, "target")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("target", path); err != nil {
			t.Fatal(err)
		}
		if _, err := readOwnedFile(dir, name, uid, gid, 0640, 58); err == nil {
			t.Fatal("symlink secret accepted")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := unix.Mkfifo(path, 0640); err != nil {
			t.Fatal(err)
		}
		if _, err := readOwnedFile(dir, name, uid, gid, 0640, 58); err == nil {
			t.Fatal("FIFO secret accepted")
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		for _, data := range [][]byte{nil, bytes.Repeat([]byte{'S'}, 59)} {
			if err := os.WriteFile(path, data, 0640); err != nil {
				t.Fatal(err)
			}
			if _, err := readOwnedFile(dir, name, uid, gid, 0640, 58); err == nil {
				t.Fatal("bad secret size accepted")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("trusted-ancestor-and-canonical-path", func(t *testing.T) {
		for _, path := range []string{directory, directory + "/../", "relative", "/tmp", "/proc/self"} {
			if opened, err := openTrustedDirectory(path); err == nil {
				_ = opened.Close()
				t.Fatalf("untrusted ancestor admitted: %s", path)
			}
		}
		if os.Geteuid() != 0 {
			if err := BootstrapCredentials(directory, gid); !errors.Is(err, ErrPermission) {
				t.Fatal("nonroot bootstrap admitted")
			}
			if _, err := LoadCredential(directory, SYSRead, gid); err == nil {
				t.Fatal("public credential loader trusted caller-owned root")
			}
			if _, err := LoadPolicy(directory); err == nil {
				t.Fatal("public policy loader trusted caller-owned root")
			}
		}
	})
	t.Run("seed-format-closed", func(t *testing.T) {
		account, err := nkeys.EncodeSeed(nkeys.PrefixByteAccount, bytes.Repeat([]byte{19}, ed25519.SeedSize))
		if err != nil {
			t.Fatal(err)
		}
		for _, data := range [][]byte{account, append(bytes.Clone(before[0]), '\n'), []byte("seed-canary-invalid"), []byte("-----BEGIN NATS USER JWT-----")} {
			if key, err := decodeCredential(data); err == nil {
				_ = key.Close()
				t.Fatal("wrong credential format/type accepted")
			}
			if !bytes.Equal(data, make([]byte, len(data))) {
				t.Fatal("invalid input seed was not wiped")
			}
		}
	})
}

func exerciseCredentialCanary(t *testing.T, key *Credential, encoded []byte) {
	t.Helper()
	raw := bytes.Clone(key.secret.seed)
	message := []byte("fixed launch request")
	signature, err := key.Sign(message)
	if err != nil || !ed25519.Verify(key.PublicKey(), message, signature) {
		t.Fatal("loaded nkey did not sign expected raw public key")
	}
	if key.NKey() == "" {
		t.Fatal("SYS adapter lacks public NATS user identity")
	}
	key.PublicKey()[0]++
	if !ed25519.Verify(key.PublicKey(), message, signature) {
		t.Fatal("public key getter aliases stored identity")
	}
	// A credential reaches an encoder as a field of some record (a log entry, a
	// status reply), so it is marshalled in that shape. encoding/json uses the
	// same *Credential encoder for a field as for a top-level value, so a future
	// exported field or Marshal* method still surfaces here.
	jsonBytes, err := json.Marshal(struct{ Key *Credential }{key})
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{fmt.Sprintf("%v", key), fmt.Sprintf("%+v", key), fmt.Sprintf("%#v", key), fmt.Sprintf("%x", key),
		fmt.Sprintf("%#v", *key), fmt.Sprintf("%+v", struct{ Key Credential }{*key}), fmt.Errorf("loader: %v", key).Error(), string(jsonBytes)} {
		for _, secret := range [][]byte{raw, encoded} {
			// The last spelling is the %v decimal byte list ("[12 34 ...]"), the
			// form a slice takes in a formatted struct; string(secret) is listed first.
			for _, spelling := range []string{string(secret), hex.EncodeToString(secret), base64.StdEncoding.EncodeToString(secret), fmt.Sprintf("%v", secret)} {
				if bytes.Contains([]byte(output), []byte(spelling)) {
					t.Fatal("credential canary leaked through diagnostic/JSON formatting")
				}
			}
		}
	}
	var wg sync.WaitGroup
	for round := 0; round < 8; round++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = key.Sign(message); _ = key.PublicKey(); _ = key.NKey() }()
	}
	retained := key.secret.seed
	if err := key.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !bytes.Equal(retained, make([]byte, len(retained))) {
		t.Fatal("closed credential retained raw seed")
	}
	if _, err := key.Sign(message); !errors.Is(err, ErrCredential) {
		t.Fatal("closed credential still signs")
	}
	if err := key.Close(); err != nil {
		t.Fatal(err)
	}
}

func openTestDirectory(t *testing.T, path string) *os.File {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	return os.NewFile(uintptr(fd), "test-private-directory")
}
