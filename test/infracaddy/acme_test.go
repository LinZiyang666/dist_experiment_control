//go:build infracaddy_integration && linux && amd64

package infracaddy_test

// The local CA speaks ACME v2 to the stock process and validates its actual
// HTTP-01 response. It issues a short-lived A and a long-lived B. Production
// storage migration is performed exclusively by infrahost.ACMEStorage.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/infraharness"
	"golang.org/x/crypto/acme"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §5.3 / T67
// Exercise the test supply independently so a broken ACME server cannot hide
// behind an absent stock archive or return invented certificate receipts.
func TestCaddyACMEFixtureIssuesOnlyValidatedCSRs(t *testing.T) {
	ca := newFixtureCA(t)
	defer ca.server.Close()
	accountKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &acme.Client{Key: accountKey, DirectoryURL: ca.server.URL + "/directory"}
	if _, err := client.Register(t.Context(), &acme.Account{}, acme.AcceptTOS); err != nil {
		t.Fatal(err)
	}
	var responseMu sync.Mutex
	var challengeResponse string
	challengeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		responseMu.Lock()
		defer responseMu.Unlock()
		_, _ = io.WriteString(w, challengeResponse)
	}))
	defer challengeServer.Close()
	ca.mu.Lock()
	ca.challengePort = challengeServer.Listener.Addr().(*net.TCPAddr).Port
	ca.mu.Unlock()
	for serial := int64(1); serial <= 2; serial++ {
		order, err := client.AuthorizeOrder(t.Context(), []acme.AuthzID{{Type: "dns", Value: fixtureHost}})
		if err != nil || len(order.AuthzURLs) != 1 {
			t.Fatal("ACME supply did not create a real authorization")
		}
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{DNSNames: []string{fixtureHost}}, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := client.CreateOrderCert(t.Context(), order.FinalizeURL, csr, false); err == nil || ca.issueCount() != serial-1 {
			t.Fatal("test CA issued before the actual challenge was validated")
		}
		authorization, err := client.GetAuthorization(t.Context(), order.AuthzURLs[0])
		if err != nil || len(authorization.Challenges) != 1 {
			t.Fatal("test CA did not offer HTTP-01")
		}
		challenge := authorization.Challenges[0]
		keyAuthorization, err := client.HTTP01ChallengeResponse(challenge.Token)
		if err != nil {
			t.Fatal(err)
		}
		responseMu.Lock()
		challengeResponse = "wrong-key-authorization"
		responseMu.Unlock()
		if _, err := client.Accept(t.Context(), challenge); err == nil {
			t.Fatal("test CA accepted an incorrect actual HTTP-01 response")
		}
		responseMu.Lock()
		challengeResponse = keyAuthorization
		responseMu.Unlock()
		if _, err := client.Accept(t.Context(), challenge); err != nil {
			t.Fatal(err)
		}
		if _, err := client.WaitAuthorization(t.Context(), order.AuthzURLs[0]); err != nil {
			t.Fatal(err)
		}
		chain, _, err := client.CreateOrderCert(t.Context(), order.FinalizeURL, csr, false)
		if err != nil || len(chain) != 1 {
			t.Fatal("validated ACME CSR did not produce an actual certificate")
		}
		cert, err := x509.ParseCertificate(chain[0])
		if err != nil || cert.SerialNumber.Int64() != serial || cert.VerifyHostname(fixtureHost) != nil ||
			cert.CheckSignatureFrom(ca.root) != nil || !cert.PublicKey.(*ecdsa.PublicKey).Equal(&key.PublicKey) {
			t.Fatal("test CA certificate does not bind the validated CSR and SNI")
		}
	}
}

// origin: hot-update S9 plan §5.3 / T67 / M11
func TestCaddyCurrentStorageRollback(t *testing.T) {
	fixture := newCaddyFixture(t)
	defer fixture.close(t)
	ca := newFixtureCA(t)
	defer ca.server.Close()
	fixture.leaf = ca.root
	current := filepath.Join(fixture.root, "current-acme")
	if err := os.Mkdir(current, 0700); err != nil {
		t.Fatal(err)
	}
	challenge := reserveFixturePort(t)
	ca.mu.Lock()
	ca.challengePort = challenge
	ca.mu.Unlock()
	configure := func(s *stockCaddy, root map[string]any) {
		root["storage"] = map[string]any{"module": "file_system", "root": current}
		apps := root["apps"].(map[string]any)
		// Renewal polling and alternate challenge ports are fixture-only stock
		// settings. They do not enlarge P08's production configuration surface.
		apps["tls"] = map[string]any{
			"certificates": map[string]any{"automate": []string{fixtureHost}},
			"automation": map[string]any{"renew_interval": "100ms", "policies": []any{map[string]any{
				"subjects": []string{fixtureHost}, "issuers": []any{map[string]any{
					"module": "acme", "ca": ca.server.URL + "/directory",
					"challenges": map[string]any{"http": map[string]any{"alternate_port": challenge},
						"tls-alpn": map[string]any{"disabled": true}},
				}},
			}}},
		}
		httpApp := apps["http"].(map[string]any)
		httpApp["http_port"], httpApp["https_port"] = challenge, s.port
		configServer(root)["automatic_https"] = map[string]any{"disable_redirects": true}
	}
	old := fixture.startConfig(t, "2.7.6", 0, "revision-A", configure, func(s *stockCaddy) {
		waitFixture(t, 30*time.Second, func() bool { return storedCertificateSerial(t, current) == 1 })
	})
	if servedSerial(t, old) != 1 {
		t.Fatal("initial stock writer did not serve the real ACME A certificate")
	}
	migrationDir := filepath.Join(fixture.root, "storage-owner")
	if err := os.Mkdir(migrationDir, 0700); err != nil {
		t.Fatal(err)
	}
	rootFD, err := os.Open(migrationDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rootFD.Close() }()
	currentFD, err := os.Open(current)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = currentFD.Close() }()
	storage, err := infrahost.NewACMEStorage(infrahost.ACMEConfig{Root: rootFD, Initial: currentFD,
		UID: os.Geteuid(), GID: os.Getegid(), Exit: func(expected c.Process, _ c.Deadline, _ c.Stop) (c.Census, error) {
			if expected != old.identity {
				return c.Census{}, c.ErrIncomplete
			}
			exists, err := infraharness.ProcessExists(old.process.Identity())
			if err != nil || exists || !old.retired {
				return c.Census{}, c.ErrIncomplete
			}
			// This is the private fixture writer scope, not a host census. Bind
			// its exit proof to the independently waited exact incarnation and
			// initial tree identity; an unrelated writer's exit cannot reuse it.
			var stat unix.Stat_t
			if err := unix.Fstat(int(currentFD.Fd()), &stat); err != nil {
				return c.Census{}, c.ErrIncomplete
			}
			proof, err := json.Marshal(struct {
				Expected              c.Process
				Writer                infraharness.Identity
				TreeDevice, TreeInode uint64
			}{expected, old.process.Identity(), uint64(stat.Dev), stat.Ino})
			if err != nil {
				return c.Census{}, c.ErrIncomplete
			}
			return c.Census{Complete: true, Generation: 1, Digest: sha256.Sum256(proof)}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = storage.Close() }()
	request := storageRequest(old.identity)
	request.Op = c.FenceStart
	if _, err := infrahost.FenceACME(storage, request, fixtureDeadline(t, 5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	warm, err := infrahost.WarmACME(storage, fixtureDeadline(t, 5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = warm.Directory.Close() }()
	if storedCertificateSerial(t, directoryPath(t, warm.Directory)) != 1 {
		t.Fatal("warm snapshot did not capture A before the late renewal")
	}
	servedCert, servedKey := certificateFiles(t, directoryPath(t, warm.Directory))
	beforeCert, beforeKey := fixture.cert, fixture.key
	fixture.cert, fixture.key = servedCert, servedKey
	warmEntry := fixture.start(t, "2.7.6", 0, "revision-A")
	fixture.cert, fixture.key = beforeCert, beforeKey
	if servedSerial(t, warmEntry) != 1 {
		t.Fatal("actual warm invocation did not serve the read-only ACME A snapshot")
	}
	request.Op = c.FinalizeStorage
	if receipt, err := storage.Do(request, fixtureDeadline(t, 5*time.Second), nil); err == nil || receipt.Applied {
		t.Fatal("storage finalized while the exact old ACME writer was alive")
	}
	// This handshake comes from actual stock renewal and an independently read
	// current tree, rather than a test-side copy or a regenerated certificate.
	waitFixture(t, 30*time.Second, func() bool {
		return storedCertificateSerial(t, current) == 2 && servedSerial(t, old) == 2
	})
	if servedSerial(t, old) != 2 || storedCertificateSerial(t, directoryPath(t, warm.Directory)) != 1 {
		t.Fatal("old writer's late B update did not diverge from the warm snapshot")
	}
	old.requireDrained(t)
	old.retire(t)
	before := storageTreeDigest(t, current)
	receipt, err := storage.Do(request, fixtureDeadline(t, 10*time.Second), nil)
	if err != nil || !receipt.Applied || receipt.Journal == (c.Digest{}) {
		t.Fatalf("final current-state migration failed: %v", err)
	}
	// Finalized is still fenced. A stale or incomplete release cannot authorize
	// an automatic restart, even after storage selection has changed.
	// A live claim must actually wait on the persisted barrier. Pre-cancelling
	// Stop would fail before reading the fence and let a broken fence look green.
	if writer, err := infrahost.ClaimACMEWriter(storage, request.Tx, request.Domain, receipt.Revision,
		fixtureDeadline(t, upgradebudget.RequestLatency/2), nil); err == nil {
		_ = writer.Directory.Close()
		_ = writer.Lease.Close()
		t.Fatal("finalized storage prematurely released the startup fence")
	}
	revision, err := infrahost.ReleaseACMEStart(storage, request.Tx, request.Domain, receipt.Revision,
		fixtureDeadline(t, 5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := infrahost.ClaimACMEWriter(storage, request.Tx, request.Domain, revision,
		fixtureDeadline(t, 5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Directory.Close(); _ = writer.Lease.Close() }()
	current = directoryPath(t, writer.Directory)
	if storedCertificateSerial(t, current) != 2 || storageTreeDigest(t, current) != before {
		t.Fatal("final selector retained the old snapshot or lost current account metadata")
	}
	issuedBefore := ca.issueCount()
	for _, version := range []string{"2.7.5", "2.7.6"} {
		fixture.launchLease = writer.Lease
		var leaseStat unix.Stat_t
		if err := unix.Fstat(int(writer.Lease.Fd()), &leaseStat); err != nil {
			t.Fatal(err)
		}
		main := fixture.startConfig(t, version, old.port, "revision-A", configure, nil)
		fixture.launchLease = nil
		var childStat unix.Stat_t
		if err := unix.Stat(fmt.Sprintf("/proc/%d/fd/3", main.identity.PID), &childStat); err != nil ||
			childStat.Dev != leaseStat.Dev || childStat.Ino != leaseStat.Ino {
			t.Fatal("stock writer did not inherit the actual ACME lifetime lease")
		}
		_ = writer.Lease.Close()
		if second, err := infrahost.ClaimACMEWriter(storage, request.Tx, request.Domain, revision,
			fixtureDeadline(t, time.Second), nil); err == nil {
			_ = second.Directory.Close()
			_ = second.Lease.Close()
			t.Fatal("a second renewal writer acquired the live stock invocation's lease")
		} else if err.Error() != "infra_busy" {
			t.Fatal("writer exclusion failed for an unrelated admission error")
		}
		if servedSerial(t, main) != 2 || storedCertificateSerial(t, current) != 2 || ca.issueCount() != issuedBefore {
			t.Fatal("rollback failed to read current B or reissued to conceal missing storage")
		}
		main.requireDrained(t)
		main.retire(t)
		fixture.requireRetired(t, main)
		_ = writer.Directory.Close()
		writer, err = infrahost.ClaimACMEWriter(storage, request.Tx, request.Domain, revision,
			fixtureDeadline(t, 5*time.Second), nil)
		if err != nil {
			t.Fatal("exited stock writer retained an ACME lease")
		}
	}
	warmEntry.requireDrained(t)
	warmEntry.retire(t)
	fixture.requireRetired(t, warmEntry)
	if err := infrahost.RetireACMEWarm(storage, warm); err != nil {
		t.Fatal(err)
	}
	fixture.requireRetired(t, old)
	t.Logf("current storage receipt: old_pid=%d warm_pid=%d current_tree=%x final_journal=%x issued=%d rollback_serial=2",
		old.identity.PID, warmEntry.identity.PID, before, receipt.Journal, ca.issueCount())
}

func storageRequest(process c.Process) c.HostRequest {
	return c.HostRequest{Version: 1, Op: c.FinalizeStorage, Component: c.Caddy, Tx: c.ID{1}, Domain: c.ID{2},
		Step: 1, Sequence: 1, PolicyRevision: 1, KeyID: "fixture-policy", HostBootID: process.BootID,
		Nonce: c.ID{3}, Expected: process, ExpectedJournal: c.Digest{4}, BeforeCensus: c.Digest{5},
		Payload: c.Digest{6}, RemainingNS: uint64(10 * time.Second)}
}

func reserveFixturePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func directoryPath(t *testing.T, directory *os.File) string {
	t.Helper()
	path, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()))
	if err != nil || !filepath.IsAbs(path) {
		t.Fatal("selected ACME directory has no independent filesystem identity")
	}
	return path
}

func waitFixture(t *testing.T, duration time.Duration, ready func() bool) {
	t.Helper()
	limit := time.NewTimer(duration)
	defer limit.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-limit.C:
			t.Fatal("independent fixture receipt did not arrive within its budget")
		case <-tick.C:
		}
	}
}

func servedSerial(t *testing.T, s *stockCaddy) int64 {
	t.Helper()
	transport := &http.Transport{TLSClientConfig: s.tlsConfig(), DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: upgradebudget.RequestLatency}
	response, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/manifest", s.port))
	if err != nil {
		t.Fatal("current ACME certificate failed verified TLS")
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.TLS == nil || len(response.TLS.PeerCertificates) == 0 {
		t.Fatal("current certificate observation lacks a real TLS peer")
	}
	return response.TLS.PeerCertificates[0].SerialNumber.Int64()
}

func storedCertificateSerial(t *testing.T, directory string) int64 {
	t.Helper()
	var serial int64
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("unexpected link in ACME tree")
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".crt") {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			block, _ := pem.Decode(data)
			if block == nil {
				return errors.New("ACME certificate is not PEM")
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil || cert.VerifyHostname(fixtureHost) != nil {
				return errors.New("ACME certificate does not bind the original SNI")
			}
			if serial != 0 {
				return errors.New("ambiguous current certificate set")
			}
			serial = cert.SerialNumber.Int64()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

func certificateFiles(t *testing.T, directory string) (string, string) {
	t.Helper()
	var certificate, key string
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".crt") {
			if certificate != "" {
				return errors.New("ambiguous served certificate set")
			}
			certificate, key = path, strings.TrimSuffix(path, ".crt")+".key"
		}
		return nil
	})
	if err != nil || certificate == "" {
		t.Fatal("warm ACME snapshot has no unique served certificate/key reference")
	}
	if info, err := os.Lstat(key); err != nil || !info.Mode().IsRegular() {
		t.Fatal("warm ACME snapshot is missing the original served key")
	}
	return certificate, key
}

func storageTreeDigest(t *testing.T, directory string) c.Digest {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if rel == "locks" && entry.IsDir() {
			return filepath.SkipDir // Advisory locks are deliberately absent in migrated trees.
		}
		if !entry.IsDir() {
			paths = append(paths, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, rel := range paths {
		data, err := os.ReadFile(filepath.Join(directory, rel))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintf(hash, "%d:%s:%d:", len(rel), rel, len(data))
		_, _ = hash.Write(data)
	}
	if len(paths) < 3 {
		t.Fatal("current ACME tree lacks certificate, key and account/metadata files")
	}
	return c.Digest(hash.Sum(nil))
}

type fixtureOrder struct {
	id, token, status string
	authValid         bool
	certificate       []byte
}

type fixtureCA struct {
	server        *httptest.Server
	root          *x509.Certificate
	key           *ecdsa.PrivateKey
	mu            sync.Mutex
	orders        map[string]*fixtureOrder
	issued        int64
	nonce         uint64
	thumbprint    string
	challengePort int
}

func newFixtureCA(t *testing.T) *fixtureCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1000), Subject: pkix.Name{CommonName: "private infra test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	ca := &fixtureCA{root: root, key: key, orders: make(map[string]*fixtureOrder)}
	ca.server = httptest.NewServer(http.HandlerFunc(ca.serve))
	return ca
}

func (ca *fixtureCA) issueCount() int64 {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return ca.issued
}

func (ca *fixtureCA) serve(w http.ResponseWriter, r *http.Request) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.nonce++
	w.Header().Set("Replay-Nonce", base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(ca.nonce, 10))))
	w.Header().Set("Content-Type", "application/json")
	base := ca.server.URL
	if r.URL.Path == "/directory" {
		ca.json(w, http.StatusOK, map[string]any{"newNonce": base + "/nonce", "newAccount": base + "/account",
			"newOrder": base + "/order", "revokeCert": base + "/revoke", "keyChange": base + "/key-change"})
		return
	}
	if r.URL.Path == "/nonce" {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "ACME POST required", http.StatusMethodNotAllowed)
		return
	}
	var jws struct{ Protected, Payload, Signature string }
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	if decoder.Decode(&jws) != nil || jws.Signature == "" {
		http.Error(w, "invalid ACME envelope", http.StatusBadRequest)
		return
	}
	payload, err := base64.RawURLEncoding.DecodeString(jws.Payload)
	if err != nil {
		http.Error(w, "invalid ACME payload", http.StatusBadRequest)
		return
	}
	if r.URL.Path == "/account" {
		protected, err := base64.RawURLEncoding.DecodeString(jws.Protected)
		var header struct {
			JWK map[string]string `json:"jwk"`
		}
		if err != nil || json.Unmarshal(protected, &header) != nil || header.JWK["kty"] == "" {
			http.Error(w, "missing ACME account key", http.StatusBadRequest)
			return
		}
		canonical, _ := json.Marshal(header.JWK)
		thumb := sha256.Sum256(canonical)
		ca.thumbprint = base64.RawURLEncoding.EncodeToString(thumb[:])
		w.Header().Set("Location", base+"/account/1")
		ca.json(w, http.StatusCreated, map[string]any{"status": "valid", "orders": base + "/orders"})
		return
	}
	if r.URL.Path == "/account/1" {
		ca.json(w, http.StatusOK, map[string]any{"status": "valid", "orders": base + "/orders"})
		return
	}
	if r.URL.Path == "/order" {
		var request struct {
			Identifiers []struct{ Type, Value string }
		}
		if json.Unmarshal(payload, &request) != nil || len(request.Identifiers) != 1 ||
			request.Identifiers[0].Type != "dns" || request.Identifiers[0].Value != fixtureHost {
			http.Error(w, "unsupported ACME identifier", http.StatusBadRequest)
			return
		}
		id := strconv.Itoa(len(ca.orders) + 1)
		order := &fixtureOrder{id: id, token: "infra-challenge-" + id, status: "pending"}
		ca.orders[id] = order
		w.Header().Set("Location", base+"/order/"+id)
		ca.json(w, http.StatusCreated, ca.order(order))
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 2 || ca.orders[parts[1]] == nil {
		http.Error(w, "unknown ACME resource", http.StatusNotFound)
		return
	}
	order := ca.orders[parts[1]]
	switch parts[0] {
	case "auth":
		status := "pending"
		if order.authValid {
			status = "valid"
		}
		ca.json(w, http.StatusOK, map[string]any{"status": status, "identifier": map[string]any{"type": "dns", "value": fixtureHost},
			"challenges": []any{map[string]any{"type": "http-01", "url": base + "/challenge/" + order.id, "token": order.token, "status": status}}})
	case "challenge":
		transport := &http.Transport{DisableKeepAlives: true}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: upgradebudget.RequestLatency}
		url := fmt.Sprintf("http://127.0.0.1:%d/.well-known/acme-challenge/%s", ca.challengePort, order.token)
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "invalid challenge request", http.StatusInternalServerError)
			return
		}
		req.Host = fixtureHost
		response, err := client.Do(req)
		if err != nil {
			http.Error(w, "actual HTTP-01 listener unavailable", http.StatusBadRequest)
			return
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		_ = response.Body.Close()
		if readErr != nil || response.StatusCode != http.StatusOK || string(body) != order.token+"."+ca.thumbprint {
			http.Error(w, "actual HTTP-01 key authorization differs", http.StatusBadRequest)
			return
		}
		order.authValid, order.status = true, "ready"
		ca.json(w, http.StatusOK, map[string]any{"type": "http-01", "status": "valid", "token": order.token, "url": base + "/challenge/" + order.id})
	case "finalize":
		var request struct {
			CSR string `json:"csr"`
		}
		if !order.authValid || json.Unmarshal(payload, &request) != nil {
			http.Error(w, "unvalidated ACME order", http.StatusBadRequest)
			return
		}
		der, err := base64.RawURLEncoding.DecodeString(request.CSR)
		csr, parseErr := x509.ParseCertificateRequest(der)
		if err != nil || parseErr != nil || csr.CheckSignature() != nil || len(csr.DNSNames) != 1 || csr.DNSNames[0] != fixtureHost {
			http.Error(w, "invalid ACME CSR", http.StatusBadRequest)
			return
		}
		ca.issued++
		lifetime := time.Hour
		if ca.issued == 1 {
			lifetime = 30 * time.Second
		}
		certificate := &x509.Certificate{SerialNumber: big.NewInt(ca.issued), DNSNames: csr.DNSNames,
			NotBefore: time.Now().Add(-time.Second), NotAfter: time.Now().Add(lifetime),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err = x509.CreateCertificate(rand.Reader, certificate, ca.root, csr.PublicKey, ca.key)
		if err != nil {
			http.Error(w, "test CA issuance failed", http.StatusInternalServerError)
			return
		}
		order.certificate = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		order.status = "valid"
		ca.json(w, http.StatusOK, ca.order(order))
	case "order":
		ca.json(w, http.StatusOK, ca.order(order))
	case "cert":
		if order.status != "valid" {
			http.Error(w, "ACME certificate not ready", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/pem-certificate-chain")
		_, _ = w.Write(order.certificate)
	default:
		http.Error(w, "unknown ACME operation", http.StatusNotFound)
	}
}

func (ca *fixtureCA) order(order *fixtureOrder) map[string]any {
	result := map[string]any{"status": order.status,
		"identifiers":    []any{map[string]any{"type": "dns", "value": fixtureHost}},
		"authorizations": []string{ca.server.URL + "/auth/" + order.id}, "finalize": ca.server.URL + "/finalize/" + order.id}
	if order.status == "valid" {
		result["certificate"] = ca.server.URL + "/cert/" + order.id
	}
	return result
}

func (*fixtureCA) json(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
