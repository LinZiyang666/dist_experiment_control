//go:build linux && infrabusiness_integration

package infrabusiness_test

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/infraauth"
	i "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type authReceipt struct {
	Step uint32
	evidenceScope
	Scenario                                string
	Participant                             i.ID
	Runtime                                 i.Process
	Case, ServerID, Queue, Evidence         string
	Generation, AuthCID, BusinessCID        uint64
	AuthSUBNS, AuthPONGNS, OrdinaryAuthNS   int64
	PrefixReadNS, RepairNS, BusinessSUBNS   int64
	MemberRPCNS, AgentRPCNS                 int64
	MemberRequest, AgentRequest, PrefixHash string
	Admitted, LocalQueue                    bool
}

// origin: hot-update S9 plan §4.3–§4.5 / T70
func TestInfraAuthReplayIntegration(t *testing.T) {
	t.Run("normal_auth_queue", exerciseNormalAuth)
	t.Run("oracle_controls", func(t *testing.T) {
		r := authControls()
		if err := checkAuth(r); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"business_before_auth", "peer_queue", "repair_before_read", "lost_ack_without_effect", "no_reply_cut", "missing_case", "duplicate_effect"} {
			t.Run(name, func(t *testing.T) {
				m := copyReceipt(r)
				switch name {
				case "business_before_auth":
					m.Auth[0].BusinessSUBNS = m.Auth[0].AuthSUBNS
				case "peer_queue":
					m.Auth[0].LocalQueue = false
				case "repair_before_read":
					m.Auth[0].RepairNS = m.Auth[0].PrefixReadNS - 1
				case "lost_ack_without_effect":
					m.Faults[0].EffectEvidence = ""
				case "no_reply_cut":
					m.Faults[0].ReplyCutEvidence = ""
				case "missing_case":
					m.Auth = m.Auth[1:]
				case "duplicate_effect":
					m.Faults[0].Effects++
				}
				if checkAuth(m) == nil {
					t.Fatal("auth/replay oracle accepted a missing barrier or fault hit")
				}
			})
		}
	})
	t.Run("live", func(t *testing.T) {
		r := runLive(t, "auth-replay")
		if err := checkAuth(r); err != nil {
			t.Fatal(err)
		}
		if err := checkMixed(r); err != nil {
			t.Fatal(err)
		}
	})
}

type authTransport struct{ *infratransport.Transport }

func (t authTransport) Cancel()                 { t.Interrupt() }
func (t authTransport) Join(d i.Deadline) error { return infratransport.Move(t.Transport, nil, d, nil) }

// The real embedded server sends signed auth_callout requests to the ordinary
// local queue. Handler reads a real session/provisioning DB. RPC echo subjects
// exercise private-inbox ordering only; full broker business handlers and all
// producers are intentionally required by the separate live P22/P23 lane.
func exerciseNormalAuth(t *testing.T) {
	var keys []nkeys.KeyPair
	for n := 0; n < 3; n++ {
		key, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	keys = append(keys, account)
	defer func() {
		for _, key := range keys {
			key.Wipe()
		}
	}()
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestResource(t, db)
	brokerKey, _ := keys[0].PublicKey()
	memberKey, _ := keys[1].PublicKey()
	agentKey, _ := keys[2].PublicKey()
	accountKey, _ := account.PublicKey()
	memberFP, _ := auth.FingerprintFromActor(memberKey)
	agentFP, _ := auth.FingerprintFromActor(agentKey)
	if _, err := session.Create(db, "business", "business", memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, "business", "node", agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		AlwaysEnableNonce: true, AuthTimeout: 0.15, Nkeys: []*server.NkeyUser{{Nkey: brokerKey}},
		AuthCallout: &server.AuthCallout{Issuer: accountKey, Account: "$G", AuthUsers: []string{brokerKey}}})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("auth server did not start")
	}
	handler := &authcallout.Handler{DB: db, AccountKp: account, Now: time.Now, JWTTTL: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var generation atomic.Uint64
	var reject, recoveryBlocked atomic.Bool
	memberSubject, agentSubject := proto.SubjCtrlNodeList(memberKey, "business"), proto.SubjNodeRegister("business", "node")
	respond := func(msg *nats.Msg) {
		if !strings.HasPrefix(msg.Reply, auth.InboxRoot+".") {
			t.Error("ordinary probe did not use a private inbox")
			return
		}
		if err := msg.Respond([]byte("original-private-rpc")); err != nil {
			t.Error(err)
		}
	}
	owner, err := infraauth.New(infraauth.Config{URL: s.ClientURL(), AccountPublicKey: accountKey,
		Options: func() []nats.Option { return []nats.Option{nats.Nkey(brokerKey, keys[0].Sign)} },
		Transport: func(d i.Deadline, stop i.Stop) (infraauth.Transport, error) {
			tr, err := infratransport.New(generation.Add(1), d, stop, nil)
			return authTransport{tr}, err
		},
		HandleAuth: func(request string) (string, error) {
			if reject.Load() {
				return "", errors.New("normal auth responder disabled")
			}
			return handler.Handle(request)
		},
		Business: []infraauth.Binding{{Subject: memberSubject, Handle: respond}, {Subject: agentSubject, Handle: respond}},
		Member:   infraauth.Identity{Name: "tether-cli:business", PublicKey: memberKey, Sign: keys[1].Sign, RPCSubject: memberSubject, RPCRequest: []byte("member"), RPCResponse: []byte("original-private-rpc")},
		Agent:    infraauth.Identity{Name: "tether-agent:business:node", PublicKey: agentKey, Sign: keys[2].Sign, RPCSubject: agentSubject, RPCRequest: []byte("agent"), RPCResponse: []byte("original-private-rpc")},
		Recovery: func(uint64, i.Deadline, i.Stop) error {
			if recoveryBlocked.Load() {
				return nats.ErrNoResponders
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	var previous infraauth.Receipt
	for round := 0; round < 3; round++ {
		receipt, err := owner.Open(deadline(t), nil)
		if err != nil || receipt.Phase != infraauth.ControlReady || receipt.AuthCID == 0 || receipt.BusinessCID == 0 || receipt.AuthCID == receipt.BusinessCID ||
			receipt.Generation <= previous.Generation || receipt.AuthCID == previous.AuthCID || receipt.BusinessCID == previous.BusinessCID ||
			receipt.AuthPONG > receipt.AuthUsable || receipt.AuthUsable > receipt.BusinessPONG || receipt.BusinessPONG > receipt.ControlReady {
			t.Fatal("real ordinary reconnect did not rebuild auth before business: ", err)
		}
		for _, probe := range receipt.Probes {
			if probe.Generation != receipt.Generation || probe.ServerID != receipt.ServerID || probe.RequestDigest == (i.Digest{}) ||
				probe.ReplyBootNS < receipt.AuthPONG || probe.RPCBootNS < receipt.BusinessPONG {
				t.Fatal("ordinary local queue or actual private RPC proof is missing")
			}
		}
		if len(receipt.AuthSUBs) != 1 || receipt.AuthSUBs[0].Subject != "$SYS.REQ.USER.AUTH" || receipt.AuthSUBs[0].Queue != "tether-authcallout" {
			t.Fatal("ordinary proof came from a temporary auth responder")
		}
		previous = receipt
	}
	recoveryBlocked.Store(true)
	if _, err := owner.Open(deadline(t), nil); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatal("JS503 did not retain the replay barrier: ", err)
	}
	if _, _, err := owner.Business(); err == nil {
		t.Fatal("business SUB replay crossed a failed recovery gate")
	}
	recoveryBlocked.Store(false)
	reject.Store(true)
	if _, err := owner.Open(deadline(t), nil); err == nil {
		t.Fatal("missing normal auth responder was covered by a peer or temporary probe")
	}
	if _, _, err := owner.Business(); err == nil {
		t.Fatal("business admission survived missing ordinary auth")
	}
}

// origin: hot-update S9 plan §4.3 / §4.4 / §7.2 / T70 / M19 / M20 / M24
func checkAuth(r businessReceipt) error {
	if err := checkAuthScopes(r); err != nil {
		return err
	}
	expected := map[string]bool{"cold": false, "hot": false, "ordinary-reconnect": false, "missing-normal-queue": false,
		"js503": false, "ack-prefix-loss": false, "replica-insufficient": false}
	for _, row := range r.Auth {
		if _, ok := expected[row.Case]; !ok || row.Step == 0 || !validDigest(row.Evidence) {
			return errors.New("auth/replay case lacks independent evidence")
		}
		positive := row.Case == "cold" || row.Case == "hot" || row.Case == "ordinary-reconnect"
		if positive {
			if !row.Admitted || !row.LocalQueue || row.Queue != "tether-authcallout" || row.Generation == 0 || row.ServerID == "" || row.AuthCID == 0 ||
				row.BusinessCID == 0 || row.BusinessCID == row.AuthCID || row.AuthSUBNS <= 0 || row.AuthPONGNS < row.AuthSUBNS ||
				row.OrdinaryAuthNS < row.AuthPONGNS || row.PrefixReadNS < row.OrdinaryAuthNS || row.RepairNS < row.PrefixReadNS ||
				row.BusinessSUBNS < row.RepairNS || row.MemberRPCNS < row.BusinessSUBNS || row.AgentRPCNS < row.BusinessSUBNS ||
				!validDigest(row.MemberRequest) || !validDigest(row.AgentRequest) || !validDigest(row.PrefixHash) {
				return errors.New("ordinary local auth and pre-repair prefix did not precede business replay")
			}
		} else if row.Admitted || row.RepairNS != 0 || row.BusinessSUBNS != 0 {
			return errors.New("negative auth/recovery gate admitted business or repaired evidence")
		}
		expected[row.Case] = true
	}
	for _, present := range expected {
		if !present {
			return errors.New("auth/recovery barrier case is missing")
		}
	}
	faults := map[string]bool{"publish": false, "request": false, "pty-input": false, "pty-resize": false, "pty-signal": false, "pty-keepalive": false}
	for _, fault := range r.Faults {
		if _, ok := faults[fault.Kind]; !ok || faults[fault.Kind] || !validDigest(fault.RequestDigest) || !validDigest(fault.SentEvidence) ||
			!validDigest(fault.EffectEvidence) || !validDigest(fault.ReplyCutEvidence) || fault.Effects != 1 || fault.SentRequestDigest != fault.RequestDigest || fault.EffectRequestDigest != fault.RequestDigest || fault.ReplyRequestDigest != fault.RequestDigest {
			return errors.New("lost ACK lacks same-request sent/effect/reply-cut custody")
		}
		faults[fault.Kind] = true
	}
	for _, present := range faults {
		if !present {
			return errors.New("real lost ACK effect lane is missing")
		}
	}
	return nil
}

func authControls() businessReceipt {
	r := oracleReceipt()
	populateAuthControls(&r)
	return r
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
