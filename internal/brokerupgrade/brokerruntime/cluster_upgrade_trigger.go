package brokerruntime

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// upgradeTriggerSkew bounds replay: a signed request whose issued_at is farther than this from the
// broker's clock is refused (the same request can't be replayed hours later).
const upgradeTriggerSkew = 5 * time.Minute

// upgradeTriggerCodeUnauthorized is the ClusterUpgradeResp code for a request that does not carry a valid
// account signature (the socket-only adminsock vocabulary has no auth code — the socket is root-gated).
const upgradeTriggerCodeUnauthorized = "unauthorized"

// verifyUpgradeTrigger is the shared account-signature, target and replay gate. A nil request with a
// nil response means "another broker's request: stay silent"; a nil request with a response refuses.
func VerifyUpgradeTrigger(selfID string, accountSeed func() []byte, data []byte, now time.Time) (*proto.ClusterUpgradeReq, *proto.ClusterUpgradeResp) {
	var req proto.ClusterUpgradeReq
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "malformed request"}
	}
	// Act on THIS broker only; a wrong/empty target is silent so the intended broker's reply wins.
	if selfID == "" || req.TargetNode != selfID {
		return nil, nil
	}
	// origin: hot-update S6 plan §9.1 / T40
	if err := proto.ValidateUpgradeReq(&req); err != nil {
		return nil, &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "invalid signed upgrade identifiers"}
	}
	// The REAL gate: verify the account signature against this broker's pinned account_pub. Fail-closed.
	if len(accountSeed()) == 0 {
		return nil, &proto.ClusterUpgradeResp{Code: adminsock.CodeClusterNotEnabled, Error: "no account key to verify the upgrade trigger"}
	}
	accountPub, err := auth.PublicKeyFromSeed(accountSeed())
	if err != nil {
		return nil, &proto.ClusterUpgradeResp{Code: adminsock.CodeStoreError, Error: "account key error"}
	}
	sig, decErr := hex.DecodeString(req.Sig)
	if decErr != nil || auth.VerifySignature(accountPub, proto.CanonicalUpgradeReqBytes(&req), sig) != nil {
		return nil, &proto.ClusterUpgradeResp{Code: upgradeTriggerCodeUnauthorized, Error: "signature does not verify against the cluster account key"}
	}
	// Bound replay: issued_at must be within the skew window of the broker's clock.
	t, perr := time.Parse(time.RFC3339, req.IssuedAt)
	if perr != nil || absDuration(now.Sub(t)) > upgradeTriggerSkew {
		return nil, &proto.ClusterUpgradeResp{Code: adminsock.CodeBadRequest, Error: "stale or unparseable issued_at (replay guard)"}
	}
	return &req, nil
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
