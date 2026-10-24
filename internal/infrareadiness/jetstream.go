package infrareadiness

import (
	"bytes"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
)

// Requester permits loss/corruption injection at the read-only wire seam. The
// production adapter owns only request contexts, never the caller's shared NC.
type Requester interface {
	Request(string, []byte, infracontract.Deadline, infracontract.Stop) ([]byte, error)
}

type NATSRequester struct{ Conn *nats.Conn }

func (r NATSRequester) Request(subject string, payload []byte, deadline infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	if r.Conn == nil {
		return nil, infracontract.ErrIncomplete
	}
	ctx, finish, err := requestContext(deadline, stop)
	if err != nil {
		return nil, err
	}
	defer finish()
	msg, err := r.Conn.RequestWithContext(ctx, subject, payload)
	if err != nil || admission(deadline, stop) != nil {
		return nil, infracontract.ErrIncomplete
	}
	return msg.Data, nil
}

// Each account needs an explicitly authorized read connection. Missing access to
// a foreign JS-enabled account is an incomplete inventory, never a filtered LIST.
type AccountAccess struct {
	ID    string
	Query Requester
}

type JetStream struct {
	system   Requester
	accounts []AccountAccess
}

var _ InventorySource = (*JetStream)(nil)

func NewJetStream(system Requester, accounts []AccountAccess) (*JetStream, error) {
	if system == nil || len(accounts) == 0 {
		return nil, infracontract.ErrIncomplete
	}
	seen := make(map[string]bool)
	for _, a := range accounts {
		if a.ID == "" || a.Query == nil || seen[a.ID] {
			return nil, infracontract.ErrIncomplete
		}
		seen[a.ID] = true
	}
	return &JetStream{system: system, accounts: append([]AccountAccess(nil), accounts...)}, nil
}

// These reduced upstream records retain the fields used by the predicate. The
// production leaf imports only the NATS client; SYS carries server-side facts.
type systemResponse struct {
	Server *struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version string `json:"ver"`
	} `json:"server"`
	Data  *systemData    `json:"data"`
	Error *nats.APIError `json:"error"`
}

type systemData struct {
	ID       string `json:"server_id"`
	Version  string `json:"version"`
	Disabled bool   `json:"disabled"`
	Total    *int   `json:"total"`
	Accounts []struct {
		ID string `json:"id"`
	} `json:"account_details"`
	Meta *struct {
		Name     string           `json:"name"`
		Leader   string           `json:"leader"`
		Size     int              `json:"cluster_size"`
		Replicas []*nats.PeerInfo `json:"replicas"`
	} `json:"meta_cluster"`
}

type paged struct {
	Type    string          `json:"type"`
	Total   *int            `json:"total"`
	Offset  *int            `json:"offset"`
	Limit   *int            `json:"limit"`
	Error   *nats.APIError  `json:"error"`
	Missing []string        `json:"missing"`
	Offline json.RawMessage `json:"offline"`
}

type streamPage struct {
	paged
	Streams []*nats.StreamInfo `json:"streams"`
}

type consumerPage struct {
	paged
	Consumers []*nats.ConsumerInfo `json:"consumers"`
}

type readBudget struct{ bytes int }

// query checks the whole snapshot budget before decoding: oversized or malformed
// replies cannot be truncated into an apparently complete set of assets.
// origin: hot-update S9 plan §2.2 / §3.2 / T25
func (b *readBudget) query(r Requester, subject string, payload []byte, out interface{}, deadline infracontract.Deadline, stop infracontract.Stop) error {
	if err := admission(deadline, stop); err != nil {
		return err
	}
	data, err := r.Request(subject, payload, deadline, stop)
	if err != nil || len(data) == 0 || len(data) > infracontract.MaxCensusPageBytes ||
		len(data) > infracontract.MaxCensusBytes-b.bytes || !utf8.Valid(data) {
		return infracontract.ErrIncomplete
	}
	b.bytes += len(data)
	if json.Unmarshal(data, out) != nil || admission(deadline, stop) != nil {
		return infracontract.ErrIncomplete
	}
	return nil
}

// ReadInventory queries every observed JS server, then every discovered account,
// stream and consumer. No create/update/delete/purge or hidden replica repair is
// reachable from this owner. The normal $G connection cannot stand in for SYS.
// origin: hot-update S9 plan §3.2 / T25
func (j *JetStream) ReadInventory(t Topology, deadline infracontract.Deadline, stop infracontract.Stop) (Snapshot, error) {
	b := &readBudget{}
	ids, meta, err := j.readSystem(t, b, deadline, stop)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Meta: meta}
	for _, id := range ids {
		var query Requester
		for _, a := range j.accounts {
			if a.ID == id {
				query = a.Query
			}
		}
		if query == nil {
			return Snapshot{}, infracontract.ErrIncomplete
		}
		account, groups, err := readAccount(t, id, query, b, deadline, stop)
		if err != nil {
			return Snapshot{}, err
		}
		snapshot.Accounts = append(snapshot.Accounts, account)
		snapshot.Groups = append(snapshot.Groups, groups...)
	}
	// An account added/deleted during the first pass invalidates the inventory.
	// The configuration admission fence additionally excludes changes between reads.
	afterIDs, afterMeta, err := j.readSystem(t, b, deadline, stop)
	if err != nil || digest(ids) != digest(afterIDs) || digest(meta) != digest(afterMeta) {
		return Snapshot{}, infracontract.ErrIncomplete
	}
	snapshot.Complete = true
	return canonicalSnapshot(snapshot), nil
}

func (j *JetStream) readSystem(t Topology, b *readBudget, deadline infracontract.Deadline, stop infracontract.Stop) ([]string, Group, error) {
	ids := make(map[string]bool)
	var meta Group
	metaLeader, metaName, metaSize := "", "", 0
	js := 0
	for _, s := range canonicalTopology(t).Servers {
		if !s.JetStream {
			continue
		}
		js++
		var varz systemResponse
		if b.query(j.system, "$SYS.REQ.SERVER."+s.ID+".VARZ", nil, &varz, deadline, stop) != nil ||
			!validSystem(varz, s) || varz.Data.ID != s.ID || varz.Data.Version != s.Version {
			return nil, Group{}, infracontract.ErrIncomplete
		}
		seen, offset, total := make(map[string]bool), 0, -1
		finished := false
		for page := 0; page < infracontract.MaxCensusPages; page++ {
			request, _ := json.Marshal(struct {
				Accounts bool `json:"accounts"`
				Offset   int  `json:"offset"`
				Limit    int  `json:"limit"`
			}{true, offset, infracontract.MaxCensusPages})
			var response systemResponse
			if b.query(j.system, "$SYS.REQ.SERVER."+s.ID+".JSZ", request, &response, deadline, stop) != nil ||
				!validSystem(response, s) || response.Data.ID != s.ID || response.Data.Disabled || response.Data.Total == nil {
				return nil, Group{}, infracontract.ErrIncomplete
			}
			data := response.Data
			if total == -1 {
				total = *data.Total
			}
			if total < 0 || *data.Total != total || len(data.Accounts) > infracontract.MaxCensusPages || offset+len(data.Accounts) > total {
				return nil, Group{}, infracontract.ErrIncomplete
			}
			for _, a := range data.Accounts {
				if a.ID == "" || seen[a.ID] {
					return nil, Group{}, infracontract.ErrIncomplete
				}
				seen[a.ID], ids[a.ID] = true, true
			}
			if t.Standalone {
				if data.Meta != nil {
					return nil, Group{}, infracontract.ErrIncomplete
				}
			} else {
				if data.Meta == nil || data.Meta.Size < 3 || data.Meta.Leader == "" {
					return nil, Group{}, infracontract.ErrIncomplete
				}
				if metaLeader == "" {
					metaLeader, metaName, metaSize = data.Meta.Leader, data.Meta.Name, data.Meta.Size
				}
				// The leader's complete peer list and each independent follower must
				// name the same meta group/leader/configured size in this observation.
				// origin: hot-update S9 plan §3.2 / T24
				if data.Meta.Name == "" || data.Meta.Leader != metaLeader || data.Meta.Name != metaName || data.Meta.Size != metaSize {
					return nil, Group{}, infracontract.ErrIncomplete
				}
				// Only the actual meta leader supplies the full peer list. Follower
				// observations must agree on its identity, never certify an empty list.
				if data.Meta.Leader == s.Name {
					ci := &nats.ClusterInfo{Name: data.Meta.Name, Leader: data.Meta.Leader, Replicas: data.Meta.Replicas}
					g, err := observedGroup(t, ci, data.Meta.Size, data.Meta.Name+"/meta")
					if err != nil || (meta.ID != "" && digest(meta) != digest(g)) {
						return nil, Group{}, infracontract.ErrIncomplete
					}
					meta = g
				}
				if serverID(t, data.Meta.Leader) == "" {
					return nil, Group{}, infracontract.ErrIncomplete
				}
			}
			offset += len(data.Accounts)
			if offset == total {
				finished = true
				break
			}
			if len(data.Accounts) == 0 {
				return nil, Group{}, infracontract.ErrIncomplete
			}
		}
		if !finished {
			return nil, Group{}, infracontract.ErrIncomplete
		}
	}
	if js == 0 || (!t.Standalone && (meta.ID == "" || meta.Replicas != js)) {
		return nil, Group{}, infracontract.ErrIncomplete
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, canonicalGroup(meta), nil
}

func validSystem(r systemResponse, s Server) bool {
	return r.Error == nil && r.Server != nil && r.Data != nil &&
		r.Server.ID == s.ID && r.Server.Name == s.Name && r.Server.Version == s.Version
}

func readAccount(t Topology, id string, q Requester, b *readBudget, deadline infracontract.Deadline, stop infracontract.Stop) (Account, []Group, error) {
	var info struct {
		Type      string         `json:"type"`
		Error     *nats.APIError `json:"error"`
		Streams   *int           `json:"streams"`
		Consumers *int           `json:"consumers"`
	}
	if b.query(q, "$JS.API.INFO", nil, &info, deadline, stop) != nil || info.Error != nil ||
		info.Type != "io.nats.jetstream.api.v1.account_info_response" || info.Streams == nil || info.Consumers == nil ||
		*info.Streams < 0 || *info.Consumers < 0 || uint64(*info.Streams) > uint64(^uint32(0)) || uint64(*info.Consumers) > uint64(^uint32(0)) {
		return Account{}, nil, infracontract.ErrIncomplete
	}
	streams, err := listStreams(q, b, *info.Streams, deadline, stop)
	if err != nil {
		return Account{}, nil, err
	}
	var groups []Group
	consumerCount := 0
	for _, s := range streams {
		g, err := streamGroup(t, id, s)
		if err != nil {
			return Account{}, nil, err
		}
		groups = append(groups, g)
		consumers, err := listConsumers(q, b, s.Config.Name, s.State.Consumers, deadline, stop)
		if err != nil {
			return Account{}, nil, err
		}
		for _, c := range consumers {
			cg, err := consumerGroup(t, id, s, c)
			if err != nil {
				return Account{}, nil, err
			}
			groups = append(groups, cg)
		}
		consumerCount += len(consumers)
		// Creation/config/group identity must still match after enumerating the
		// consumers; deletion/recreation under the same name is not continuity.
		// origin: hot-update S9 plan §3.2 / T25 / M13
		var after struct {
			*nats.StreamInfo
			Error *nats.APIError `json:"error"`
		}
		if b.query(q, "$JS.API.STREAM.INFO."+s.Config.Name, nil, &after, deadline, stop) != nil ||
			after.Error != nil || after.StreamInfo == nil || after.State.Consumers != s.State.Consumers {
			return Account{}, nil, infracontract.ErrIncomplete
		}
		ag, err := streamGroup(t, id, after.StreamInfo)
		if err != nil || digest(g) != digest(ag) {
			return Account{}, nil, infracontract.ErrIncomplete
		}
	}
	if consumerCount != *info.Consumers {
		return Account{}, nil, infracontract.ErrIncomplete
	}
	return Account{ID: id, Streams: uint32(len(streams)), Consumers: uint32(consumerCount)}, groups, nil
}

func listStreams(q Requester, b *readBudget, total int, deadline infracontract.Deadline, stop infracontract.Stop) ([]*nats.StreamInfo, error) {
	var result []*nats.StreamInfo
	seen := make(map[string]bool)
	for page := 0; page < infracontract.MaxCensusPages; page++ {
		var p streamPage
		if b.query(q, "$JS.API.STREAM.LIST", offsetRequest(len(result)), &p, deadline, stop) != nil ||
			!validPage(p.paged, "stream", len(result), total, len(p.Streams)) {
			return nil, infracontract.ErrIncomplete
		}
		for _, s := range p.Streams {
			if s == nil || !assetName(s.Config.Name) || seen[s.Config.Name] {
				return nil, infracontract.ErrIncomplete
			}
			seen[s.Config.Name] = true
			result = append(result, s)
		}
		if len(result) == total {
			return result, nil
		}
	}
	return nil, infracontract.ErrIncomplete
}

func listConsumers(q Requester, b *readBudget, stream string, total int, deadline infracontract.Deadline, stop infracontract.Stop) ([]*nats.ConsumerInfo, error) {
	var result []*nats.ConsumerInfo
	seen := make(map[string]bool)
	for page := 0; page < infracontract.MaxCensusPages; page++ {
		var p consumerPage
		if b.query(q, "$JS.API.CONSUMER.LIST."+stream, offsetRequest(len(result)), &p, deadline, stop) != nil ||
			!validPage(p.paged, "consumer", len(result), total, len(p.Consumers)) {
			return nil, infracontract.ErrIncomplete
		}
		for _, c := range p.Consumers {
			if c == nil || !assetName(c.Name) || c.Stream != stream || seen[c.Name] {
				return nil, infracontract.ErrIncomplete
			}
			seen[c.Name] = true
			result = append(result, c)
		}
		if len(result) == total {
			return result, nil
		}
	}
	return nil, infracontract.ErrIncomplete
}

func validPage(p paged, kind string, offset, total, count int) bool {
	// Require reported offsets/totals, forward progress and zero missing/offline
	// entries. Partial upstream LIST responses must never acquire Complete=true.
	// origin: hot-update S9 plan §3.2 / T25 / M13
	return p.Error == nil && p.Type == "io.nats.jetstream.api.v1."+kind+"_list_response" &&
		p.Offset != nil && *p.Offset == offset && p.Total != nil && *p.Total == total && total >= 0 &&
		p.Limit != nil && *p.Limit > 0 && count <= *p.Limit && count <= total-offset &&
		(count > 0 || offset == total) && len(p.Missing) == 0 &&
		(len(p.Offline) == 0 || bytes.Equal(p.Offline, []byte("null")) || bytes.Equal(p.Offline, []byte("{}")))
}

func offsetRequest(offset int) []byte {
	return []byte(`{"offset":` + strconv.Itoa(offset) + `}`)
}

func assetName(name string) bool {
	return name != "" && !strings.ContainsAny(name, ".*>/\\ \t\n\r\x00")
}

func streamGroup(t Topology, account string, s *nats.StreamInfo) (Group, error) {
	if s == nil || !assetName(s.Config.Name) || s.Created.IsZero() || s.Config.Replicas < 1 {
		return Group{}, infracontract.ErrIncomplete
	}
	g, err := observedGroup(t, s.Cluster, s.Config.Replicas, "")
	if err != nil {
		return Group{}, err
	}
	cfg := s.Config
	cfg.Metadata = configurationMetadata(cfg.Metadata)
	g.Account, g.Stream, g.CreatedNS, g.ConfigSHA256 = account, s.Config.Name, s.Created.UnixNano(), digest(cfg)
	if g.Replicas == 1 {
		g.ID = digestID(g)
	}
	return g, nil
}

func consumerGroup(t Topology, account string, s *nats.StreamInfo, c *nats.ConsumerInfo) (Group, error) {
	if c == nil || !assetName(c.Name) || c.Stream != s.Config.Name || c.Created.IsZero() || c.Config.Replicas < 0 {
		return Group{}, infracontract.ErrIncomplete
	}
	replicas := c.Config.Replicas
	// NATS' configured effective count is not always the parent count: legacy
	// ephemerals on a limits-policy stream default to R1. Durable/default and
	// counts above the parent's count inherit/clamp to the stream's replicas.
	// Derive that count before comparing the actual peer list; otherwise a healthy
	// off-target ephemeral R1 would be rejected as a missing R3 consumer group.
	// origin: hot-update S9 plan §3.2 / T24
	if replicas == 0 || replicas > s.Config.Replicas {
		if c.Config.Durable == "" && s.Config.Retention == nats.LimitsPolicy && replicas == 0 {
			replicas = 1
		} else {
			replicas = s.Config.Replicas
		}
	}
	g, err := observedGroup(t, c.Cluster, replicas, "")
	if err != nil {
		return Group{}, err
	}
	g.Account, g.Stream, g.Consumer = account, s.Config.Name, c.Name
	cfg := c.Config
	cfg.Metadata = configurationMetadata(cfg.Metadata)
	g.CreatedNS, g.ConfigSHA256 = c.Created.UnixNano(), digest(cfg)
	if replicas == 1 {
		g.ID = digestID(g)
	}
	return g, nil
}

func observedGroup(t Topology, ci *nats.ClusterInfo, replicas int, id string) (Group, error) {
	if ci == nil {
		if !t.Standalone || replicas != 1 || len(t.Servers) != 1 {
			return Group{}, infracontract.ErrIncomplete
		}
		owner := t.Servers[0].ID
		return Group{Replicas: 1, Leader: owner, Members: []Member{{ServerID: owner, Current: true, Online: t.Servers[0].Online}}}, nil
	}
	leader := serverID(t, ci.Leader)
	if id == "" {
		id = ci.RaftGroup
	}
	if leader == "" || replicas < 1 || (replicas > 1 && id == "") || len(ci.Replicas) != replicas-1 {
		return Group{}, infracontract.ErrIncomplete
	}
	g := Group{ID: id, Replicas: replicas, Leader: leader, Members: []Member{{ServerID: leader, Current: true, Online: true}}}
	for _, peer := range ci.Replicas {
		if peer == nil || serverID(t, peer.Name) == "" {
			return Group{}, infracontract.ErrIncomplete
		}
		g.Members = append(g.Members, Member{ServerID: serverID(t, peer.Name), Current: peer.Current, Online: !peer.Offline, Lag: peer.Lag})
	}
	return canonicalGroup(g), nil
}

func serverID(t Topology, name string) string {
	for _, s := range t.Servers {
		if s.Name == name {
			return s.ID
		}
	}
	return ""
}

func digestID(g Group) string {
	g.Members = nil
	b := digest(g)
	const hex = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[2*i], out[2*i+1] = hex[v>>4], hex[v&15]
	}
	return string(out)
}

func configurationMetadata(metadata map[string]string) map[string]string {
	// INFO enriches the response with server version/API-level metadata whereas
	// LIST reports the stored request level. These three reserved upstream keys
	// describe the response, not a configuration mutation. Preserve user metadata
	// and any other key so a genuine configuration change still invalidates a seal.
	// origin: hot-update S9 plan §3.2 / T25
	result := make(map[string]string)
	for k, v := range metadata {
		if k != "_nats.ver" && k != "_nats.level" && k != "_nats.req.level" {
			result[k] = v
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
