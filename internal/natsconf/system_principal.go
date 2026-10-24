package natsconf

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/nats-io/nkeys"
)

// SystemPrincipals carries public user identities only. P03 owns seed generation, protected
// installation and explicit rotation; rendering must never read or serialize their seeds.
// origin: hot-update S9 plan §3.3 / T27/T28
type SystemPrincipals struct {
	ReadNkey string
	StepNkey string
}

const (
	SystemReadSeedFile    = "infra-sys-read.nk"
	SystemStepSeedFile    = "infra-sys-step.nk"
	SystemStepdownSubject = "$JS.API.META.LEADER.STEPDOWN"
)

// SystemPrincipalDescriptor is the authority consumed by the nonroot query/step adapters.
// SeedFile is a basename under P03's trusted credential directory, never a caller path.
// ReplyPrefix is passed as nats.CustomInboxPrefix so the default shared inbox is not authorized.
type SystemPrincipalDescriptor struct {
	SeedFile    string
	Nkey        string
	ReplyPrefix string
	Publish     []string
	Subscribe   []string
}

// SystemPrincipalDescriptors returns fresh slices: a consumer cannot widen the renderer's
// permission set by mutating a descriptor. Step also reads readiness before requesting a cut.
// origin: hot-update S9 plan §3.3 / T28
func SystemPrincipalDescriptors(principals SystemPrincipals) ([]SystemPrincipalDescriptor, error) {
	if !nkeys.IsValidPublicUserKey(principals.ReadNkey) || !nkeys.IsValidPublicUserKey(principals.StepNkey) || principals.ReadNkey == principals.StepNkey {
		return nil, fmt.Errorf("natsconf: system principals require two distinct public user nkeys")
	}
	var descriptors []SystemPrincipalDescriptor
	for _, identity := range []struct{ file, key string }{{SystemReadSeedFile, principals.ReadNkey}, {SystemStepSeedFile, principals.StepNkey}} {
		prefix := "_INBOX.tether.infra.sys." + identity.key
		descriptor := SystemPrincipalDescriptor{
			SeedFile: identity.file, Nkey: identity.key, ReplyPrefix: prefix,
			Publish:   []string{"$SYS.REQ.SERVER.*.JSZ", "$SYS.REQ.SERVER.*.VARZ", "$SYS.REQ.ACCOUNT.*.INFO"},
			Subscribe: []string{prefix + ".>"},
		}
		if identity.file == SystemStepSeedFile {
			descriptor.Publish = append(descriptor.Publish, SystemStepdownSubject)
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, nil
}

// SystemPrincipalBlock renders the existing default $SYS account, without changing the
// SystemAccount option or moving the top-level authorization's users out of $G.
func SystemPrincipalBlock(principals SystemPrincipals) (string, error) {
	descriptors, err := SystemPrincipalDescriptors(principals)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	b.WriteString("accounts {\n  $SYS {\n    users: [\n")
	for _, descriptor := range descriptors {
		fmt.Fprintf(&b, "      { nkey: %q\n        permissions {\n", descriptor.Nkey)
		writeAllowBlock(&b, "publish", descriptor.Publish)
		writeAllowBlock(&b, "subscribe", descriptor.Subscribe)
		b.WriteString("        }\n      }\n")
	}
	b.WriteString("    ]\n  }\n}\n")
	return b.String(), nil
}

// SystemPrincipalIdentity reads the exact managed block. It refuses foreign accounts or
// broadened permissions rather than silently rewriting an operator's intent on reconcile.
// origin: hot-update S9 plan §3.3 / T27/T28
func SystemPrincipalIdentity(own *Ownership) (SystemPrincipals, error) {
	if own == nil {
		return SystemPrincipals{}, fmt.Errorf("natsconf: system principal ownership is missing")
	}
	return systemPrincipalsFromParsed(own.Parsed)
}

// LoadedSystemPrincipalDescriptors is the auth-callout side of SYS admission: the
// managed users of the config file the server loads. NATS delegates those static
// $SYS users to the normal callout queue (config mode, empty allowed_accounts),
// and adding them to AuthCallout.AuthUsers is refused on reload, so the callout
// must recognize exactly these identities. No managed block yields none; a
// broadened or malformed block is an error, so the caller admits no SYS identity.
// origin: hot-update S9 plan §3.3 / P12 / T27 — normal-queue SYS admission
func LoadedSystemPrincipalDescriptors(confPath string) ([]SystemPrincipalDescriptor, error) {
	own, err := Preflight(confPath)
	if err != nil {
		return nil, err
	}
	principals, err := SystemPrincipalIdentity(own)
	if err != nil || principals == (SystemPrincipals{}) {
		return nil, err
	}
	return SystemPrincipalDescriptors(principals)
}

// WithSystemPrincipals produces a new ownership view for initial provisioning or
// explicit P03 rotation. The old view, $G authorization, storage and listeners stay intact.
// This operation only renders public identities; its caller still owns validation/apply/reload.
// origin: hot-update S9 plan §3.3 / T27/T28
func WithSystemPrincipals(own *Ownership, principals SystemPrincipals) (*Ownership, error) {
	if _, err := SystemPrincipalIdentity(own); err != nil {
		return nil, err
	}
	descriptors, err := SystemPrincipalDescriptors(principals)
	if err != nil {
		return nil, err
	}
	parsed := make(map[string]any, len(own.Parsed)+1)
	for key, value := range own.Parsed {
		parsed[key] = value
	}
	users := make([]any, 0, len(descriptors))
	for _, descriptor := range descriptors {
		users = append(users, systemPrincipalUser(descriptor))
	}
	parsed["accounts"] = map[string]any{"$SYS": map[string]any{"users": users}}
	if _, err := systemPrincipalsFromParsed(parsed); err != nil {
		return nil, err
	}
	entries := append([]Owned(nil), own.Entries...)
	if _, present := own.Parsed["accounts"]; !present {
		entries = append(entries, Owned{Key: "accounts", Bucket: InstallSafe})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return &Ownership{Entries: entries, Parsed: parsed}, nil
}

// origin: hot-update S9 plan §3.3 / T27/T28
func systemPrincipalsFromParsed(parsed map[string]any) (SystemPrincipals, error) {
	if value, present := parsed["system_account"]; present && value != "$SYS" {
		return SystemPrincipals{}, fmt.Errorf("natsconf: refusing a change to the default system account")
	}
	value, present := parsed["accounts"]
	if !present {
		return SystemPrincipals{}, nil
	}
	accounts, ok := value.(map[string]any)
	if !ok || len(accounts) != 1 {
		return SystemPrincipals{}, fmt.Errorf("natsconf: only the managed $SYS account is supported")
	}
	system, ok := accounts["$SYS"].(map[string]any)
	if !ok || len(system) != 1 {
		return SystemPrincipals{}, fmt.Errorf("natsconf: unsupported system account options")
	}
	users, ok := system["users"].([]any)
	if !ok || len(users) != 2 {
		return SystemPrincipals{}, fmt.Errorf("natsconf: system account requires exactly two managed users")
	}
	var principals SystemPrincipals
	for _, raw := range users {
		user, ok := raw.(map[string]any)
		if !ok {
			return SystemPrincipals{}, fmt.Errorf("natsconf: invalid managed system user")
		}
		key, _ := user["nkey"].(string)
		if !nkeys.IsValidPublicUserKey(key) {
			return SystemPrincipals{}, fmt.Errorf("natsconf: invalid system user public nkey")
		}
		// The role is recognized by its exact authority, never its position in an array.
		// This also refuses secrets, foreign reply roots, denies/response grants and aliases.
		descriptors, _ := SystemPrincipalDescriptors(SystemPrincipals{ReadNkey: key, StepNkey: otherSystemKey(users, key)})
		if len(descriptors) != 2 {
			return SystemPrincipals{}, fmt.Errorf("natsconf: system users must have distinct identities")
		}
		read := descriptors[0]
		step := read
		step.Publish = append(append([]string(nil), read.Publish...), SystemStepdownSubject)
		switch {
		case reflect.DeepEqual(user, systemPrincipalUser(read)) && principals.ReadNkey == "":
			principals.ReadNkey = key
		case reflect.DeepEqual(user, systemPrincipalUser(step)) && principals.StepNkey == "":
			principals.StepNkey = key
		default:
			return SystemPrincipals{}, fmt.Errorf("natsconf: system user authority differs from the managed descriptor")
		}
	}
	if _, err := SystemPrincipalDescriptors(principals); err != nil {
		return SystemPrincipals{}, err
	}
	if auth, ok := parsed["authorization"].(map[string]any); ok {
		if users, ok := auth["users"].([]any); ok {
			for _, raw := range users {
				if user, ok := raw.(map[string]any); ok && (user["nkey"] == principals.ReadNkey || user["nkey"] == principals.StepNkey) {
					return SystemPrincipals{}, fmt.Errorf("natsconf: system principal cannot also be a global account user")
				}
			}
		}
	}
	return principals, nil
}

func otherSystemKey(users []any, key string) string {
	for _, raw := range users {
		if user, ok := raw.(map[string]any); ok {
			if candidate, _ := user["nkey"].(string); candidate != key {
				return candidate
			}
		}
	}
	return ""
}

func systemPrincipalUser(descriptor SystemPrincipalDescriptor) map[string]any {
	allow := func(subjects []string) map[string]any {
		values := make([]any, len(subjects))
		for i, subject := range subjects {
			values[i] = subject
		}
		return map[string]any{"allow": values}
	}
	return map[string]any{"nkey": descriptor.Nkey, "permissions": map[string]any{
		"publish": allow(descriptor.Publish), "subscribe": allow(descriptor.Subscribe),
	}}
}

// preserveSystemPrincipals is the final merge guard. Every RenderDesired/reconcile intent
// passes here, so a topology render retains the provisioned SYS users and default account selection.
// origin: hot-update S9 plan §3.3 / T27
func preserveSystemPrincipals(own *Ownership, rendered string) (string, error) {
	principals, err := SystemPrincipalIdentity(own)
	if err != nil {
		return "", err
	}
	if principals != (SystemPrincipals{}) {
		// A peer nkey appearing in both account scopes makes server config load fail;
		// check the generated authorization too, before any caller can Apply it.
		if strings.Contains(rendered, principals.ReadNkey) || strings.Contains(rendered, principals.StepNkey) {
			return "", fmt.Errorf("natsconf: rendered global identity collides with a system principal")
		}
		block, err := SystemPrincipalBlock(principals)
		if err != nil {
			return "", err
		}
		rendered += "\n" + block
	}
	if _, explicit := own.Parsed["system_account"]; explicit {
		rendered += "system_account: \"$SYS\"\n"
	}
	return rendered, nil
}

// lowerKeys recursively lowercases every map key (nats keys are case-insensitive). Values
// are preserved; nested maps + slices of maps are descended so the accessors that read
// nested keys (auth_callout, users, store_dir) are also case-insensitive.
func lowerKeys(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		key := strings.ToLower(k)
		// origin: hot-update S9 plan §3.3 / T27
		// Account names are identities, not directive names; $sys is a different account.
		if key == "accounts" {
			if accounts, ok := v.(map[string]any); ok {
				preserved := make(map[string]any, len(accounts))
				for name, account := range accounts {
					preserved[name] = lowerValue(account)
				}
				v = preserved
			}
			out[key] = v
		} else {
			out[key] = lowerValue(v)
		}
	}
	return out
}

func lowerValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return lowerKeys(t)
	case []any:
		for i := range t {
			t[i] = lowerValue(t[i])
		}
		return t
	default:
		return v
	}
}
