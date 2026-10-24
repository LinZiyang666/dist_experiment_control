package brokerimage

// These descriptors name the closed broker-only partition. The codecs in P02
// and P07 supply field descriptors beside their actual encoders/decoders; the
// skeleton must not make empty field lists look like a complete codec inventory.
// Global sealedstate kinds stay unchanged, so old agent readers remain usable.
// origin: hot-update S6 plan §2.2 / §12.1 P00
const (
	PrivateStateRecord       = "broker-state-v1"
	TunnelStateRecord        = "broker-tunnel-state-v1"
	PrivateDescriptorVersion = 1
)

// Encoding and Type describe private bytes; Secret forbids public projection.
// A variable-width field requires a positive MaxBytes even when currently empty.
type PrivateFieldDescriptor struct {
	Name             string
	Type             string
	Encoding         string
	MaxBytes         int
	Secret           bool
	CompatibleReader string
}

type PrivateRecordDescriptor struct {
	Kind             string
	Version          uint32
	StateRecord      string
	Owner            string
	CompatibleReader string
	Fields           []PrivateFieldDescriptor
}

// PrivateRecordDescriptors is the namespace/owner skeleton, not permission to
// decode arbitrary fields. The actual codec field tables remain with their owner
// and are joined by P32's inventory gate before any implementation claims closure.
var PrivateRecordDescriptors = []PrivateRecordDescriptor{
	{Kind: "broker-runtime", Version: PrivateDescriptorVersion, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain},
	{Kind: "store", Version: PrivateDescriptorVersion, StateRecord: PrivateStateRecord, Owner: "P03", CompatibleReader: Domain},
	{Kind: "control", Version: PrivateDescriptorVersion, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain},
	{Kind: "server", Version: PrivateDescriptorVersion, StateRecord: TunnelStateRecord, Owner: "P07", CompatibleReader: Domain},
	{Kind: "endpoint", Version: PrivateDescriptorVersion, StateRecord: TunnelStateRecord, Owner: "P07", CompatibleReader: Domain},
	{Kind: "permit", Version: PrivateDescriptorVersion, StateRecord: TunnelStateRecord, Owner: "P07", CompatibleReader: Domain},
	{Kind: "receipt", Version: PrivateDescriptorVersion, StateRecord: TunnelStateRecord, Owner: "P07", CompatibleReader: Domain},
	{Kind: "fences", Version: PrivateDescriptorVersion, StateRecord: TunnelStateRecord, Owner: "P07", CompatibleReader: Domain},
	{Kind: "resources", Version: PrivateDescriptorVersion, StateRecord: PrivateStateRecord, Owner: "P02", CompatibleReader: Domain},
}
