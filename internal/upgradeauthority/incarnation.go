package upgradeauthority

// CreationIncarnation returns the digest of a creation row that ReadCreation
// already compared inside the caller's reader. Register and lease replies report
// this actual authority; exposing the derivation here keeps every writer on the
// one parsed-salt rule instead of re-deriving from re-encodable hash text.
// origin: hot-update S8 plan §6.2 / §11.2 P15
func CreationIncarnation(creation Creation) (string, error) {
	return creationIncarnation(creation)
}
