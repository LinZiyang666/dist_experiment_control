package upgradecli

import "github.com/LinZiyang666/tether/internal/upgradecli/legacy"

// URL policy stays in the configuration leaf, shared with the legacy CLI.
// origin: S10 internal review MAKETEST-GATES F4
const DefaultUpgradeURLPrefix = legacy.DefaultUpgradeURLPrefix

func BrokerUpgradeAllow(flagChanged bool, flagVal, yamlVal []string) []string {
	return legacy.BrokerUpgradeAllow(flagChanged, flagVal, yamlVal)
}

func AgentUpgradeAllow(flagChanged bool, flagVal, yamlVal []string) ([]string, error) {
	return legacy.AgentUpgradeAllow(flagChanged, flagVal, yamlVal)
}
