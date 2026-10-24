// supplymeta prints the typed S7 schema for offline fixture staging. It starts
// no product runtime and reads no installed configuration or credentials.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F6
package main

import (
	"encoding/hex"
	"fmt"

	"github.com/LinZiyang666/tether/internal/infratransfer"
)

func main() {
	schema := infratransfer.ObservationSchema()
	fmt.Println(hex.EncodeToString(schema[:]))
}
