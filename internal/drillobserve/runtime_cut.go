package drillobserve

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

var runtimeImageCut struct {
	sync.Mutex
	id string
}

// BindRuntimeCut binds observation to the validated adjunct before Run starts
// its sampler. An imported process cannot relabel itself as a different cut.
// origin: S8 internal review activation-and-route-migration F4
func BindRuntimeCut(id string) error {
	if len(id) != 32 {
		return errors.New("runtime observation: invalid cut identity")
	}
	decoded, err := hex.DecodeString(id)
	if err != nil || hex.EncodeToString(decoded) != id {
		return errors.New("runtime observation: invalid cut identity")
	}
	runtimeImageCut.Lock()
	defer runtimeImageCut.Unlock()
	if runtimeImageCut.id != "" && runtimeImageCut.id != id {
		return errors.New("runtime observation: cut already bound")
	}
	runtimeImageCut.id = id
	return nil
}

func runtimeCutIdentity(c RuntimeCounters) string {
	runtimeImageCut.Lock()
	id := runtimeImageCut.id
	runtimeImageCut.Unlock()
	if id != "" {
		return id
	}
	// Cold images have no transaction. Name this actual boot owner, lineage
	// and generation independently; a future adjunct always supplies its own ID.
	sum := sha256.Sum256([]byte(fmt.Sprintf("runtime-image:%s:%d:%d:%s:%d", c.BootID, c.PID, c.StartTicks, c.Binding.InstanceID, c.Binding.Generation)))
	return hex.EncodeToString(sum[:16])
}
