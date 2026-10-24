//go:build linux

package observationexec

// ResetCustodyForTest restores process startup state between sealed FD exercises.
func ResetCustodyForTest() {
	observationCustody.Lock()
	defer observationCustody.Unlock()
	observationCustody.bound = false
	observationCustody.fd, observationCustody.channel = 0, 0
	observationCustody.identity = [32]byte{}
	observationCustody.poison = nil
}
