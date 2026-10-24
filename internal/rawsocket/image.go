package rawsocket

import "os"

// socketImageSource is implemented by the socket owner, under its freeze guard.
// The borrowed file remains owned by that guard; exporting must neither dup it
// nor install a second netpoll wrapper. An unavailable guard fails closed.
// origin: hot-update S5 plan §5.1 / §5.3
type socketImageSource interface {
	imageFile() (*os.File, Fingerprint, error)
}

// TakeImageFile transfers exactly one descriptor, preserving it even on identity
// failure. The caller must put a nonnil result in salvage BEFORE checking err.
// Tuple and connect receipts are private runtime state and are checked by Adopt
// and the flow restore hook; this check never consumes SO_ERROR or repairs flags.
// origin: hot-update S5 plan §5.1 / §5.3
func TakeImageFile(src FileSource, id, name string, want Fingerprint) (*os.File, error) {
	if src == nil {
		return nil, ErrIdentity
	}
	f, err := src.TakeFile(id, name)
	if err != nil {
		return f, err
	}
	return f, validateImageFile(f, want)
}
