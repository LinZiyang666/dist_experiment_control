package ssproxy

import "github.com/LinZiyang666/tether/internal/ssstate"

// EncodeProxyCut never emits secrets into JSON or Resource.Data. Callers seal
// the returned bytes immediately, then clear that temporary buffer.
func EncodeProxyCut(c ProxyCut) ([]byte, error) {
	data, err := ssstate.EncodeProxyCut(c)
	return data, ssImageError(err)
}

// DecodeProxyCut performs exact closure through the canonical builder, so
// every required but inapplicable broker field is validated as zero/empty.
func DecodeProxyCut(data []byte) (ProxyCut, error) { return ssstate.DecodeProxyCut(data) }
