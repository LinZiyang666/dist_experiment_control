//go:build !linux

package infraunit

import "github.com/LinZiyang666/tether/internal/infracontract"

func ParseEffectiveUnit(UnitTarget, []byte) (EffectiveUnit, error) {
	return EffectiveUnit{}, infracontract.ErrUnsupported
}

func ValidateEffectiveUnit(EffectiveUnit, UnitPolicy) error {
	return infracontract.ErrUnsupported
}

func ManagedUnitDropIn(infracontract.Component) ([]byte, error) {
	return nil, infracontract.ErrUnsupported
}

func PropertyNames(UnitTarget) []string { return nil }
