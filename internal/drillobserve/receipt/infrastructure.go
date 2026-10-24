package receipt

// AttachmentProjection supplies only an already-projected public row and its
// neutral identity. Implementations live with their DTOs, outside custody's
// dependency closure, so encoding cannot pull control owners into the observer.
// origin: S10 internal review ARCH-REDS R2
type AttachmentProjection interface {
	InfrastructureObservation() ([16]byte, []byte, error)
}

func InfrastructureAttachment(a AttachmentProjection) ([16]byte, []byte, error) {
	return a.InfrastructureObservation()
}

// The composition root supplies BOOTTIME. A shared absolute value bounds every
// borrower, including a final callback that consumes the last nanosecond.
// origin: S10 internal review ARCH-REDS R2
func BeforeInfrastructureDeadline(d int64, now func() (int64, error)) bool {
	if now == nil || d <= 0 {
		return false
	}
	at, err := now()
	return err == nil && at > 0 && at < d
}
