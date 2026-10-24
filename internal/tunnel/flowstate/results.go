package flowstate

func ChannelErrors(ch <-chan error) []error {
	var out []error
	for e := range ch {
		out = append(out, e)
	}
	return out
}
