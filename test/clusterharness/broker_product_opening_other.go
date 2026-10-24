//go:build !linux

package clusterharness

// The private opening witness is reached only through StartBrokerCohortFixture,
// whose unsupported-platform twin refuses before creating any process/socket.
// origin: hot-update S6 plan §11 T48
