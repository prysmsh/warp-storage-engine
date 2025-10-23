//go:build !linux
// +build !linux

package transport

import "syscall"

func setTCPOptions(network, address string, c syscall.RawConn) error {
	// No-op on non-Linux systems; platform-specific tuning only applies on Linux.
	return nil
}
