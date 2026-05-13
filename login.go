package emap

import (
	"fmt"
)

// TestLogin opens a fresh connection to the server, authenticates with the
// provided credentials, and disconnects. Returns nil if the full handshake
// succeeded. Intended for credential-test UI flows ("does this password
// work?") — production code subscribes via Manager, which authenticates
// once and keeps the connection warm.
//
// The connection is closed before this function returns, so it does not
// participate in the Manager's pool.
func TestLogin(host string, port int, useTLS bool, email, password string) error {
	c, err := dialIMAPConn(host, port, useTLS)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	if err := c.login(email, password); err != nil {
		return fmt.Errorf("login: %w", err)
	}
	c.logout()
	return nil
}
