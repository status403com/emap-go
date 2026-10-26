package emap

import (
	"context"
	"fmt"
)

// TestLogin opens a fresh connection to the credential's server,
// authenticates (LOGIN, or XOAUTH2 when cred.OAuth2 is set), and
// disconnects. Returns nil if the full handshake succeeded. Intended for
// credential-test UI flows ("does this password work?") — production code
// subscribes via Manager, which authenticates once and keeps the
// connection warm.
//
// The connection is closed before this function returns, so it does not
// participate in the Manager's pool.
func TestLogin(cred Credential) error {
	token, err := bearerToken(context.Background(), cred)
	if err != nil {
		return err
	}
	c, err := dialIMAPConn(cred.Host, cred.Port, cred.UseTLS)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer c.Close()
	if err := c.authenticate(cred, token); err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}
	c.logout()
	return nil
}
