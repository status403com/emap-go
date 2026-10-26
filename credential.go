package emap

import (
	"context"
	"errors"
	"fmt"
)

// Credential identifies one IMAP mailbox by what's required to connect.
// Minimal by design — persistence concerns, display names, grouping, etc.
// belong to whatever layer wraps emap, not the connection client itself.
//
// Two credentials are considered the same mailbox for pooling purposes
// when their (Host, Port, Email) match (Email is normalized to lowercase).
// Password and OAuth2 are intentionally excluded from the pool key: changing
// the secret to fix an auth failure should reuse the entry in your config,
// not fork a second persistent connection.
type Credential struct {
	// Host is the IMAP server hostname or IP (e.g. "imap.gmail.com").
	Host string
	// Port is the IMAP server port. 993 for TLS, 143 for plain.
	Port int
	// UseTLS controls whether to dial via TLS. Should be true for any
	// real-world mailbox; false is only useful for local test servers.
	UseTLS bool
	// Email is the user's IMAP login name. Matched case-insensitively for
	// pooling.
	Email string
	// Password is the IMAP password or app-specific password. For Gmail,
	// generate an app password — regular passwords are rejected by default.
	// Ignored when OAuth2 is set.
	Password string
	// OAuth2, when non-nil, authenticates with SASL XOAUTH2 using a bearer
	// token from this source instead of LOGIN with Password. Required for
	// Microsoft mailboxes (Outlook.com, Hotmail, Live, Microsoft 365), which
	// reject password LOGIN with "Basic authentication is disabled". See
	// MicrosoftOAuth for a ready-made source.
	OAuth2 TokenSource
}

// TokenSource supplies OAuth2 access tokens for XOAUTH2. Token is called
// once per connection attempt (initial connect and every reconnect), so
// implementations should cache the token until it nears expiry rather than
// hitting the identity provider each time. ctx is canceled when the session
// shuts down mid-reconnect; honor it in any network call. Must be safe for
// concurrent use.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// ErrReauthRequired reports that a TokenSource can no longer mint access
// tokens without the mailbox owner signing in again — the refresh token
// expired, was revoked (e.g. password change), or the provider demands new
// interaction such as MFA or consent. Custom TokenSource implementations
// should wrap it for the same condition.
//
// The library never retries it. Subscribe and TestLogin return it, and a
// live session that hits it on reconnect is removed from the pool with its
// subscriptions closed (Subscription.Err reports it). Either way the error
// is a *ReauthRequiredError naming the mailbox.
var ErrReauthRequired = errors.New("emap: oauth sign-in required")

// ReauthRequiredError names the mailbox whose owner must sign in again.
// It wraps the TokenSource's error, so errors.Is(err, ErrReauthRequired)
// matches; use errors.As to learn which account to re-authorize.
type ReauthRequiredError struct {
	Host  string
	Port  int
	Email string
	Err   error
}

func (e *ReauthRequiredError) Error() string { return e.Email + ": " + e.Err.Error() }

func (e *ReauthRequiredError) Unwrap() error { return e.Err }

// bearerToken resolves cred's OAuth2 access token, or "" for password
// credentials. Callers run it before dialing so a slow or failing refresh
// never holds an unauthenticated IMAP connection open.
func bearerToken(ctx context.Context, cred Credential) (string, error) {
	if cred.OAuth2 == nil {
		return "", nil
	}
	token, err := cred.OAuth2.Token(ctx)
	if errors.Is(err, ErrReauthRequired) {
		return "", &ReauthRequiredError{Host: cred.Host, Port: cred.Port, Email: cred.Email, Err: err}
	}
	if err != nil {
		return "", fmt.Errorf("oauth2 token: %w", err)
	}
	return token, nil
}
