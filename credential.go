package emap

// Credential identifies one IMAP mailbox by what's required to connect.
// Minimal by design — persistence concerns, display names, grouping, etc.
// belong to whatever layer wraps emap, not the connection client itself.
//
// Two credentials are considered the same mailbox for pooling purposes
// when their (Host, Port, Email) match (Email is normalized to lowercase).
// Password is intentionally excluded from the pool key: changing the
// password to fix an auth failure should reuse the entry in your config,
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
	Password string
}
